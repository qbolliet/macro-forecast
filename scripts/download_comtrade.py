"""Script de téléchargement des données tariffline UN Comtrade.

Télécharge les données de commerce international depuis l'API UN Comtrade en
scindant les requêtes par période x lot de produits. La mise à jour incrémentale
s'appuie sur ``getFinalDataAvailability`` (champ ``lastReleased``) : une période
n'est re-téléchargée que si sa date de publication est postérieure au dernier
téléchargement enregistré. Peut être ordonnancé (Argo, cron) ou intégré comme
nœud Kedro via les fonctions exportées.

UN Comtrade n'étant pas un provider SDMX, ce script n'utilise pas
l'orchestrateur ``download_updates`` mais ré-emploie directement
``dt_ducklake_manager`` et un registre JSON de dernier téléchargement, sur le
même modèle que ``scripts/download_eurostat_comext.py``.
"""

import argparse
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# Racine du dépôt (résolution relative à ce fichier)
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# Nom de la table de faits DuckLake (convention dt_ducklake_manager)
_FACT_TABLE = "fact_table"
# Clé racine du registre JSON des dates de dernier téléchargement
_REGISTRY_ROOT = "DOWNLOADS"


def fetch_codes(client=None):
    """Fetch the reporter and product codelists from UN Comtrade.

    Args:
        client: ``ComtradeClient`` instance; a new one is created if ``None``.

    Returns:
        Tuple ``(reporter_codes, product_codes)``, each a DataFrame with a
        ``code`` column. Products are restricted to 6-digit HS codes (a request
        on an aggregated HS code already covers its sub-nomenclatures).

    Examples:
        >>> reporter_codes, product_codes = fetch_codes()  # doctest: +SKIP
    """
    from macroforecast.datasets import ComtradeClient

    if client is None:
        client = ComtradeClient()

    # Reporters valides (codes M49 non expirés)
    reporters = client._extract_codes("reporter")
    reporter_codes = pd.DataFrame({"code": [str(c) for c in reporters]})
    logger.info("%d reporters", len(reporter_codes))

    # Produits : nomenclature HS restreinte aux SH6
    products = client._extract_codes("cmd:HS")
    product_codes = pd.DataFrame(
        {"code": [str(c) for c in products if len(str(c)) == 6]}
    )
    logger.info("%d produits SH6", len(product_codes))

    return reporter_codes, product_codes


def build_split_queries(
    dataflow: str,
    reporter_codes: pd.DataFrame,
    product_codes: pd.DataFrame,
    config_path: str,
    frequency: str = "annual",
    flows: Optional[List[str]] = None,
    products_step: int = 10,
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
    periods: Optional[List[str]] = None,
    client=None,
) -> List[Any]:
    """Build the split queries for a Comtrade dataflow.

    Applies the include/exclude filters declared in the YAML configuration to
    the reporter and product codelists, then returns one query per
    (period, product-batch) pair. Reporters are passed as a single value
    (``None`` = all reporters) when the configuration does not narrow them,
    keeping the number of queries — and thus API calls — small.

    Args:
        dataflow: Logical dataflow identifier (e.g. ``"C_A_HS"``).
        reporter_codes: DataFrame with a ``code`` column (reporter dimension).
        product_codes: DataFrame with a ``code`` column (product dimension).
        config_path: Path to the YAML filter configuration.
        frequency: Data frequency (``"annual"`` or ``"monthly"``).
        flows: Trade flow codes (default ``["M", "X"]``).
        products_step: Number of products per query.
        period_start: Start period (used when ``periods`` is omitted).
        period_end: End period (used when ``periods`` is omitted).
        periods: Explicit list of periods.
        client: ``ComtradeClient`` instance; a new one is created if ``None``.

    Returns:
        List of ``ComtradeQueryRequest`` objects.

    Raises:
        KeyError: If the dataflow has no entry in the YAML ``split_filters``.
    """
    from macroforecast.datasets import ComtradeClient, ComtradeQueryRequest
    from macroforecast.datasets.utils import filter_codes, load_split_filters

    if client is None:
        client = ComtradeClient()
    if flows is None:
        flows = ["M", "X"]

    # Application des filtres de la configuration
    split_filters = load_split_filters(config_path, dataflow)
    products = filter_codes(product_codes["code"], **split_filters["product"])

    # Reporters : None (tous) si aucun filtre, sinon la liste filtrée
    reporter_filter = split_filters["reporter"]
    if all(reporter_filter.get(k) is None for k in reporter_filter):
        reporters: Optional[List[str]] = None
    else:
        reporters = filter_codes(reporter_codes["code"], **reporter_filter)
    logger.info(
        "%s reporters | %d produits sélectionnés",
        "tous" if reporters is None else len(reporters),
        len(products),
    )

    # Périodes valides à requêter
    if periods is None:
        periods = client.get_valid_periods(
            period_start=period_start, period_end=period_end, frequency=frequency
        )

    # Construction d'une requête par (période, lot de produits)
    queries = []
    for period in reversed(periods):
        for i in range(0, len(products), products_step):
            product_batch = products[i: i + products_step]
            queries.append(
                ComtradeQueryRequest(
                    dataflow=dataflow,
                    flows=flows,
                    reporters=reporters,
                    products=product_batch,
                    periods=period,
                    frequency=frequency,
                )
            )
    logger.info("%d requêtes construites", len(queries))
    return queries


def run_comtrade_download(
    queries: List[Any],
    catalog_path: str,
    data_path: str,
    last_download_path: str,
    frequency: str = "annual",
    type_code: str = "C",
    classification: str = "HS",
    categorical_threshold: Optional[int] = None,
    client=None,
) -> Dict[str, Any]:
    """Download or incrementally update Comtrade data for the given queries.

    For each period, the final-data availability (``lastReleased``) is compared
    with the last recorded download: queries whose period has not been
    re-released since are skipped. Results are written to a DuckLake catalog
    (one schema per dataflow) and the last-download registry is updated after
    each successful write.

    Args:
        queries: List of ``ComtradeQueryRequest`` objects.
        catalog_path: Path to the DuckLake catalog file.
        data_path: Directory for Parquet data files.
        last_download_path: Path to the JSON registry of last-download dates.
        frequency: Data frequency (``"annual"`` or ``"monthly"``).
        type_code: Trade type (``"C"`` or ``"S"``).
        classification: Classification code (``"HS"``, ...).
        categorical_threshold: Forwarded to ``dt_ducklake_manager`` (``None``
            disables dimension tables).
        client: ``ComtradeClient`` instance; a new one is created if ``None``.

    Returns:
        A report dict ``{processed, rows_written, skipped, empty, errors}``.
    """
    from macroforecast.datasets import ComtradeClient
    from macroforecast.datasets.core.download import (
        _now,
        _parse_iso,
        _primary_keys,
        _schema_name,
    )
    from dt_ducklake_manager import (
        DatabaseUpdater,
        DuckLakeConnector,
        DuckLakeTablesBuilder,
    )

    if client is None:
        client = ComtradeClient()

    # Préparation des chemins
    Path(data_path).mkdir(parents=True, exist_ok=True)
    Path(last_download_path).parent.mkdir(parents=True, exist_ok=True)

    # Chargement du registre des dates de dernier téléchargement
    registry = _load_registry(last_download_path)

    # Pré-calcul de la disponibilité finale par période (un appel par période)
    last_released_by_period = _fetch_last_released(
        client, queries, frequency, type_code, classification
    )

    # Initialisation du rapport
    report = {"processed": 0, "rows_written": 0, "skipped": 0, "empty": 0, "errors": 0}

    # Ouverture de la connexion au catalogue DuckLake
    connector = DuckLakeConnector(catalog_path, data_path)
    conn = connector.connect()
    catalog_alias = connector.catalog_alias
    try:
        # Parcours des requêtes
        for query in queries:
            key = query.identity_key()
            entry = registry.get(key)
            since = _parse_iso(entry.get("last_download")) if entry else None

            # Saut si la période n'a pas été republiée depuis le dernier téléchargement
            last_released = _parse_iso(last_released_by_period.get(str(query.periods)))
            if since is not None and last_released is not None and since >= last_released:
                report["skipped"] += 1
                continue

            # Instant de référence capturé avant la requête
            req_started = _now()

            # Récupération des données
            try:
                df = client.execute_query(query)
            except Exception as e:
                logger.exception("Query %s failed: %s", key, e)
                report["errors"] += 1
                continue

            report["processed"] += 1
            schema = _schema_name(query.dataflow)

            # Écriture en base si des données ont été récupérées
            if df is not None and not df.empty:
                structure = client.resolve_query_structure(query)
                _write_dataframe(
                    conn=conn,
                    df=df,
                    structure=structure,
                    schema=schema,
                    dataflow=query.dataflow,
                    catalog_alias=catalog_alias,
                    categorical_threshold=categorical_threshold,
                    primary_keys_fn=_primary_keys,
                    builder_cls=DuckLakeTablesBuilder,
                    updater_cls=DatabaseUpdater,
                )
                report["rows_written"] += len(df)
            else:
                report["empty"] += 1
                logger.info("%s / %s: no data", query.dataflow, query.periods)

            # ORDRE CRITIQUE : la base est écrite avant la mise à jour du JSON
            registry[key] = {
                "agency": query.agency,
                "dataflow": query.dataflow,
                "params": _json_safe_params(query.to_dict()),
                "last_download": req_started.isoformat(),
            }
            _save_registry(last_download_path, registry)
    finally:
        conn.close()
        logger.info("DuckLake connection closed")

    logger.info("Téléchargement terminé : %s", report)
    return report


def _fetch_last_released(
    client, queries: List[Any], frequency: str, type_code: str, classification: str
) -> Dict[str, Optional[str]]:
    """Fetch the ``lastReleased`` date for every period present in the queries.

    Args:
        client: ``ComtradeClient`` instance.
        queries: Queries whose ``periods`` are inspected.
        frequency: Data frequency.
        type_code: Trade type.
        classification: Classification code.

    Returns:
        Mapping ``{period: lastReleased}`` (the most recent release across all
        reporters for that period).
    """
    from macroforecast.datasets.sources.comtrade.parsing import (
        parse_availability_last_released,
    )

    # Ensemble des périodes distinctes
    periods = sorted({str(q.periods) for q in queries if q.periods is not None})

    result: Dict[str, Optional[str]] = {}
    for period in periods:
        try:
            # Disponibilité finale (tous reporters) pour la période
            availability = client.get_final_data_availability(
                reporters=None,
                periods=period,
                frequency=frequency,
                type_code=type_code,
                classification=classification,
            )
            released = parse_availability_last_released(availability)
            # Date de publication la plus récente sur l'ensemble des reporters
            dates = [v for v in released.values() if v is not None]
            result[period] = max(dates) if dates else None
        except Exception as e:
            # Échec non bloquant : la période sera rafraîchie par précaution
            logger.warning("Could not fetch availability for %s: %s", period, e)
            result[period] = None
    return result


def _write_dataframe(
    conn,
    df: pd.DataFrame,
    structure,
    schema: str,
    dataflow: str,
    catalog_alias: str,
    categorical_threshold: Optional[int],
    primary_keys_fn,
    builder_cls,
    updater_cls,
) -> None:
    """Create or update the dataflow's DuckLake table with a DataFrame.

    Mirrors :meth:`SDMXDownloader._write_dataframe`: builds the schema on first
    encounter or upserts by primary key on subsequent runs.

    Args:
        conn: Open DuckLake connection.
        df: Non-empty DataFrame to persist.
        structure: Resolved dataflow structure (for primary keys).
        schema: Target DuckLake schema.
        dataflow: Dataflow identifier (for logging).
        catalog_alias: DuckLake catalog alias (for table introspection).
        categorical_threshold: Forwarded to ``dt_ducklake_manager``.
        primary_keys_fn: ``_primary_keys`` helper from ``core.download``.
        builder_cls: ``DuckLakeTablesBuilder`` class.
        updater_cls: ``DatabaseUpdater`` class.

    Raises:
        ValueError: If no primary-key column can be resolved or the update fails.
    """
    # Calcul des clés primaires (dimensions + colonne temporelle)
    primary_keys = primary_keys_fn(structure, list(df.columns))
    if not primary_keys:
        raise ValueError(
            f"No primary-key column resolved for '{dataflow}'. "
            f"Columns: {list(df.columns)}"
        )

    # Détection de l'existence de la fact table
    row = conn.execute(
        "SELECT count(*) FROM duckdb_tables() "
        "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
        [catalog_alias, schema, _FACT_TABLE],
    ).fetchone()
    fact_exists = bool(row and row[0] > 0)

    # Mise à jour incrémentale ou création initiale du schéma
    if fact_exists:
        updater = updater_cls(
            connection=conn,
            categorical_threshold=categorical_threshold,
            schema=schema,
        )
        success = updater.update_database(
            df, use_transaction=True, compact_after_update=True
        )
        if not success:
            raise ValueError(f"DatabaseUpdater reported failure for '{dataflow}'")
        logger.info("%s: updated %d rows in schema '%s'", dataflow, len(df), schema)
    else:
        builder = builder_cls(
            df,
            categorical_threshold=categorical_threshold,
            primary_keys=primary_keys,
            connection=conn,
            schema=schema,
        )
        builder.build_schema()
        logger.info(
            "%s: created schema '%s' with %d rows (primary keys: %s)",
            dataflow, schema, len(df), primary_keys,
        )


def _json_safe_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Convert query params to a JSON-serialisable mapping.

    Args:
        params: ``query.to_dict()`` output.

    Returns:
        JSON-serialisable equivalent (enums replaced by their value).
    """
    from macroforecast.datasets.core.download import _json_safe

    return _json_safe(params)


def _load_registry(path: str) -> Dict[str, Any]:
    """Load the last-download registry from a JSON file (empty if absent).

    Args:
        path: Local path to the registry JSON file.

    Returns:
        The ``DOWNLOADS`` mapping (identity_key → entry).
    """
    from macroforecast.storage import Loader

    # Court-circuit si le fichier n'existe pas encore
    if not Path(path).exists():
        return {}
    data = Loader().load(str(path)) or {}
    return data.get(_REGISTRY_ROOT, {})


def _save_registry(path: str, registry: Dict[str, Any]) -> None:
    """Persist the last-download registry atomically (tempfile + replace).

    Args:
        path: Local path to the registry JSON file.
        registry: The ``DOWNLOADS`` mapping to persist.
    """
    from macroforecast.storage import Saver

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    # Écriture atomique : fichier temporaire .json puis remplacement
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), suffix=".json")
    os.close(fd)
    try:
        Saver().save(tmp_name, {_REGISTRY_ROOT: registry}, indent=2, ensure_ascii=False)
        os.replace(tmp_name, str(target))
    except Exception:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise


def main() -> None:
    """CLI entry point for the Comtrade download script.

    Parses command-line arguments, instantiates a shared ComtradeClient, and
    chains fetch_codes → build_split_queries → run_comtrade_download.
    """
    parser = argparse.ArgumentParser(
        description="Download UN Comtrade tariffline trade data."
    )
    parser.add_argument(
        "--dataflow",
        default="C_A_HS",
        help="Logical dataflow id typeCode_freqCode_clCode (default: C_A_HS)",
    )
    parser.add_argument(
        "--frequency",
        default="annual",
        choices=["annual", "monthly"],
        help="Data frequency (default: annual)",
    )
    parser.add_argument("--type-code", default="C", help="Trade type (default: C)")
    parser.add_argument(
        "--classification", default="HS", help="Classification code (default: HS)"
    )
    parser.add_argument("--period-start", default=None, help="Start period (YYYY[-MM])")
    parser.add_argument("--period-end", default=None, help="End period (YYYY[-MM])")
    parser.add_argument(
        "--products-step", type=int, default=10, help="Products per query (default: 10)"
    )
    parser.add_argument(
        "--config",
        default=str(ROOT / "config" / "datasets" / "comtrade.yaml"),
        help="Path to the YAML filter configuration",
    )
    parser.add_argument(
        "--catalog",
        default=str(ROOT / "data" / "comtrade.ducklake"),
        help="Path to the DuckLake catalog file",
    )
    parser.add_argument(
        "--data-dir",
        default=str(ROOT / "data" / "comtrade"),
        help="Directory for Parquet data files",
    )
    parser.add_argument(
        "--last-download",
        default=str(ROOT / "parameters" / "comtrade_last_download.json"),
        help="Path to the last-download timestamps JSON",
    )
    args = parser.parse_args()

    from macroforecast.datasets import ComtradeClient

    client = ComtradeClient()
    try:
        reporter_codes, product_codes = fetch_codes(client)
        queries = build_split_queries(
            dataflow=args.dataflow,
            reporter_codes=reporter_codes,
            product_codes=product_codes,
            config_path=args.config,
            frequency=args.frequency,
            products_step=args.products_step,
            period_start=args.period_start,
            period_end=args.period_end,
            client=client,
        )
        run_comtrade_download(
            queries,
            catalog_path=args.catalog,
            data_path=args.data_dir,
            last_download_path=args.last_download,
            frequency=args.frequency,
            type_code=args.type_code,
            classification=args.classification,
            client=client,
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()
