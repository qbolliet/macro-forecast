"""Script de redressement BACI des flux de commerce international.

Lit les déclarations brutes COMTRADE depuis un catalogue DuckLake, applique la
méthodologie de reconstruction BACI du CEPII (conversion en tonnes, estimation et
retrait des coûts de fret, évaluation de la qualité des déclarants, réconciliation
pondérée des flux miroirs, réallocation des zones non spécifiées) et écrit les flux
réconciliés dans une nouvelle base DuckLake résultat.

La séparation des rôles est stricte : ``macroforecast.trade.processing.baci`` ne
contient que la méthodologie (dataframes, noms de colonnes et paramètres en
entrée) ; le présent script assume tout l'I/O — chargement de la configuration
YAML, construction de la ``BaciConfig``, lecture des fichiers Excel CEPII,
lecture de la table de faits COMTRADE (DuckLake) et écriture du résultat
(DuckLake). Tous les chemins proviennent de ``config/baci.yaml`` (jamais écrits
en dur) ; ``BUCKET`` y est explicitement à ``null`` et passé au loader Excel des
fichiers CEPII, pour préparer une future migration S3. Peut être ordonnancé
(Argo, cron) ou intégré comme nœud Kedro via la fonction exportée.
"""

import argparse
import logging
import sys
from dataclasses import fields, replace
from pathlib import Path
from typing import Dict, Optional, Sequence, Union

import duckdb
import pandas as pd
import yaml

# Module de gestion de la connexion à la base de données
from dt_ducklake_manager import (
    DatabaseUpdater,
    DuckLakeConnector,
    DuckLakeTablesBuilder,
)

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


def _resolve(path: str) -> str:
    """Resolve a configured path against the repository root when relative.

    Args:
        path: Path read from the YAML configuration.

    Returns:
        An absolute path string (unchanged if already absolute).
    """
    # Résolution relative à la racine du dépôt pour les chemins non absolus
    p = Path(path)
    return str(p if p.is_absolute() else ROOT / p)


def _sql_path(path: Union[str, Path]) -> str:
    """Return a forward-slash string form of a path for SQL literals.

    Args:
        path: Filesystem path.

    Returns:
        The path as a forward-slash string (portable inside DuckDB literals).
    """
    # Slashes avant : portables dans les littéraux DuckDB, y compris sous Windows
    return Path(path).as_posix()


def _read_comtrade_fact_table(
    source_catalog: Union[str, Path],
    source_data_path: Union[str, Path],
    source_schema: str,
    columns: Sequence[str],
) -> pd.DataFrame:
    """Read selected columns of a COMTRADE DuckLake fact table, read-only.

    Uses a hand-rolled ``ATTACH`` with ``OVERRIDE_DATA_PATH true`` rather than
    ``DuckLakeConnector.connect()``: the connector fails to re-attach an existing
    catalog under Windows/OneDrive because of a normalised ``DATA_PATH`` mismatch
    (same workaround as :func:`macroforecast.trade.vulnerabilities.runner._read_source_fact_table`).

    Args:
        source_catalog: Path to the source ``.ducklake`` catalog file.
        source_data_path: Directory of the source Parquet data files.
        source_schema: Schema holding the ``fact_table``.
        columns: Columns to project.

    Returns:
        A pandas DataFrame of the projected fact table.
    """
    # Connexion DuckDB en mémoire et chargement de l'extension DuckLake
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("INSTALL ducklake; LOAD ducklake;")
        # Attachement en lecture seule ; OVERRIDE_DATA_PATH tolère un chemin de
        # données normalisé différemment de celui stocké dans le catalogue.
        conn.execute(
            f"ATTACH 'ducklake:{_sql_path(source_catalog)}' AS src "
            f"(DATA_PATH '{_sql_path(source_data_path)}/', READ_ONLY, "
            f"OVERRIDE_DATA_PATH true)"
        )
        col_list = ", ".join(f'"{c}"' for c in columns)
        return conn.execute(
            f"SELECT {col_list} FROM src.{source_schema}.{_FACT_TABLE}"
        ).df()
    finally:
        conn.close()


def _fact_table_exists(
    conn: duckdb.DuckDBPyConnection, catalog_alias: str, schema: str
) -> bool:
    """Return whether ``{schema}.fact_table`` exists in the attached catalog.

    Args:
        conn: Open DuckLake connection.
        catalog_alias: Alias of the attached catalog.
        schema: Target schema.

    Returns:
        ``True`` if the fact table already exists.
    """
    row = conn.execute(
        "SELECT count(*) FROM duckdb_tables() "
        "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
        [catalog_alias, schema, _FACT_TABLE],
    ).fetchone()
    return bool(row and row[0] > 0)


def _write_result(
    result: pd.DataFrame,
    primary_keys: Sequence[str],
    result_catalog: Union[str, Path],
    result_data_path: Union[str, Path],
    result_schema: str,
) -> bool:
    """Create or upsert the reconciled table into the result DuckLake catalog.

    Mirrors :func:`macroforecast.trade.vulnerabilities.runner._write_result`:
    builds the schema on first encounter, upserts by primary key afterwards.

    Args:
        result: Reconciled flows to persist.
        primary_keys: Primary-key columns.
        result_catalog: Path to the result ``.ducklake`` catalog file.
        result_data_path: Directory for the result Parquet data files.
        result_schema: Target schema in the result catalog.

    Returns:
        ``True`` if the schema was created, ``False`` if it was upserted.

    Raises:
        ValueError: If the update operation reports failure.
    """
    # Préparation des répertoires
    Path(result_data_path).mkdir(parents=True, exist_ok=True)
    Path(result_catalog).parent.mkdir(parents=True, exist_ok=True)

    # Connexion au catalogue résultat
    connector = DuckLakeConnector(str(result_catalog), str(result_data_path))
    conn = connector.connect()
    try:
        # Distinction création / mise à jour selon l'existence de la fact table
        if _fact_table_exists(conn, connector.catalog_alias, result_schema):
            updater = DatabaseUpdater(
                connection=conn, categorical_threshold=None, schema=result_schema
            )
            success = updater.update_database(
                result, use_transaction=True, compact_after_update=True
            )
            if not success:
                raise ValueError("DatabaseUpdater reported failure for result table")
            logger.info("Upserted %d rows into '%s'", len(result), result_schema)
            return False
        # Première construction : métadonnées + fact table
        builder = DuckLakeTablesBuilder(
            result,
            categorical_threshold=None,
            primary_keys=list(primary_keys),
            connection=conn,
            schema=result_schema,
        )
        builder.build_schema()
        logger.info(
            "Created schema '%s' with %d rows (primary keys: %s)",
            result_schema, len(result), list(primary_keys),
        )
        return True
    finally:
        conn.close()


def load_baci_config(config_path: str) -> Dict:
    """Load the BACI YAML configuration file.

    Args:
        config_path: Path to ``config/baci.yaml``.

    Returns:
        The parsed configuration mapping (keys ``BUCKET``, ``paths``,
        ``parameters``).

    Examples:
        >>> cfg = load_baci_config("config/baci.yaml")  # doctest: +SKIP
        >>> cfg["BUCKET"]  # doctest: +SKIP
    """
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def baci_config_from_params(params: Optional[Dict]):
    """Build a ``BaciConfig`` from the YAML ``parameters`` section.

    Generic construction: every key matching a ``BaciConfig`` field name
    overrides the dataclass default; unknown keys are ignored with a warning.
    YAML lists are coerced to the tuple types expected by the frozen dataclass
    (including nested pairs such as ``excluded_pairs``).

    Args:
        params: The ``parameters`` mapping of ``config/baci.yaml`` (or ``None``).

    Returns:
        A ``BaciConfig`` reflecting the configured overrides.
    """
    from macroforecast.trade.processing import BaciConfig, DEFAULT_CONFIG

    # Aucune surcharge : configuration par défaut
    if not params:
        return DEFAULT_CONFIG

    # Surcharge générique champ à champ, avec coercition listes → tuples
    valid = {f.name for f in fields(BaciConfig)}
    overrides: Dict[str, object] = {}
    for key, value in params.items():
        if key not in valid:
            logger.warning("Paramètre BACI inconnu ignoré : %s", key)
            continue
        default = getattr(DEFAULT_CONFIG, key)
        if isinstance(default, tuple) and isinstance(value, (list, tuple)):
            value = tuple(
                tuple(v) if isinstance(v, (list, tuple)) else v for v in value
            )
        overrides[key] = value

    return replace(DEFAULT_CONFIG, **overrides)


def run(config_path: str, apply_nes: bool = True):
    """Run the BACI reconstruction from a YAML configuration file.

    Args:
        config_path: Path to ``config/baci.yaml``.
        apply_nes: Whether to apply the "Areas NES" reallocation step.

    Returns:
        The :class:`~macroforecast.trade.processing.BaciReport` of the run.
    """
    from macroforecast.storage2 import Loader
    from macroforecast.trade.processing import required_columns, run_baci

    # Chargement de la configuration (chemins + paramètres méthodologiques)
    config = load_baci_config(config_path)
    paths = config["paths"]
    bucket = config["BUCKET"]
    baci_config = baci_config_from_params(config.get("parameters"))

    # Lecture des fichiers Excel CEPII (loader local/S3 selon BUCKET)
    excel_loader = Loader()
    dist = excel_loader.load(_resolve(paths["dist_cepii"]), bucket=bucket)
    geo = excel_loader.load(_resolve(paths["geo_cepii"]), bucket=bucket)

    # Lecture de la table de faits COMTRADE (DuckLake, lecture seule)
    comtrade = _read_comtrade_fact_table(
        _resolve(paths["comtrade_catalog"]),
        _resolve(paths["comtrade_data"]),
        paths["comtrade_schema"],
        required_columns(baci_config),
    )

    # Application de la méthodologie sur les jeux de données chargés
    reconciled, report = run_baci(
        comtrade, dist, geo, config=baci_config, apply_nes=apply_nes
    )

    # Écriture du résultat dans le catalogue DuckLake
    report.created = _write_result(
        reconciled,
        baci_config.primary_keys,
        _resolve(paths["result_catalog"]),
        _resolve(paths["result_data"]),
        paths["result_schema"],
    )
    logger.info("Redressement BACI terminé : %s", report)
    return report


def main() -> None:
    """CLI entry point for the BACI reconstruction script.

    Parses command-line arguments and runs the reconstruction from the YAML
    configuration.
    """
    parser = argparse.ArgumentParser(
        description="Reconstruct BACI reconciled trade flows from COMTRADE."
    )
    parser.add_argument(
        "--config",
        default=str(ROOT / "config" / "baci.yaml"),
        help="Path to the YAML configuration (default: config/baci.yaml)",
    )
    parser.add_argument(
        "--no-nes",
        action="store_true",
        help="Disable the 'Areas NES' reallocation step",
    )
    args = parser.parse_args()
    run(args.config, apply_nes=not args.no_nes)


if __name__ == "__main__":
    main()
