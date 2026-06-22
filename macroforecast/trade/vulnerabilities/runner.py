"""Vulnerability computation runner.

Iterates the registered vulnerability metrics over an entire trade DuckLake
catalog and writes the scores to a result DuckLake catalog — one column per
metric, keyed by ``date × nomenclature × indicator × flow × reporter`` (plus
frequency).

The source fact table is read through a direct, read-only DuckDB ``ATTACH`` (the
data lives in immutable Parquet files), while the result catalog is created or
upserted with the same ``DuckLakeTablesBuilder`` / ``DatabaseUpdater`` pattern as
:mod:`macroforecast.datasets.core.download`.
"""
# Importation des modules
from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
from typing import List, Optional, Sequence, Union

import duckdb
import narwhals as nw
import pandas as pd

from dt_ducklake_manager import (
    DatabaseUpdater,
    DuckLakeConnector,
    DuckLakeTablesBuilder,
)

from .base import DEFAULT_CONFIG, VulnerabilityConfig, VulnerabilityMetric
from .metrics import default_metrics

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nom de la table de faits DuckLake (convention dt_ducklake_manager)
_FACT_TABLE = "fact_table"

__all__ = ["VulnerabilityReport", "compute_vulnerabilities", "run_vulnerabilities"]


# ──────────────────────────────────────────────────────────────────────
# Rapport d'exécution
# ──────────────────────────────────────────────────────────────────────

# Structure résumant l'exécution du calcul des vulnérabilités
@dataclass
class VulnerabilityReport:
    """Summary of a vulnerability run.

    Attributes:
        cells: Number of output cells (distinct key combinations) scored.
        metrics: Names of the metrics computed.
        created: Whether the result schema was created (vs. upserted).
    """
    cells: int = 0
    metrics: Optional[List[str]] = None
    created: bool = False


# ──────────────────────────────────────────────────────────────────────
# Calcul (backend-agnostique, narwhals)
# ──────────────────────────────────────────────────────────────────────

# Fonction d'application de toutes les métriques sur un jeu de données
def compute_vulnerabilities(
    data: nw.DataFrame,
    metrics: Sequence[VulnerabilityMetric],
    config: VulnerabilityConfig = DEFAULT_CONFIG,
) -> nw.DataFrame:
    """Compute every metric and assemble a one-column-per-metric frame.

    Builds the canonical grid of distinct cells and left-joins each metric's
    output onto it, so cells a metric does not score (e.g. CDI2/CDI3 on non-import
    flows) carry a null value.

    Args:
        data: Narwhals frame of partner-level observations.
        metrics: Metric instances to apply.
        config: Column conventions (its ``key_columns`` define the grid).

    Returns:
        Narwhals frame with ``config.key_columns`` plus one column per metric.
    """
    # Clés de la grille de sortie
    keys = list(config.key_columns)

    # Suppression des observations inexploitables (partenaire ou valeur nuls) :
    # un partenaire nul fausse les masques booléens du filtre des pays individuels.
    data = data.drop_nulls(subset=[config.partner_col, config.value_col])

    # Grille canonique : cellules distinctes de la base
    result = data.select(*keys).unique()

    # Jointure gauche de la sortie de chaque métrique sur la grille
    for metric in metrics:
        # Calcul de la métrique (frame indexé par les clés + colonne metric.name)
        scored = metric.compute(data)
        result = result.join(scored, on=keys, how="left")

    return result


# ──────────────────────────────────────────────────────────────────────
# Lecture de la source / écriture du résultat (DuckLake)
# ──────────────────────────────────────────────────────────────────────

# Fonction de conversion d'un chemin en littéral SQL à slashes avant
def _sql_path(path: Union[str, Path]) -> str:
    """Return a forward-slash string form of a path for SQL literals.

    Args:
        path: Filesystem path.

    Returns:
        The path as a forward-slash string (portable inside DuckDB literals).
    """
    # Slashes avant : portables dans les littéraux DuckDB, y compris sous Windows
    return Path(path).as_posix()


# Fonction de lecture de la table de faits source (DuckDB en lecture seule)
def _read_source_fact_table(
    source_catalog: Union[str, Path],
    source_data_path: Union[str, Path],
    source_schema: str,
    columns: Sequence[str],
) -> pd.DataFrame:
    """Read selected columns of a DuckLake fact table, read-only.

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
        # données déplacé/normalisé différemment de celui stocké dans le catalogue.
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


# Fonction de détection de l'existence de la fact table d'un schéma
def _fact_table_exists(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    schema: str,
) -> bool:
    """Return whether ``{schema}.fact_table`` exists in the attached catalog.

    Args:
        conn: Open DuckLake connection.
        catalog_alias: Alias of the attached catalog.
        schema: Target schema.

    Returns:
        ``True`` if the fact table already exists.
    """
    # Introspection des tables du catalogue attaché
    row = conn.execute(
        "SELECT count(*) FROM duckdb_tables() "
        "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
        [catalog_alias, schema, _FACT_TABLE],
    ).fetchone()
    return bool(row and row[0] > 0)


# Fonction d'écriture du résultat dans le catalogue DuckLake (création ou upsert)
def _write_result(
    result_df: pd.DataFrame,
    primary_keys: Sequence[str],
    result_catalog: Union[str, Path],
    result_data_path: Union[str, Path],
    result_schema: str,
) -> bool:
    """Create or upsert the result table into the result DuckLake catalog.

    Mirrors :meth:`macroforecast.datasets.core.download.SDMXDownloader._write_dataframe`:
    builds the schema on first encounter, upserts by primary key afterwards.

    Args:
        result_df: Result DataFrame (grid keys + one column per metric).
        primary_keys: Primary-key columns (the grid keys).
        result_catalog: Path to the result ``.ducklake`` catalog file.
        result_data_path: Directory for the result Parquet data files.
        result_schema: Target schema in the result catalog.

    Returns:
        ``True`` if the schema was created, ``False`` if it was upserted.

    Raises:
        ValueError: If the update operation reports failure.
    """
    # Création du répertoire de données si nécessaire
    Path(result_data_path).mkdir(parents=True, exist_ok=True)
    Path(result_catalog).parent.mkdir(parents=True, exist_ok=True)

    # Connexion au catalogue résultat
    connector = DuckLakeConnector(str(result_catalog), str(result_data_path))
    conn = connector.connect()
    try:
        # Distinction création / mise à jour selon l'existence de la fact table
        if _fact_table_exists(conn, connector.catalog_alias, result_schema):
            # Mise à jour incrémentale (upsert par clé primaire)
            updater = DatabaseUpdater(
                connection=conn,
                categorical_threshold=None,
                schema=result_schema,
            )
            success = updater.update_database(
                result_df,
                use_transaction=True,
                compact_after_update=True,
            )
            if not success:
                raise ValueError("DatabaseUpdater reported failure for result table")
            logger.info(
                f"Upserted {len(result_df)} rows into '{result_schema}'"
            )
            return False
        # Première construction : métadonnées + fact table
        builder = DuckLakeTablesBuilder(
            result_df,
            categorical_threshold=None,
            primary_keys=list(primary_keys),
            connection=conn,
            schema=result_schema,
        )
        builder.build_schema()
        logger.info(
            f"Created schema '{result_schema}' with {len(result_df)} rows "
            f"(primary keys: {list(primary_keys)})"
        )
        return True
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────
# Orchestration de bout en bout
# ──────────────────────────────────────────────────────────────────────

# Fonction d'orchestration : source DuckLake → métriques → résultat DuckLake
def run_vulnerabilities(
    source_catalog: Union[str, Path],
    source_data_path: Union[str, Path],
    result_catalog: Union[str, Path],
    result_data_path: Union[str, Path],
    *,
    source_schema: str = "DS_045409",
    result_schema: str = "vulnerabilities",
    metrics: Optional[Sequence[VulnerabilityMetric]] = None,
    config: VulnerabilityConfig = DEFAULT_CONFIG,
    backend: str = "pandas",
) -> VulnerabilityReport:
    """Compute trade-vulnerability metrics and write them to a result catalog.

    Reads the source fact table, applies every metric over the whole dataset,
    and persists the scores (one column per metric) into the result DuckLake
    catalog, keyed by ``config.key_columns``.

    Args:
        source_catalog: Path to the source ``.ducklake`` catalog file.
        source_data_path: Directory of the source Parquet data files.
        result_catalog: Path to the result ``.ducklake`` catalog file.
        result_data_path: Directory for the result Parquet data files.
        source_schema: Schema of the source ``fact_table``.
        result_schema: Target schema in the result catalog.
        metrics: Metric instances to apply. Defaults to
            :func:`~macroforecast.trade.vulnerabilities.metrics.default_metrics`.
        config: Column and partner-code conventions.
        backend: Native eager backend for narwhals computation (``"pandas"`` or,
            when installed, ``"polars"``/``"pyarrow"``).

    Returns:
        A :class:`VulnerabilityReport` summarising the run.
    """
    # Métriques par défaut si non fournies
    metric_list = list(metrics) if metrics is not None else default_metrics(config)

    # Colonnes nécessaires : clés de la grille + partenaire + valeur
    required = list(
        dict.fromkeys(
            [*config.key_columns, config.partner_col, config.value_col]
        )
    )

    # Lecture de la table de faits source (pandas)
    source_pdf = _read_source_fact_table(
        source_catalog, source_data_path, source_schema, required
    )

    # Bascule éventuelle vers un autre backend eager (polars, pyarrow…)
    if backend == "polars":
        import polars as pl  # Import paresseux : dépendance optionnelle

        native = pl.from_pandas(source_pdf)
    elif backend == "pyarrow":
        import pyarrow as pa  # Import paresseux : dépendance optionnelle

        native = pa.Table.from_pandas(source_pdf)
    else:
        native = source_pdf

    # Calcul des métriques via narwhals (agnostique du backend)
    data = nw.from_native(native, eager_only=True)
    result = compute_vulnerabilities(data, metric_list, config)

    # Retour en pandas pour l'écriture DuckLake
    result_pdf = nw.to_native(result)
    if not isinstance(result_pdf, pd.DataFrame):
        result_pdf = result.to_pandas()

    # Écriture dans le catalogue résultat
    created = _write_result(
        result_pdf,
        config.key_columns,
        result_catalog,
        result_data_path,
        result_schema,
    )

    return VulnerabilityReport(
        cells=len(result_pdf),
        metrics=[m.name for m in metric_list],
        created=created,
    )
