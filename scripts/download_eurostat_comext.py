"""Script de téléchargement des données Comext (commerce extérieur) Eurostat.

Télécharge le dataflow DS-045409 depuis l'API SDMX 3.0 d'Eurostat en scindant
les requêtes par pays reporter x code produit. Peut être ordonnancé (Argo, cron)
ou intégré directement comme nœud Kedro via les fonctions exportées.
"""

import argparse
import itertools
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

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


def fetch_codelists(
    dataflow: str,
    client=None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch reporter and product codelists for a Comext dataflow.

    Retrieves the Data Structure Definition (DSD) of the dataflow to deduce
    the codelist identifiers of the ``reporter`` and ``product`` dimensions,
    then downloads and parses each codelist.

    Args:
        dataflow: Eurostat dataflow identifier (e.g. ``"DS-045409"``).
        client: EurostatClient instance; a new one is created if ``None``.

    Returns:
        Tuple ``(reporter_codes, product_codes)``, each a DataFrame with
        columns ``(code, name)``.

    Examples:
        >>> reporter_codes, product_codes = fetch_codelists("DS-045409")
        >>> "FR" in reporter_codes["code"].values
        True
    """
    from macroforecast.datasets import EurostatClient, StructureResourceType
    from macroforecast.datasets.sources.eurostat.parsing import parse_codelist_response

    if client is None:
        client = EurostatClient()

    # Déduction des codelists depuis la DSD (évite de coder en dur les identifiants)
    structure = client.get_dataflow_structure(dataflow)
    dim_codelists = {d.name: d.codelist for d in structure.dimensions}

    reporter_codelist = dim_codelists["reporter"]
    product_codelist = dim_codelists["product"]
    logger.info("reporter -> %s | product -> %s", reporter_codelist, product_codelist)

    reporter_xml = client.get_structure(StructureResourceType.CODELIST, reporter_codelist)
    reporter_codes = parse_codelist_response(reporter_xml)
    logger.info("%d codes reporter", len(reporter_codes))

    product_xml = client.get_structure(StructureResourceType.CODELIST, product_codelist)
    product_codes = parse_codelist_response(product_xml)
    logger.info("%d codes produit", len(product_codes))

    return reporter_codes, product_codes


def build_split_queries(
    dataflow: str,
    reporter_codes: pd.DataFrame,
    product_codes: pd.DataFrame,
    config_path: str,
    fixed_dims: Optional[Dict[str, Union[List[str], str]]] = None,
) -> List[Any]:
    """Build the split queries for a Comext dataflow.

    Applies the include/exclude filters declared in the YAML configuration to
    the reporter and product codelists, then returns one query per
    (reporter, product) pair in the cartesian product.

    Args:
        dataflow: Eurostat dataflow identifier (e.g. ``"DS-045409"``).
        reporter_codes: DataFrame with column ``code`` for the reporter dimension.
        product_codes: DataFrame with column ``code`` for the product dimension.
        config_path: Path to the YAML filter configuration file
            (e.g. ``config/datasets/eurostat.yaml``).
        fixed_dims: Fixed dimensions shared by every query. Defaults to the
            standard DS-045409 dimensions (freq=A, partner=*, flow=1,
            indicators=QUANTITY_IN_100KG).

    Returns:
        List of ``EurostatQueryRequestV30`` objects, one per (reporter, product) pair.

    Raises:
        AssertionError: If a filtered code is absent from the upstream codelist.
        KeyError: If the dataflow has no entry in the YAML ``split_filters`` section.

    Examples:
        >>> queries = build_split_queries(
        ...     "DS-045409", reporter_codes, product_codes,
        ...     "config/datasets/eurostat.yaml"
        ... )  # doctest: +SKIP
        >>> len(queries) > 0
        True
    """
    from macroforecast.datasets import EurostatQueryRequestV30
    from macroforecast.datasets.utils import (
        filter_codes,
        load_dataflow_parameters,
        load_split_filters,
    )

    if fixed_dims is None:
        # Dimensions ancrées lues depuis le YAML (source unique) ; repli sur les
        # dimensions standard DS-045409 si la section est absente
        fixed_dims = load_dataflow_parameters(
            config_path, dataflow, section="fixed_dims"
        ) or {
            "freq": "A",
            "partner": "*",
            "flow": ["1", "2"],                                     # Les flux (import, export)
            "indicators": ["QUANTITY_IN_100KG", "VALUE_IN_EUROS"],  # Masse et valeur
        }

    split_filters = load_split_filters(config_path, dataflow)
    reporters = filter_codes(reporter_codes["code"], **split_filters["reporter"])
    products = filter_codes(product_codes["code"], **split_filters["product"])
    logger.info("%d reporters | %d produits sélectionnés", len(reporters), len(products))

    assert set(reporters) <= set(reporter_codes["code"]), "reporter inconnu de la codelist"
    assert set(products) <= set(product_codes["code"]), "produit inconnu de la codelist"

    queries = [
        EurostatQueryRequestV30(
            dataflow=dataflow,
            dimensions={**fixed_dims, "reporter": reporter, "product": product},
        )
        for reporter, product in itertools.product(reporters, products)
    ]
    logger.info("%d requêtes construites", len(queries))
    return queries


def run_comext_download(
    queries: List[Any],
    catalog_path: str,
    data_path: str,
    structures_path: str,
    last_download_path: str,
    client=None,
) -> Dict[str, Any]:
    """Download or incrementally update Comext data for the given queries.

    On the first run (no prior timestamp), all series are fetched. On subsequent
    runs, the client checks the dataflow's data constraint and skips queries that
    have not been updated since the last download.

    Args:
        queries: List of ``EurostatQueryRequestV30`` objects to execute.
        catalog_path: Path to the DuckLake catalog file.
        data_path: Directory for Parquet data files.
        structures_path: Path to the JSON registry storing dataflow structures.
        last_download_path: Path to the JSON registry storing last-download
            timestamps (updated in-place after each successful download).
        client: EurostatClient instance; a new one is created if ``None``.

    Returns:
        Download report dict returned by ``download_updates``.

    Examples:
        >>> report = run_comext_download(
        ...     queries, "data/eurostat.ducklake", "data/eurostat",
        ...     "parameters/structures.json", "parameters/last_download.json"
        ... )  # doctest: +SKIP
    """
    from macroforecast.datasets import EurostatClient
    from macroforecast.datasets.core.download import download_updates
    from dt_ducklake_manager import DuckLakeConnector

    if client is None:
        client = EurostatClient()

    Path(data_path).mkdir(parents=True, exist_ok=True)
    Path(structures_path).parent.mkdir(parents=True, exist_ok=True)

    connector = DuckLakeConnector(catalog_path, data_path)
    report = download_updates(
        client,
        queries,
        connector,
        structures_path=structures_path,
        last_download_path=last_download_path,
    )
    logger.info("Téléchargement terminé : %s", report)
    return report


def main() -> None:
    """CLI entry point for the Comext download script.

    Parses command-line arguments, instantiates a shared EurostatClient, and
    chains fetch_codelists → build_split_queries → run_comext_download.
    """
    parser = argparse.ArgumentParser(
        description="Download Eurostat Comext (DS-045409) trade data."
    )
    parser.add_argument(
        "--dataflow",
        default="DS-045409",
        help="Eurostat dataflow identifier (default: DS-045409)",
    )
    parser.add_argument(
        "--config",
        default=str(ROOT / "config" / "datasets" / "eurostat.yaml"),
        help="Path to the YAML filter configuration",
    )
    parser.add_argument(
        "--catalog",
        default=str(ROOT / "data" / "eurostat.ducklake"),
        help="Path to the DuckLake catalog file",
    )
    parser.add_argument(
        "--data-dir",
        default=str(ROOT / "data" / "eurostat"),
        help="Directory for Parquet data files",
    )
    parser.add_argument(
        "--structures",
        default=str(ROOT / "parameters" / "eurostat_structures.json"),
        help="Path to the structures JSON registry",
    )
    parser.add_argument(
        "--last-download",
        default=str(ROOT / "parameters" / "eurostat_last_download.json"),
        help="Path to the last-download timestamps JSON",
    )
    args = parser.parse_args()

    from macroforecast.datasets import EurostatClient

    client = EurostatClient()
    try:
        reporter_codes, product_codes = fetch_codelists(args.dataflow, client)
        queries = build_split_queries(
            args.dataflow,
            reporter_codes,
            product_codes,
            args.config,
        )
        run_comext_download(
            queries,
            catalog_path=args.catalog,
            data_path=args.data_dir,
            structures_path=args.structures,
            last_download_path=args.last_download,
            client=client,
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()
