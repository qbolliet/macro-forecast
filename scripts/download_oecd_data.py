#!/usr/bin/env python3
"""Download OECD data using pre-configured queries.

This script loads query configurations from config/datasets/oecd.yaml
and executes them using the OECDClient. It supports filtering queries
by last update date.

Usage:
    # Download all configured queries
    python scripts/download_oecd_data.py

    # Only download queries updated since a specific date
    python scripts/download_oecd_data.py --updated-since 2024-01-01

    # Limit to specific queries
    python scripts/download_oecd_data.py --queries kei_g7_monthly

    # Save to specific output directory
    python scripts/download_oecd_data.py --output-dir data/raw
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd
import yaml

# Ajout du répertoire parent au path pour importer macroforecast
sys.path.insert(0, str(Path(__file__).parent.parent))

from macroforecast.datasets.sources.oecd import OECDClient, QueryRequest

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: Path) -> Dict[str, Any]:
    """Load YAML configuration file.

    Args:
        config_path: Path to oecd.yaml configuration file.

    Returns:
        Parsed configuration dictionary.
    """
    logger.info(f"Loading configuration from {config_path}")
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config


def parse_query_requests(config: Dict[str, Any]) -> Dict[str, QueryRequest]:
    """Parse queries section from config into QueryRequest objects.

    Args:
        config: Configuration dictionary containing 'queries' section.

    Returns:
        Dictionary mapping query names to QueryRequest objects.
    """
    if 'queries' not in config:
        raise ValueError(
            "Configuration must contain 'queries' section. "
            "Please update config/datasets/oecd.yaml"
        )

    query_requests_config = config['queries']
    logger.info(f"Found {len(query_requests_config)} query configurations")

    queries: Dict[str, QueryRequest] = {}

    for query_name, query_config in query_requests_config.items():
        try:
            query = QueryRequest(
                agency=query_config['agency'],
                dataflow=query_config['dataflow'],
                version=query_config.get('version', '+'),
                dimensions=query_config.get('dimensions'),
                start_period=query_config.get('start_period'),
                end_period=query_config.get('end_period'),
                last_n_observations=query_config.get('last_n_observations'),
                split_dimensions=query_config.get('split_dimensions'),
                max_split_combinations=query_config.get('max_split_combinations', 100),
            )
            queries[query_name] = query
            logger.info(f"✓ Parsed query '{query_name}': {query.get_dataflow_key()}")

        except KeyError as e:
            logger.error(f"✗ Query '{query_name}' missing required field: {e}")
            raise ValueError(f"Invalid query configuration for '{query_name}': {e}")

    return queries


def save_dataframe(
    df: pd.DataFrame,
    query_name: str,
    output_dir: Path,
    format: str = 'parquet'
) -> Path:
    """Save DataFrame to file.

    Args:
        df: DataFrame to save.
        query_name: Name of the query (used in filename).
        output_dir: Output directory.
        format: File format ('parquet', 'csv', 'feather').

    Returns:
        Path to saved file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    if format == 'parquet':
        output_path = output_dir / f"{query_name}.parquet"
        df.to_parquet(output_path, index=False)
    elif format == 'csv':
        output_path = output_dir / f"{query_name}.csv"
        df.to_csv(output_path, index=False)
    else:
        raise ValueError(f"Unsupported format: {format}")

    logger.info(f"Saved {len(df)} rows to {output_path}")
    return output_path


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Download OECD data using pre-configured queries',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--config',
        type=Path,
        default=Path('config/datasets/oecd.yaml'),
        help='Path to OECD configuration file'
    )
    parser.add_argument(
        '--updated-since',
        type=str,
        default=None,
        help='Only download queries updated since this date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--queries',
        nargs='+',
        default=None,
        help='Limit to specific query names'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path('data/raw/oecd'),
        help='Output directory for downloaded data'
    )
    parser.add_argument(
        '--format',
        choices=['parquet', 'csv', 'feather'],
        default='parquet',
        help='Output file format'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print queries without executing them'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # Chargement de la configuration
        config = load_config(args.config)
        all_queries = parse_query_requests(config)

        # Filtrage des requêtes à exécuter
        if args.queries:
            queries_to_execute = {
                name: query for name, query in all_queries.items()
                if name in args.queries
            }
            missing = set(args.queries) - set(all_queries.keys())
            if missing:
                logger.warning(f"Queries not found in config: {missing}")
        else:
            queries_to_execute = all_queries

        logger.info(f"Will execute {len(queries_to_execute)} queries")

        # Mode dry-run
        if args.dry_run:
            logger.info("DRY RUN MODE - No data will be downloaded")
            for name, query in queries_to_execute.items():
                logger.info(f"  - {name}: {query.get_dataflow_key()}")
            return 0

        # Initialisation du client
        logger.info("Initializing OECD client")
        client = OECDClient()

        # Filtrage par date de mise à jour
        queries_list = list(queries_to_execute.values())
        if args.updated_since:
            logger.info(f"Filtering queries by update date: {args.updated_since}")
            queries_list = client.filter_updated_queries(
                queries_list,
                updated_since=args.updated_since
            )
        else:
            # Pas de filtrage si updated_since n'est pas spécifié (None par défaut)
            queries_list = client.filter_updated_queries(
                queries_list,
                updated_since=None
            )

        # Exécution des requêtes
        results = {}
        errors = {}

        for i, (name, query) in enumerate(queries_to_execute.items(), 1):
            if query not in queries_list:
                logger.info(f"[{i}/{len(queries_to_execute)}] Skipping '{name}' (not updated)")
                continue

            logger.info(f"[{i}/{len(queries_to_execute)}] Executing query '{name}'")

            try:
                df = client.execute_query(query)
                logger.info(f"  ✓ Retrieved {len(df)} rows")

                output_path = save_dataframe(df, name, args.output_dir, args.format)
                results[name] = {'rows': len(df), 'path': output_path}

            except Exception as e:
                logger.error(f"  ✗ Query '{name}' failed: {e}")
                errors[name] = str(e)

        # Résumé
        logger.info("=" * 80)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Successful queries: {len(results)}/{len(queries_to_execute)}")
        logger.info(f"Failed queries: {len(errors)}/{len(queries_to_execute)}")

        if results:
            logger.info("\nSuccessful downloads:")
            for name, info in results.items():
                logger.info(f"  ✓ {name}: {info['rows']} rows → {info['path']}")

        if errors:
            logger.warning("\nFailed queries:")
            for name, error in errors.items():
                logger.warning(f"  ✗ {name}: {error}")

        return 0 if not errors else 1

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
