"""Script de redressement BACI des flux de commerce international.

Lit les déclarations brutes COMTRADE depuis un catalogue DuckLake, applique la
méthodologie de reconstruction BACI du CEPII (conversion en tonnes, estimation et
retrait des coûts de fret, évaluation de la qualité des déclarants, réconciliation
pondérée des flux miroirs, réallocation des zones non spécifiées) et écrit les flux
réconciliés dans une nouvelle base DuckLake résultat.

Tous les chemins proviennent de ``config/baci.yaml`` (jamais écrits en dur) ;
``BUCKET`` y est explicitement à ``null`` et passé via ``bucket=config['BUCKET']``
au loader des fichiers CEPII, pour préparer une future migration S3. Peut être
ordonnancé (Argo, cron) ou intégré comme nœud Kedro via la fonction exportée.
"""

import argparse
import logging
import sys
from pathlib import Path

# Racine du dépôt (résolution relative à ce fichier)
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


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


def run(config_path: str, apply_nes: bool = True):
    """Run the BACI reconstruction from a YAML configuration file.

    Args:
        config_path: Path to ``config/baci.yaml``.
        apply_nes: Whether to apply the "Areas NES" reallocation step.

    Returns:
        The :class:`~macroforecast.trade.processing.BaciReport` of the run.
    """
    from macroforecast.trade.processing import (
        baci_config_from_params,
        load_baci_config,
        run_baci,
    )

    # Chargement de la configuration (chemins + paramètres méthodologiques)
    config = load_baci_config(config_path)
    paths = config["paths"]
    baci_config = baci_config_from_params(config.get("parameters"))

    # Exécution du redressement (BUCKET passé tel quel au loader CEPII)
    report = run_baci(
        source_catalog=_resolve(paths["comtrade_catalog"]),
        source_data_path=_resolve(paths["comtrade_data"]),
        result_catalog=_resolve(paths["result_catalog"]),
        result_data_path=_resolve(paths["result_data"]),
        dist_path=_resolve(paths["dist_cepii"]),
        geo_path=_resolve(paths["geo_cepii"]),
        source_schema=paths["comtrade_schema"],
        result_schema=paths["result_schema"],
        bucket=config["BUCKET"],
        config=baci_config,
        apply_nes=apply_nes,
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
