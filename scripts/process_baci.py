"""Script de redressement BACI des flux de commerce international.

Lit les déclarations brutes COMTRADE depuis un catalogue DuckLake, applique la
méthodologie de reconstruction BACI du CEPII (conversion en tonnes, estimation et
retrait des coûts de fret, évaluation de la qualité des déclarants, réconciliation
pondérée des flux miroirs, réallocation des zones non spécifiées) et écrit les flux
réconciliés dans une nouvelle base DuckLake résultat.

Tous les chemins proviennent de ``config/baci.yaml`` (jamais écrits en dur) ;
``BUCKET`` y est explicitement à ``null`` et passé au loader Excel des fichiers
CEPII, pour préparer une future migration S3. Le chargement de la configuration
YAML, la construction de la ``BaciConfig`` et la lecture des fichiers Excel CEPII
sont réalisés ici : ``macroforecast.trade.processing.baci`` ne reçoit que des
jeux de données déjà chargés et des paramètres déjà résolus. Peut être
ordonnancé (Argo, cron) ou intégré comme nœud Kedro via la fonction exportée.
"""

import argparse
import logging
import sys
from dataclasses import replace
from pathlib import Path
from typing import Dict, Optional

import yaml

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

    Only the keys present in the mapping override the dataclass defaults; every
    other field keeps its ``BaciConfig`` default. Country lists and pairs are
    coerced to the tuple types expected by the frozen dataclass.

    Args:
        params: The ``parameters`` mapping of ``config/baci.yaml`` (or ``None``).

    Returns:
        A ``BaciConfig`` reflecting the configured overrides.
    """
    from macroforecast.trade.processing import DEFAULT_CONFIG

    # Aucune surcharge : configuration par défaut
    if not params:
        return DEFAULT_CONFIG

    overrides: Dict[str, object] = {}
    # Variable de distance
    if params.get("distance_column"):
        overrides["distance_column"] = params["distance_column"]
    # Seuils de conversion en tonnes
    tonnage = params.get("tonnage") or {}
    if "min_mirror_flows" in tonnage:
        overrides["min_mirror_flows"] = int(tonnage["min_mirror_flows"])
    if "max_std" in tonnage:
        overrides["max_conversion_std"] = float(tonnage["max_std"])
    # Robustesse de la gravité
    gravity = params.get("gravity") or {}
    if "cook_factor" in gravity:
        overrides["cook_factor"] = float(gravity["cook_factor"])
    # Listes de pays
    countries = params.get("countries") or {}
    if "non_cif" in countries:
        overrides["non_cif_countries"] = tuple(countries["non_cif"])
    if "fas" in countries:
        overrides["fas_countries"] = tuple(countries["fas"])
    # Exclusions géographiques
    exclusions = params.get("exclusions") or {}
    if "reexport_reporters" in exclusions:
        overrides["reexport_reporters"] = tuple(exclusions["reexport_reporters"])
    if "excluded_pairs" in exclusions:
        overrides["excluded_pairs"] = tuple(tuple(p) for p in exclusions["excluded_pairs"])
    # Zones non spécifiées
    nes = params.get("nes") or {}
    if "partner_codes" in nes:
        overrides["nes_partner_codes"] = tuple(int(c) for c in nes["partner_codes"])
    if "skip_codes" in nes:
        overrides["nes_skip_codes"] = tuple(int(c) for c in nes["skip_codes"])

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
    from macroforecast.trade.processing import run_baci

    # Chargement de la configuration (chemins + paramètres méthodologiques)
    config = load_baci_config(config_path)
    paths = config["paths"]
    bucket = config["BUCKET"]
    baci_config = baci_config_from_params(config.get("parameters"))

    # Lecture des fichiers Excel CEPII (loader local/S3 selon BUCKET)
    excel_loader = Loader()
    dist = excel_loader.load(_resolve(paths["dist_cepii"]), bucket=bucket)
    geo = excel_loader.load(_resolve(paths["geo_cepii"]), bucket=bucket)

    # Exécution du redressement sur les jeux de données déjà chargés
    report = run_baci(
        source_catalog=_resolve(paths["comtrade_catalog"]),
        source_data_path=_resolve(paths["comtrade_data"]),
        result_catalog=_resolve(paths["result_catalog"]),
        result_data_path=_resolve(paths["result_data"]),
        dist=dist,
        geo=geo,
        source_schema=paths["comtrade_schema"],
        result_schema=paths["result_schema"],
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
