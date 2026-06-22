"""Construction helpers for SDMX clients and query objects.

Shared factory helpers used by the download orchestration entry points (CLI
script, Kedro nodes, notebooks). Kept at the package root so the relative
imports of the provider clients and query DTOs resolve correctly and the logic
is reusable rather than duplicated in each script.
"""
# Importation des modules
import logging
import re
from typing import Any, Dict, Iterable, List, Optional, Type

import yaml

from .core.client import AbstractSDMXClient
from .core.queries import SDMXQueryRequest
from .sources.eurostat.queries import EurostatQueryRequestV30
from .sources.oecd.queries import OECDQueryRequest

# Initialisation du logger
logger = logging.getLogger(__name__)


# Table de correspondance provider → classe de requête par défaut
_QUERY_CLASSES: Dict[str, Type[SDMXQueryRequest]] = {
    "eurostat": EurostatQueryRequestV30,
    "oecd": OECDQueryRequest,
}


# Fonction de construction du client provider
def build_client(provider: str) -> AbstractSDMXClient:
    """Instantiate the SDMX client for a provider.

    Args:
        provider: ``"eurostat"`` or ``"oecd"``.

    Returns:
        A provider client instance.

    Raises:
        ValueError: For an unknown provider.
    """
    # Sélection paresseuse du client selon le provider
    if provider == "eurostat":
        from .sources.eurostat.client import EurostatClient

        return EurostatClient()
    if provider == "oecd":
        from .sources.oecd.client import OECDClient

        return OECDClient()
    raise ValueError(f"Unknown provider '{provider}'")


# Fonction de construction des requêtes à partir d'une liste de spécifications
def build_queries(
    provider: str, specs: List[Dict[str, Any]]
) -> List[SDMXQueryRequest]:
    """Build provider query objects from a list of JSON specifications.

    Each specification is passed to the provider query DTO
    :meth:`~macroforecast.datasets.core.queries.SDMXQueryRequest.from_dict`,
    which ignores keys not matching a DTO field (e.g. a human-readable
    ``"description"``) and coerces a textual ``"format"`` value into the
    provider enum.

    Args:
        provider: ``"eurostat"`` or ``"oecd"``.
        specs: List of keyword-argument dicts for the provider query DTO.

    Returns:
        List of provider query objects.

    Raises:
        ValueError: For an unknown provider.
    """
    # Sélection de la classe de requête du provider
    try:
        query_cls = _QUERY_CLASSES[provider]
    except KeyError:
        raise ValueError(f"Unknown provider '{provider}'")

    # Construction déléguée à from_dict (filtrage + coercition du format)
    return [query_cls.from_dict(spec) for spec in specs]


# Fonction de filtrage d'une liste de codes par inclusion/exclusion
def filter_codes(
    available: Iterable[str],
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    include_regex: Optional[str] = None,
    exclude_regex: Optional[str] = None,
) -> List[str]:
    """Filter a codelist by include/exclude lists and regular expressions.

    Selects the codes to keep when iterating a split dimension (e.g. the
    Eurostat ``reporter`` or ``product`` dimensions) to build the cartesian
    product of queries. The four criteria are applied in order and each is
    skipped when ``None``:

    1. ``include`` (list): keep only codes present in this list.
    2. ``include_regex``: keep only codes fully matching this pattern.
    3. ``exclude`` (list): drop these codes.
    4. ``exclude_regex``: drop codes fully matching this pattern.

    Args:
        available: Codes available in the dimension codelist.
        include: Allow-list of codes; ``None`` keeps every available code.
        exclude: Deny-list of codes; ``None`` removes nothing.
        include_regex: Pattern a code must fully match to be kept (``re.fullmatch``).
        exclude_regex: Pattern that drops a code when fully matched.

    Returns:
        Sorted list of selected codes (deterministic, duplicates removed).

    Examples:
        >>> filter_codes(
        ...     ["00", "27", "2710", "TOTAL", "2710XX", "271012"],
        ...     include_regex=r"^(\\d{2}|\\d{4}|\\d{6}|\\d{8})$",
        ...     exclude=["00"],
        ... )
        ['27', '2710', '271012']
    """
    # Déduplication des codes disponibles
    selected = {str(code) for code in available}

    # Avertissement si des codes inclus sont absents de la codelist
    # (diagnostic d'une codelist qui a évolué côté provider)
    if include is not None:
        include_set = {str(code) for code in include}
        missing = include_set - selected
        if missing:
            logger.warning(
                f"{len(missing)} included code(s) absent from the codelist: "
                f"{sorted(missing)}"
            )
        # Restriction à la liste d'inclusion
        selected &= include_set

    # Restriction au motif d'inclusion
    if include_regex is not None:
        pattern = re.compile(include_regex)
        selected = {code for code in selected if pattern.fullmatch(code)}

    # Retrait de la liste d'exclusion
    if exclude is not None:
        selected -= {str(code) for code in exclude}

    # Retrait du motif d'exclusion
    if exclude_regex is not None:
        pattern = re.compile(exclude_regex)
        selected = {code for code in selected if not pattern.fullmatch(code)}

    # Retour trié déterministe
    return sorted(selected)


# Fonction de chargement des filtres de dimensions scindées d'un dataflow
def load_split_filters(config_path: str, dataflow: str) -> Dict[str, Dict[str, Any]]:
    """Load the split-dimension filters of a dataflow from a YAML config.

    Reads a configuration file shaped like ``config/datasets/eurostat.yaml``
    and returns the ``split_filters`` section for the requested dataflow,
    mapping each split dimension to its :func:`filter_codes` keyword arguments.

    Args:
        config_path: Path to the YAML configuration file.
        dataflow: Dataflow identifier (e.g. ``"DS-045409"``).

    Returns:
        Mapping ``{dimension: {include, exclude, include_regex, exclude_regex}}``.

    Raises:
        KeyError: If the dataflow has no entry under ``split_filters``.

    Examples:
        >>> filters = load_split_filters(
        ...     "config/datasets/eurostat.yaml", "DS-045409"
        ... )  # doctest: +SKIP
        >>> filters["reporter"]["include"][:2]  # doctest: +SKIP
        ['AT', 'BE']
    """
    # Lecture du fichier YAML (motif identique au chargement OECD)
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Extraction de la section du dataflow demandé
    split_filters = config.get("split_filters", {})
    if dataflow not in split_filters:
        raise KeyError(
            f"No split_filters entry for dataflow '{dataflow}' in {config_path}"
        )
    return split_filters[dataflow]
