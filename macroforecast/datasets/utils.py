"""Construction helpers for SDMX clients and query objects.

Shared factory helpers used by the download orchestration entry points (CLI
script, Kedro nodes, notebooks). Kept at the package root so the relative
imports of the provider clients and query DTOs resolve correctly and the logic
is reusable rather than duplicated in each script.
"""
# Importation des modules
from typing import Any, Dict, List, Type

from .core.client import AbstractSDMXClient
from .core.queries import SDMXQueryRequest
from .sources.eurostat.queries import EurostatQueryRequestV30
from .sources.oecd.queries import OECDQueryRequest


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
