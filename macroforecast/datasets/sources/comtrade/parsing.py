"""UN Comtrade parsing helpers.

Pure functions converting Comtrade reference/availability responses and the
parameter declarations into the package's shared structures. Kept free of any
HTTP or client state so they can be unit-tested in isolation, mirroring
``eurostat.parsing`` and ``oecd.parsing``.
"""
# Importation des modules
# Modules de base
from typing import Any, Dict, List, Optional

# Modules du package
from ...core.structures import DataflowStructure, DimensionInfo


# Fonction de construction d'une structure de dataflow depuis les paramètres
def build_structure_from_parameters(
    parameters: Dict[str, Any],
    agency: str,
    dataflow: str,
) -> DataflowStructure:
    """Build a :class:`DataflowStructure` for a Comtrade dataflow.

    UN Comtrade exposes no structure endpoint, so the dataflow dimensions are
    declared in ``parameters/comtrade.json``. The canonical structure is read
    from the ``STRUCTURES`` registry section when an entry matches; otherwise it
    is derived from the ``FLOW_COLUMNS`` identifier columns (so any
    ``typeCode_freqCode_clCode`` dataflow shares the same dimension layout).

    Args:
        parameters: Parsed contents of ``parameters/comtrade.json``.
        agency: Maintaining agency (``"COMTRADE"``).
        dataflow: Logical dataflow identifier (e.g. ``"C_A_HS"``).

    Returns:
        A :class:`DataflowStructure` whose dimensions are the tariffline
        identifier columns.

    Raises:
        ValueError: If neither a matching ``STRUCTURES`` entry nor a
            ``FLOW_COLUMNS`` list is available.

    Examples:
        >>> params = {"FLOW_COLUMNS": ["typeCode", "period", "reporterISO"]}
        >>> structure = build_structure_from_parameters(params, "COMTRADE", "C_A_HS")
        >>> structure.get_position("reporterISO")
        2
    """
    # Recherche d'une entrée STRUCTURES correspondant exactement au dataflow
    for entry in parameters.get("STRUCTURES", []):
        if entry.get("agency") == agency and entry.get("dataflow") == dataflow:
            return DataflowStructure.from_dict(entry)

    # Repli : construction depuis les colonnes identifiantes FLOW_COLUMNS
    flow_columns: List[str] = parameters.get("FLOW_COLUMNS", [])
    if not flow_columns:
        raise ValueError(
            "Cannot build a Comtrade structure: neither a matching STRUCTURES "
            "entry nor a FLOW_COLUMNS list was found in the parameters."
        )

    # Construction des dimensions à partir de l'ordre des colonnes identifiantes
    dimensions = [
        DimensionInfo(name=name, position=position)
        for position, name in enumerate(flow_columns)
    ]
    return DataflowStructure(
        agency=agency,
        dataflow=dataflow,
        num_dimensions=len(dimensions),
        dimensions=dimensions,
        description="UN Comtrade tariffline data (derived from FLOW_COLUMNS).",
    )


# Fonction d'extraction des codes valides d'un jeu de métadonnées
def extract_codes(df, category: str) -> List[Any]:
    """Extract the valid codes of a reference category from its metadata.

    Args:
        df: Metadata DataFrame returned by ``ComtradeClient.get_metadata``.
        category: Reference category. One of ``"flow"``, ``"reporter"``,
            ``"partner"`` or ``"cmd:HS"``.

    Returns:
        List of valid codes for the category (expired entries are dropped for
        reporters and partners).

    Raises:
        ValueError: If ``category`` is not supported.

    Examples:
        >>> extract_codes(flow_metadata, "flow")  # doctest: +SKIP
        ['M', 'X', ...]
    """
    # Flux et nomenclature : codes dans la colonne "id"
    if category in ("flow", "cmd:HS"):
        return df["id"].tolist()
    # Reporters : codes des pays non expirés
    if category == "reporter":
        return df.loc[df["entryExpiredDate"].isna(), "reporterCode"].tolist()
    # Partenaires : codes des pays non expirés
    if category == "partner":
        return df.loc[df["entryExpiredDate"].isna(), "PartnerCode"].tolist()
    # Catégorie non supportée
    raise ValueError(
        f"Unsupported category '{category}'. "
        "Expected one of 'flow', 'reporter', 'partner', 'cmd:HS'."
    )


# Fonction d'extraction de la date de dernière publication d'une disponibilité
def parse_availability_last_released(
    availability,
) -> Dict[str, Optional[str]]:
    """Map each period to its ``lastReleased`` date from an availability frame.

    Reads the DataFrame returned by ``getFinalDataAvailability`` and produces a
    ``{period: lastReleased}`` mapping used by the download script to decide
    whether a (reporter, period) couple must be refreshed.

    Args:
        availability: DataFrame returned by
            ``ComtradeClient.get_final_data_availability`` (expects ``period``
            and ``lastReleased`` columns).

    Returns:
        Mapping of period (as ``str``) to its last-released date (``str`` or
        ``None``). Empty when the input is empty or lacks the columns.
    """
    # Court-circuit si le jeu de données est vide ou incomplet
    if (
        availability is None
        or availability.empty
        or "period" not in availability.columns
        or "lastReleased" not in availability.columns
    ):
        return {}

    # Construction du dictionnaire période → date de dernière publication
    return {
        str(period): (None if released is None or released != released else str(released))
        for period, released in zip(
            availability["period"], availability["lastReleased"]
        )
    }
