"""UN Comtrade formats, constants and enumerations.

UN Comtrade does not follow the SDMX conventions of the Eurostat and OECD
APIs, so this module gathers the provider-specific constants (agency id,
response formats, frequency mapping, symmetric-flow code maps) that the SDMX
clients would otherwise read from the SDMX structure endpoints.
"""
# Importation des modules
from typing import Dict, List

# Module du package
from ...core.sdmx import SDMXResponseFormat


# Identifiant d'agence (par symétrie avec ESTAT / OECD)
AGENCY_ID = "COMTRADE"

# Codes par défaut de l'API Comtrade
DEFAULT_TYPE_CODE = "C"  # Commodities (S pour les services)
DEFAULT_CLASSIFICATION = "HS"  # Nomenclature douanière (HS, SITC, BEC, EBOPS)

# Correspondance fréquence métier → code API Comtrade
FREQUENCY_TO_CODE: Dict[str, str] = {
    "annual": "A",
    "monthly": "M",
}

# Fréquences valides acceptées par le client
VALID_FREQUENCIES: List[str] = list(FREQUENCY_TO_CODE.keys())


# Format de réponse de l'API Comtrade
class ComtradeResponseFormat(SDMXResponseFormat):
    """Response formats supported by the UN Comtrade API.

    Attributes:
        JSON: JSON response (default for ``comtradeapicall``).
        CSV: CSV response.
    """
    JSON = "json"
    CSV = "csv"


# ──────────────────────────────────────────────────────────────────────
# Flux symétriques (import / export)
# ──────────────────────────────────────────────────────────────────────

# Codes de flux relevant des imports
IMPORT_FLOW_CODES: List[str] = ["M", "FM", "MIP", "MOP", "RM"]
# Codes de flux relevant des exports
EXPORT_FLOW_CODES: List[str] = ["X", "DX", "RX", "XIP", "XOP"]

# Conversion d'un code de flux export vers son équivalent import
EXPORT_TO_IMPORT_CODE: Dict[str, str] = {
    "X": "M",
    "DX": "FM",
    "RX": "RM",
    "XIP": "MIP",
    "XOP": "MOP",
}
# Conversion d'un code de flux import vers son équivalent export
IMPORT_TO_EXPORT_CODE: Dict[str, str] = {
    "M": "X",
    "FM": "DX",
    "MIP": "XIP",
    "MOP": "XOP",
    "RM": "RX",
}

# Conversion des libellés de flux export → import
EXPORT_TO_IMPORT_DESC: Dict[str, str] = {
    "Export": "Import",
    "Domestic Export": "Foreign Import",
    "Re-export": "Re-import",
    "Export of goods after inward processing": "Import of goods for inward processing",
    "Export of goods for outward processing": "Import of goods after outward processing",
}
# Conversion des libellés de flux import → export
IMPORT_TO_EXPORT_DESC: Dict[str, str] = {
    "Import": "Export",
    "Foreign Import": "Domestic Export",
    "Re-import": "Re-export",
    "Import of goods for inward processing": "Export of goods after inward processing",
    "Import of goods after outward processing": "Export of goods for outward processing",
}
