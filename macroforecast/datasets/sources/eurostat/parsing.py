"""Eurostat response parsing helpers.

Pure functions parsing the various Eurostat API responses into the project's
data structures: gzip decompression, SDMX-CSV/TSV/JSON-stat data, and SDMX-ML
structure / dataflow-catalogue responses. They carry no client state and are
therefore exposed as module-level functions rather than methods.
"""
# Importation des modules
import gzip
from io import StringIO
import logging
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET

import pandas as pd

from ...core.structures import DataflowStructure, DimensionInfo
from .formats import AGENCY_ID

# Initialisation du logger
logger = logging.getLogger(__name__)

# Namespaces XML SDMX 3.0
_SDMX3_NS = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/common",
}

# Namespaces XML SDMX 2.1 (fallback)
_SDMX21_NS = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}


# Fonction de décompression transparente des réponses gzip
def decompress_response_bytes(content: bytes) -> bytes:
    """Transparently decompress response bytes if gzip-encoded.

    Some Eurostat API endpoints return gzip-compressed content when the
    ``compress=true`` query parameter is set, or by default for large
    structure responses.  This function inspects the magic bytes and
    decompresses only when necessary, so it is safe to call on any
    response regardless of whether compression was requested.

    Args:
        content: Raw response bytes, possibly gzip-compressed.

    Returns:
        Decompressed bytes, or the original bytes unchanged if the
        content is not gzip-compressed.
    """
    # Détection de la compression gzip par les octets magiques (0x1F 0x8B)
    if content[:2] == b"\x1f\x8b":
        return gzip.decompress(content)
    return content


# Fonction de parsing du format TSV Eurostat (format large avec flags)
def parse_tsv_response(text: str) -> pd.DataFrame:
    """Parse Eurostat TSV format (wide format with flags).

    The first column contains dimensions separated by commas, followed
    by tab-separated period columns.

    Args:
        text: TSV response text.

    Returns:
        Parsed DataFrame in long (tidy) format.

    Raises:
        ValueError: If TSV parsing fails.
    """
    try:
        # Lecture du TSV avec séparateur tabulation
        df = pd.read_csv(StringIO(text), sep="\t")
        index_col = df.columns[0]

        # Sélection des colonnes de périodes (contiennent des chiffres)
        period_cols = [
            col
            for col in df.columns[1:]
            if any(char.isdigit() for char in col)
        ]

        # Extraction des dimensions depuis la première colonne composite
        dimensions_split = df[index_col].str.split(",", expand=True)
        dim_names = [f"DIM_{i}" for i in range(len(dimensions_split.columns))]
        dimensions_split.columns = dim_names

        # Reconstruction du DataFrame en format large puis conversion en format long
        df_wide = pd.concat(
            [dimensions_split, df[period_cols].copy()], axis=1
        )
        df_long = df_wide.melt(
            id_vars=dim_names,
            var_name="TIME_PERIOD",
            value_name="value",
        )

        # Nettoyage des valeurs (suppression des flags et conversion numérique)
        df_long["value"] = df_long["value"].astype(str).str.strip()
        df_long["value"] = pd.to_numeric(df_long["value"], errors="coerce")

        return df_long
    # Gestion des erreurs de parsing
    except Exception as e:
        logger.error(f"TSV parsing failed: {e}")
        raise ValueError(f"Failed to parse TSV response: {e}")


# Fonction de parsing de réponse JSON-stat 2.0
def parse_json_response(data: Dict[str, Any]) -> pd.DataFrame:
    """Parse a JSON-stat 2.0 response.

    Eurostat serialises responses in JSON-stat 2.0:

    - ``id``: ordered list of dimension identifiers.
    - ``size``: number of categories per dimension, same order as ``id``.
    - ``dimension[dim_id].category.index``: either a ``{code: position}``
      mapping or an ordered list of codes.
    - ``value``: observations indexed by a single flat row-major position
      over the multi-dimensional array described by ``size``. May be
      serialised as a ``{position_string: value}`` dictionary (sparse)
      or as a plain list (dense, with ``None`` for missing values).

    Args:
        data: JSON-stat dictionary.

    Returns:
        Parsed DataFrame with one column per dimension (using the
        dimension code, e.g. ``"FR"``, ``"M"``, ``"2024-01"``) plus a
        ``value`` column. Empty when the response has no observations.

    Raises:
        ValueError: If JSON parsing fails.
    """
    try:
        # Récupération de l'ordre des dimensions, de leurs tailles et des valeurs
        dim_ids: List[str] = list(data.get("id", []))
        sizes: List[int] = list(data.get("size", []))
        dimensions: Dict[str, Any] = data.get("dimension", {})
        values = data.get("value", {})

        # Réponse vide ou mal formée
        if not dim_ids or not sizes or len(dim_ids) != len(sizes):
            return pd.DataFrame(columns=[*dim_ids, "value"])

        # Construction d'un mapping position -> code pour chaque dimension
        codes_by_dim: Dict[str, List[Optional[str]]] = {}
        for dim_id, dim_size in zip(dim_ids, sizes):
            cat_index = (
                dimensions.get(dim_id, {}).get("category", {}).get("index", {})
            )
            # Format objet {code: position} → inversion en liste ordonnée
            if isinstance(cat_index, dict):
                pos_to_code: List[Optional[str]] = [None] * dim_size
                for code, pos in cat_index.items():
                    pos_int = int(pos)
                    if 0 <= pos_int < dim_size:
                        pos_to_code[pos_int] = code
                codes_by_dim[dim_id] = pos_to_code
            # Format tableau : codes déjà ordonnés
            elif isinstance(cat_index, list):
                codes_by_dim[dim_id] = list(cat_index)
            else:
                codes_by_dim[dim_id] = [None] * dim_size

        # Normalisation des observations en itérable (index_plat, valeur)
        # JSON-stat 2.0 autorise un dict sparse ou une list dense
        if isinstance(values, dict):
            obs_items = ((int(k), v) for k, v in values.items())
        else:
            obs_items = (
                (i, v) for i, v in enumerate(values) if v is not None
            )

        # Décomposition row-major de l'index plat en indices multidimensionnels
        n_dims = len(sizes)
        rows: List[Dict[str, Any]] = []
        for flat_idx, value in obs_items:
            multi_idx = [0] * n_dims
            remainder = flat_idx
            for axis in range(n_dims - 1, -1, -1):
                multi_idx[axis] = remainder % sizes[axis]
                remainder //= sizes[axis]

            # Mapping de chaque indice de dimension vers son code
            row: Dict[str, Any] = {}
            for axis, dim_id in enumerate(dim_ids):
                codes = codes_by_dim[dim_id]
                idx = multi_idx[axis]
                row[dim_id] = codes[idx] if 0 <= idx < len(codes) else None
            row["value"] = value
            rows.append(row)

        return pd.DataFrame(rows, columns=[*dim_ids, "value"])
    # Gestion des erreurs de parsing
    except Exception as e:
        # Logging
        logger.error(f"JSON parsing failed: {e}")
        raise ValueError(f"Failed to parse JSON response: {e}")


# Fonction de parsing d'une réponse SDMX-ML et d'extraction des dimensions
def parse_structure_response(
    xml_content: str, dataflow: str
) -> DataflowStructure:
    """Parse an SDMX-ML structure response and extract dimensions.

    Tries SDMX 3.0 namespaces first, then falls back to 2.1.

    Args:
        xml_content: XML response content.
        dataflow: Dataflow identifier.

    Returns:
        ``DataflowStructure`` instance.

    Raises:
        ValueError: If XML parsing fails.
    """
    try:
        # Parsing du document XML
        root = ET.fromstring(xml_content)

        # Tentative avec les namespaces SDMX 3.0 puis fallback vers 2.1
        namespaces = _SDMX3_NS
        structure_elem = root.find(".//str:DataStructure", namespaces)
        if structure_elem is None:
            namespaces = _SDMX21_NS
            structure_elem = root.find(".//str:DataStructure", namespaces)

        # Vérification de la présence de l'élément DataStructure
        if structure_elem is None:
            raise ValueError(
                "DataStructure element not found in XML response"
            )

        # Construction d'un index id -> nom depuis les ConceptSchemes
        # (les dimensions ne portent pas de description directement :
        #  elles référencent un Concept via ConceptIdentity)
        concept_names: dict[str, str] = {}
        for concept in root.findall(".//str:Concept", namespaces):
            concept_id = concept.get("id")
            if not concept_id:
                continue
            # Extraction du nom
            name: str | None = None
            for name_elem in concept.findall("com:Name", namespaces):
                name = name_elem.text
            concept_names[concept_id] = name

        # Extraction de la liste des dimensions depuis le DSD
        dimensions: list[DimensionInfo] = []
        dimension_list = structure_elem.find(
            ".//str:DimensionList", namespaces
        )
        if dimension_list is not None:
            for i, dim in enumerate(
                dimension_list.findall("str:Dimension", namespaces)
            ):
                dim_id = dim.get("id")
                position = dim.get("position", str(i))

                # Résolution de la description via le ConceptScheme
                description = concept_names.get(dim_id)

                dimensions.append(
                    DimensionInfo(
                        name=dim_id,
                        position=int(position),
                        description=description,
                    )
                )

        # Construction et retour de la structure de dataflow
        return DataflowStructure(
            agency=AGENCY_ID,
            dataflow=dataflow,
            num_dimensions=len(dimensions),
            dimensions=dimensions,
            description=None,
        )
    # Gestion des erreurs de parsing XML
    except Exception as e:
        logger.error(f"Structure XML parsing failed: {e}")
        raise ValueError(f"Failed to parse structure response: {e}")


# Fonction de parsing d'une réponse SDMX-ML contenant une liste de dataflows
def parse_dataflow_list_response(xml_content: str) -> pd.DataFrame:
    """Parse an SDMX-ML structure response containing multiple dataflows.

    Tries SDMX 3.0 namespaces first, then falls back to 2.1.
    Extracts the English name for each dataflow when available.

    Args:
        xml_content: XML response content (already decompressed).

    Returns:
        DataFrame with columns: ``id``, ``name``, ``version``,
        ``agency``.

    Raises:
        ValueError: If XML parsing or element extraction fails.
    """
    try:
        # Parsing du document XML
        root = ET.fromstring(xml_content)

        # Tentative avec les namespaces SDMX 3.0 puis fallback 2.1
        namespaces = _SDMX3_NS
        dataflows = root.findall(".//str:Dataflow", namespaces)
        if not dataflows:
            namespaces = _SDMX21_NS
            dataflows = root.findall(".//str:Dataflow", namespaces)

        # Extraction des métadonnées de chaque dataflow
        rows = []
        for df_elem in dataflows:
            df_id = df_elem.get("id")
            df_agency = df_elem.get("agencyID")
            df_version = df_elem.get("version")

            # Extraction du nom anglais, ou première langue disponible
            name: Optional[str] = None
            for name_elem in df_elem.findall("com:Name", namespaces):
                lang = name_elem.get(
                    "{http://www.w3.org/XML/1998/namespace}lang", ""
                )
                if name is None or lang == "en":
                    name = name_elem.text

            rows.append(
                {
                    "id": df_id,
                    "name": name,
                    "version": df_version,
                    "agency": df_agency,
                }
            )

        # Logging
        logger.info(f"Parsed {len(rows)} dataflows from catalogue response")
        return pd.DataFrame(rows)
    # Gestion des erreurs de parsing XML
    except Exception as e:
        logger.error(f"Dataflow catalogue parsing failed: {e}")
        raise ValueError(f"Failed to parse dataflow catalogue: {e}")
