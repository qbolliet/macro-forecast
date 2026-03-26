"""Eurostat data client.

This module provides a high-level client for querying Eurostat data through
their SDMX API and converting responses to pandas DataFrames. Both SDMX 3.0
(primary) and SDMX 2.1 API versions are supported.
"""
# Importation des modules
# Modules de base
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from io import StringIO
import itertools
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
import xml.etree.ElementTree as ET

import pandas as pd

# Utilitaires internes au package pour la requête de données au format SDMX
from ..core.client import APIClient
from ..core.structures import (
    DataflowStructure,
    DataflowStructureRegistry,
    DimensionInfo,
)
from ..core.rate_limiter import RateLimiter

# Initialisation du logger
logger = logging.getLogger(__name__)

# Identifiant de l'agence Eurostat
AGENCY_ID = "ESTAT"


# ──────────────────────────────────────────────────────────────────────
# Types et énumérations
# ──────────────────────────────────────────────────────────────────────

# Gestion des doublons
DuplicateHandling = Literal["ignore", "warn", "raise"]

# Paramètres de requêtes de structure
StructureDetail = Literal[
    "full",
    "allstubs",
    "referencestubs",
    "allcompletestubs",
    "referencecompletestubs",
    "referencepartial",
]
StructureReferences = Literal[
    "none",
    "parents",
    "parentsandsiblings",
    "ancestors",
    "children",
    "descendants",
    "all",
]


class EurostatAPIVersion(str, Enum):
    """Supported Eurostat SDMX API versions.

    Attributes:
        V3_0: SDMX 3.0 API (primary, recommended).
        V2_1: SDMX 2.1 API (legacy, kept for compatibility testing).
    """

    V3_0 = "3.0"
    V2_1 = "2.1"


class StructureResourceType(str, Enum):
    """SDMX structure resource types queryable via ``get_structure``.

    Attributes:
        DATAFLOW: Dataflow definition.
        DATASTRUCTURE: Data Structure Definition (DSD).
        DATACONSTRAINT: Data constraint (valid dimension combinations).
        CONCEPTSCHEME: Concept scheme.
        CODELIST: Codelist (controlled vocabulary for a dimension).
    """

    DATAFLOW = "dataflow"
    DATASTRUCTURE = "datastructure"
    DATACONSTRAINT = "dataconstraint"
    CONCEPTSCHEME = "conceptscheme"
    CODELIST = "codelist"


class EurostatResponseFormat(str, Enum):
    """Response format for Eurostat SDMX data queries.

    Attributes:
        CSV: SDMX-CSV format (default).
        TSV: Tab-separated values format (legacy Eurostat).
        JSON: JSON-stat 2.0 format.
        XML: SDMX-ML XML format.
    """

    CSV = "csv"
    TSV = "tsv"
    JSON = "json"
    XML = "xml"


# ──────────────────────────────────────────────────────────────────────
# Endpoint builders (stratégie par version d'API)
# ──────────────────────────────────────────────────────────────────────


class EndpointBuilder(ABC):
    """Abstract base for version-specific URL and parameter construction.

    Subclasses implement the endpoint layout and query-parameter conventions
    for a given SDMX API version.
    """

    # ── Data endpoints ────────────────────────────────────────────────

    @abstractmethod
    def build_data_endpoint(
        self,
        dataflow: str,
        agency: str,
        version: str,
    ) -> str:
        """Build the URL path for a data query.

        Args:
            dataflow: Dataflow identifier.
            agency: Maintaining agency.
            version: Dataflow version.

        Returns:
            URL path segment (without base URL).
        """

    @abstractmethod
    def build_data_params(
        self,
        dimensions: Optional[Dict[str, List[str]]],
        start_period: Optional[str],
        end_period: Optional[str],
        last_n_observations: Optional[int],
        first_n_observations: Optional[int],
        response_format: "EurostatResponseFormat",
        compress: bool,
        attributes: Optional[str],
        measures: Optional[str],
    ) -> Dict[str, str]:
        """Build query parameters for a data request.

        Args:
            dimensions: Normalised dimension filters.
            start_period: Start period filter.
            end_period: End period filter.
            last_n_observations: Number of most-recent observations.
            first_n_observations: Number of first observations.
            response_format: Desired response format.
            compress: Whether to request gzip compression.
            attributes: Attributes selection.
            measures: Measures selection.

        Returns:
            Query-parameter dictionary.
        """

    # ── Structure endpoints ───────────────────────────────────────────

    @abstractmethod
    def build_structure_endpoint(
        self,
        resource_type: "StructureResourceType",
        resource_id: str,
        agency: str,
        version: str,
    ) -> str:
        """Build the URL path for a structure query.

        Args:
            resource_type: Type of structure artefact.
            resource_id: Artefact identifier.
            agency: Maintaining agency.
            version: Artefact version.

        Returns:
            URL path segment (without base URL).
        """

    @abstractmethod
    def build_structure_params(
        self,
        detail: StructureDetail,
        references: StructureReferences,
    ) -> Dict[str, str]:
        """Build query parameters for a structure request.

        Args:
            detail: Level of detail.
            references: Related artefacts to include.

        Returns:
            Query-parameter dictionary.
        """


class EndpointBuilderV30(EndpointBuilder):
    """Endpoint builder for the SDMX 3.0 API.

    URL patterns:
        data: ``/sdmx/3.0/data/dataflow/{agency}/{resource}/{version}``
        structure: ``/sdmx/3.0/structure/{type}/{agency}/{resource}/{version}``
    """

    # Mapping des formats de réponse vers les valeurs de paramètre API
    _FORMAT_PARAM: Dict[EurostatResponseFormat, str] = {
        EurostatResponseFormat.CSV: "csvdata",
        EurostatResponseFormat.TSV: "tsv",
        EurostatResponseFormat.JSON: "json",
        EurostatResponseFormat.XML: "structurespecificdata",
    }

    def build_data_endpoint(
        self, dataflow: str, agency: str, version: str
    ) -> str:
        return f"/sdmx/3.0/data/dataflow/{agency}/{dataflow}/{version}"

    def build_data_params(
        self,
        dimensions: Optional[Dict[str, List[str]]],
        start_period: Optional[str],
        end_period: Optional[str],
        last_n_observations: Optional[int],
        first_n_observations: Optional[int],
        response_format: EurostatResponseFormat,
        compress: bool,
        attributes: Optional[str],
        measures: Optional[str],
    ) -> Dict[str, str]:
        params: Dict[str, str] = {}

        # Filtres de dimensions (c[DIM]=val1,val2)
        if dimensions:
            for dim_name, dim_values in dimensions.items():
                params[f"c[{dim_name.upper()}]"] = ",".join(dim_values)

        # Filtre de période temporelle (c[TIME_PERIOD]=ge:...+le:...)
        time_parts: list[str] = []
        if start_period:
            time_parts.append(f"ge:{start_period}")
        if end_period:
            time_parts.append(f"le:{end_period}")
        if time_parts:
            params["c[TIME_PERIOD]"] = "+".join(time_parts)

        # Paramètres d'observations
        if last_n_observations is not None:
            params["lastNObservations"] = str(last_n_observations)
        if first_n_observations is not None:
            params["firstNObservations"] = str(first_n_observations)

        # Format et compression
        params["format"] = self._FORMAT_PARAM[response_format]
        params["compress"] = "true" if compress else "false"

        # Attributs et mesures optionnels
        if attributes:
            params["attributes"] = attributes
        if measures:
            params["measures"] = measures

        return params

    def build_structure_endpoint(
        self,
        resource_type: StructureResourceType,
        resource_id: str,
        agency: str,
        version: str,
    ) -> str:
        return (
            f"/sdmx/3.0/structure/{resource_type.value}"
            f"/{agency}/{resource_id}/{version}"
        )

    def build_structure_params(
        self,
        detail: StructureDetail,
        references: StructureReferences,
    ) -> Dict[str, str]:
        return {"detail": detail, "references": references}


class EndpointBuilderV21(EndpointBuilder):
    """Endpoint builder for the SDMX 2.1 API.

    URL patterns:
        data: ``/sdmx/2.1/data/{resource}/{key}``
        structure: ``/sdmx/2.1/{type}/{agency}/{resource}/{version}``

    Notes:
        * Dimension filtering uses a positional key in the URL path.
          The ``dimensions`` dict is ignored here and must be handled
          at a higher level (the client builds the key externally for 2.1).
        * ``dataconstraint`` is mapped to ``contentconstraint``.
        * The latest-version token is ``latest`` (not ``~``).
    """

    # Mapping des formats de réponse vers les valeurs de paramètre API 2.1
    _FORMAT_PARAM: Dict[EurostatResponseFormat, str] = {
        EurostatResponseFormat.CSV: "SDMX-CSV",
        EurostatResponseFormat.TSV: "TSV",
        EurostatResponseFormat.JSON: "JSON",
        EurostatResponseFormat.XML: "SDMX_2.1_STRUCTURED",
    }

    # Mapping des types de structure 3.0 vers les types 2.1
    _RESOURCE_MAP: Dict[StructureResourceType, str] = {
        StructureResourceType.DATAFLOW: "dataflow",
        StructureResourceType.DATASTRUCTURE: "datastructure",
        StructureResourceType.DATACONSTRAINT: "contentconstraint",
        StructureResourceType.CONCEPTSCHEME: "conceptscheme",
        StructureResourceType.CODELIST: "codelist",
    }

    def build_data_endpoint(
        self, dataflow: str, agency: str, version: str
    ) -> str:
        # Note : en 2.1, le path-key est ajouté ultérieurement par le client
        return f"/sdmx/2.1/data/{dataflow}"

    def build_data_params(
        self,
        dimensions: Optional[Dict[str, List[str]]],
        start_period: Optional[str],
        end_period: Optional[str],
        last_n_observations: Optional[int],
        first_n_observations: Optional[int],
        response_format: EurostatResponseFormat,
        compress: bool,
        attributes: Optional[str],
        measures: Optional[str],
    ) -> Dict[str, str]:
        params: Dict[str, str] = {}

        # Période temporelle (startPeriod / endPeriod)
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period

        # Paramètres d'observations
        if last_n_observations is not None:
            params["lastNObservations"] = str(last_n_observations)
        # Note : firstNObservations n'est pas supporté en 2.1

        # Format et compression
        params["format"] = self._FORMAT_PARAM[response_format]
        if compress:
            params["compress"] = "true"

        return params

    def build_structure_endpoint(
        self,
        resource_type: StructureResourceType,
        resource_id: str,
        agency: str,
        version: str,
    ) -> str:
        # Conversion du type de ressource vers la terminologie 2.1
        mapped_type = self._RESOURCE_MAP[resource_type]

        # Conversion du token de version (~ → latest)
        v21_version = "latest" if version in ("~", "*") else version

        return (
            f"/sdmx/2.1/{mapped_type}"
            f"/{agency}/{resource_id}/{v21_version}"
        )

    def build_structure_params(
        self,
        detail: StructureDetail,
        references: StructureReferences,
    ) -> Dict[str, str]:
        return {"detail": detail, "references": references}


# Registre des builders par version d'API
_ENDPOINT_BUILDERS: Dict[EurostatAPIVersion, EndpointBuilder] = {
    EurostatAPIVersion.V3_0: EndpointBuilderV30(),
    EurostatAPIVersion.V2_1: EndpointBuilderV21(),
}


# ──────────────────────────────────────────────────────────────────────
# Dataclass de requête
# ──────────────────────────────────────────────────────────────────────


@dataclass
class EurostatQueryRequest:
    """Encapsulates all parameters needed for a ``get_data()`` call.

    Attributes:
        dataflow: Dataflow identifier (e.g., ``"namq_10_gdp"``, ``"DS-045409"``).
        version: Dataflow version (default: ``"*"`` for latest).
        dimensions: Dimension filters as ``{name: value_or_list}``.
        start_period: Start period filter.
        end_period: End period filter.
        last_n_observations: Number of recent observations.
        first_n_observations: Number of first observations.
        format: Response format.
        compress: Whether to compress the response.
        attributes: Attributes to include.
        measures: Measures to include.
        on_duplicate: Duplicate handling strategy.
        split_dimensions: Dimensions to split into separate requests.
        max_split_combinations: Max allowed split combinations.

    Example:
        >>> query = EurostatQueryRequest(
        ...     dataflow="namq_10_gdp",
        ...     dimensions={"GEO": ["FR", "DE"], "FREQ": "Q"},
        ... )
        >>> df = client.execute_query(query)
    """

    dataflow: str
    version: str = "*"
    dimensions: Optional[Dict[str, Union[str, List[str]]]] = None
    start_period: Optional[str] = None
    end_period: Optional[str] = None
    last_n_observations: Optional[int] = None
    first_n_observations: Optional[int] = None
    format: EurostatResponseFormat = EurostatResponseFormat.CSV
    compress: bool = False
    attributes: Optional[str] = None
    measures: Optional[str] = None
    on_duplicate: DuplicateHandling = "warn"
    split_dimensions: Optional[List[str]] = None
    max_split_combinations: int = 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for ``get_data()`` kwargs.

        Returns:
            Dictionary of parameters.
        """
        return {
            "dataflow": self.dataflow,
            "version": self.version,
            "dimensions": self.dimensions,
            "start_period": self.start_period,
            "end_period": self.end_period,
            "last_n_observations": self.last_n_observations,
            "first_n_observations": self.first_n_observations,
            "format": self.format,
            "compress": self.compress,
            "attributes": self.attributes,
            "measures": self.measures,
            "on_duplicate": self.on_duplicate,
            "split_dimensions": self.split_dimensions,
            "max_split_combinations": self.max_split_combinations,
        }

    def get_dataflow_key(self) -> str:
        """Get unique key for this dataflow.

        Returns:
            Key in format ``'dataflow::version'``.
        """
        return f"{self.dataflow}::{self.version}"


# ──────────────────────────────────────────────────────────────────────
# Client principal
# ──────────────────────────────────────────────────────────────────────


class EurostatClient:
    """High-level client for the Eurostat SDMX API.

    Supports SDMX 3.0 (default) and SDMX 2.1 API versions. The API version
    can be switched at construction time, and the client automatically routes
    Comext datasets (``DS-*`` prefix) to the dedicated Comext endpoint.

    Args:
        api_version: SDMX API version to use (default: 3.0).
        base_url: Override for the main API base URL. When *None* the
            standard Eurostat dissemination endpoint matching ``api_version``
            is used.
        timeout: Request timeout in seconds.
        structure_registry: Optional registry for dimension-name resolution.
        auto_fetch_structure: If *True*, fetch structure metadata on demand.
        rate_limiter: Optional rate limiter for API requests.
        auto_load_rate_limit: If *True*, load rate limiter from
            ``parameters/eurostat.json``.

    Example:
        >>> client = EurostatClient()
        >>> df = client.get_data(
        ...     dataflow="namq_10_gdp",
        ...     dimensions={"GEO": ["FR"], "FREQ": "Q"},
        ... )
    """

    # URLs par défaut pour chaque version d'API
    _DEFAULT_BASE_URLS: Dict[EurostatAPIVersion, str] = {
        EurostatAPIVersion.V3_0: "https://ec.europa.eu/eurostat/api/dissemination",
        EurostatAPIVersion.V2_1: "https://ec.europa.eu/eurostat/api/dissemination",
    }

    # URLs Comext par version d'API
    _COMEXT_BASE_URLS: Dict[EurostatAPIVersion, str] = {
        EurostatAPIVersion.V3_0: "https://ec.europa.eu/eurostat/api/comext/dissemination",
        EurostatAPIVersion.V2_1: "https://ec.europa.eu/eurostat/api/comext/dissemination",
    }

    # Namespaces XML SDMX 3.0
    _SDMX3_NS = {
        "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/message",
        "str": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/structure",
        "com": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/common",
    }

    # Namespaces XML SDMX 2.1
    _SDMX21_NS = {
        "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
        "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
        "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
    }

    def __init__(
        self,
        api_version: EurostatAPIVersion = EurostatAPIVersion.V3_0,
        base_url: Optional[str] = None,
        timeout: int = 90,
        structure_registry: Optional[DataflowStructureRegistry] = None,
        auto_fetch_structure: bool = True,
        rate_limiter: Optional[RateLimiter] = None,
        auto_load_rate_limit: bool = True,
    ):
        # Version d'API et builder d'endpoints associé
        self.api_version = api_version
        self.endpoint_builder: EndpointBuilder = _ENDPOINT_BUILDERS[api_version]

        # URL de base (résolution par défaut selon la version)
        self.base_url = base_url or self._DEFAULT_BASE_URLS[api_version]
        self.auto_fetch_structure = auto_fetch_structure

        # Client HTTP principal
        self.api_client = APIClient(base_url=self.base_url, timeout=timeout)
        self._timeout = timeout

        # Client HTTP Comext (initialisation paresseuse)
        self._comext_client: Optional[APIClient] = None

        # Registre des structures de dataflows
        self.structure_registry = structure_registry or DataflowStructureRegistry()

        # Chargement automatique du rate limiter si demandé
        if auto_load_rate_limit and rate_limiter is None:
            rate_limiter = self._load_rate_limiter()
        self.rate_limiter = rate_limiter

    # ──────────────────────────────────────────────────────────────────
    # Méthodes publiques — Données
    # ──────────────────────────────────────────────────────────────────

    def get_data(
        self,
        dataflow: str,
        version: str = "*",
        dimensions: Optional[Dict[str, Union[str, List[str]]]] = None,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        first_n_observations: Optional[int] = None,
        format: EurostatResponseFormat = EurostatResponseFormat.CSV,
        compress: bool = False,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        on_duplicate: DuplicateHandling = "warn",
        split_dimensions: Optional[List[str]] = None,
        max_split_combinations: int = 100,
    ) -> pd.DataFrame:
        """Retrieve data from Eurostat.

        Args:
            dataflow: Dataflow identifier (e.g., ``"namq_10_gdp"``).
            version: Dataflow version (``"*"`` for latest).
            dimensions: Dimension filters as ``{name: value_or_list}``.
            start_period: Start period in SDMX format (e.g., ``"2020-Q1"``).
            end_period: End period in SDMX format.
            last_n_observations: Number of most-recent observations.
            first_n_observations: Number of first observations.
            format: Response format (default: CSV).
            compress: Whether to request gzip compression.
            attributes: Attributes to include (e.g., ``"dsd"``, ``"none"``).
            measures: Measures to include (e.g., ``"all"``, ``"none"``).
            on_duplicate: Duplicate handling strategy.
            split_dimensions: Dimensions to split into separate requests.
            max_split_combinations: Maximum split combinations allowed.

        Returns:
            DataFrame with retrieved data.

        Raises:
            ValueError: If data retrieval fails.
        """
        # Application du rate limiter
        if self.rate_limiter:
            self.rate_limiter.wait()

        # Normalisation des dimensions
        normalized_dims = self._normalize_dimensions(dimensions)

        # Chargement de la structure si disponible
        structure = None
        try:
            structure = self._ensure_structure(dataflow, version)
        except Exception as e:
            logger.warning(f"Could not load structure: {e}")

        # Gestion du split_dimensions
        if split_dimensions and normalized_dims:
            request_combinations = self._generate_request_combinations(
                normalized_dims, split_dimensions, max_split_combinations
            )
            return self._execute_split_requests(
                dataflow,
                version,
                request_combinations,
                start_period,
                end_period,
                last_n_observations,
                first_n_observations,
                format,
                compress,
                attributes,
                measures,
            )

        # Construction de l'endpoint et des paramètres via le builder
        endpoint = self.endpoint_builder.build_data_endpoint(
            dataflow, AGENCY_ID, version
        )
        params = self.endpoint_builder.build_data_params(
            normalized_dims,
            start_period,
            end_period,
            last_n_observations,
            first_n_observations,
            format,
            compress,
            attributes,
            measures,
        )

        # Sélection du client API (standard ou Comext)
        client = self._get_api_client(dataflow)

        # Requête des données
        try:
            response = client.get(endpoint, params=params)

            # Parsing de la réponse selon le format
            if format == EurostatResponseFormat.CSV:
                df = self._parse_csv_response(response.text)
            elif format == EurostatResponseFormat.TSV:
                df = self._parse_tsv_response(response.text)
            elif format == EurostatResponseFormat.JSON:
                df = self._parse_json_response(response.json())
            else:
                raise ValueError(f"Unsupported format: {format}")

            # Vérification des doublons
            self._check_duplicates(df, normalized_dims, structure, on_duplicate)

            # Post-filtrage si nécessaire
            if normalized_dims:
                df = self._filter_dataframe_by_dimensions(df, normalized_dims)

            logger.info(f"Retrieved {len(df)} rows from {dataflow}")
            return df
        except Exception as e:
            logger.error(f"Data retrieval failed: {e}")
            raise ValueError(f"Failed to retrieve data from {dataflow}: {e}")

    def execute_query(self, query: EurostatQueryRequest) -> pd.DataFrame:
        """Execute an ``EurostatQueryRequest``.

        Args:
            query: Query request instance.

        Returns:
            DataFrame with retrieved data.

        Raises:
            ValueError: If query execution fails.
        """
        return self.get_data(**query.to_dict())

    # ──────────────────────────────────────────────────────────────────
    # Méthodes publiques — Structure
    # ──────────────────────────────────────────────────────────────────

    def get_structure(
        self,
        resource_type: StructureResourceType,
        resource_id: str,
        agency: str = AGENCY_ID,
        version: str = "~",
        detail: StructureDetail = "full",
        references: StructureReferences = "none",
        dataflow: Optional[str] = None,
    ) -> str:
        """Query an SDMX structure artefact and return raw XML.

        This is the single entry point for all structure queries. The
        ``resource_type`` parameter selects which endpoint is targeted
        (dataflow, datastructure, dataconstraint, conceptscheme, codelist).

        Args:
            resource_type: Type of structure artefact to retrieve.
            resource_id: Artefact identifier (e.g., ``"namq_10_gdp"``).
            agency: Maintaining agency (default: ``AGENCY_ID``).
            version: Artefact version. Use ``"~"`` for latest (``"latest"``
                is used automatically when the 2.1 builder is active).
            detail: Level of detail (default: ``"full"``).
            references: Related artefacts to include (default: ``"none"``).
            dataflow: Optional dataflow hint used only to select the correct
                API client (standard vs. Comext).

        Returns:
            Raw XML response text.

        Raises:
            ValueError: If the request fails.

        Example:
            >>> xml = client.get_structure(
            ...     StructureResourceType.CODELIST, "CL_GEO"
            ... )
            >>> xml = client.get_structure(
            ...     StructureResourceType.DATAFLOW,
            ...     "namq_10_gdp",
            ...     references="descendants",
            ... )
        """
        # Construction de l'endpoint et des paramètres via le builder
        endpoint = self.endpoint_builder.build_structure_endpoint(
            resource_type, resource_id, agency, version
        )
        params = self.endpoint_builder.build_structure_params(detail, references)

        # Sélection du client API (Comext si nécessaire)
        client = self._get_api_client(dataflow or resource_id)

        try:
            response = client.get(endpoint, params=params)
            return response.text
        except Exception as e:
            logger.error(
                f"Failed to fetch {resource_type.value}/{resource_id}: {e}"
            )
            raise ValueError(
                f"Failed to fetch {resource_type.value} '{resource_id}': {e}"
            )

    def get_dataflow_structure(
        self,
        dataflow: str,
        version: str = "~",
    ) -> DataflowStructure:
        """Retrieve and parse the DSD for a dataflow.

        Convenience wrapper around ``get_structure`` that returns a parsed
        ``DataflowStructure`` instead of raw XML.

        Args:
            dataflow: Dataflow identifier.
            version: Dataflow version (``"~"`` for latest).

        Returns:
            Parsed ``DataflowStructure`` with dimension information.

        Raises:
            ValueError: If the structure cannot be retrieved or parsed.
        """
        xml_text = self.get_structure(
            StructureResourceType.DATASTRUCTURE,
            resource_id=dataflow.upper(),
            version=version,
            dataflow=dataflow,
        )
        return self._parse_structure_response(xml_text, dataflow)

    # ──────────────────────────────────────────────────────────────────
    # Méthodes publiques — Registre de structures
    # ──────────────────────────────────────────────────────────────────

    def register_structure(self, structure: DataflowStructure) -> None:
        """Register a pre-loaded structure in the internal registry.

        Args:
            structure: DataflowStructure to register.
        """
        self.structure_registry.register(structure)
        logger.info(f"Registered structure for {structure.dataflow}")

    # ──────────────────────────────────────────────────────────────────
    # Context manager et fermeture des ressources
    # ──────────────────────────────────────────────────────────────────

    def close(self) -> None:
        """Close API client connections."""
        if self.api_client:
            self.api_client.close()
        if self._comext_client:
            self._comext_client.close()
        logger.info("Eurostat client closed")

    def __enter__(self) -> "EurostatClient":
        """Context manager entry.

        Returns:
            Self for use in with statement.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit.

        Args:
            exc_type: Exception type if raised.
            exc_val: Exception value if raised.
            exc_tb: Exception traceback if raised.
        """
        self.close()

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Sélection du client API
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _is_comext_dataset(dataflow: str) -> bool:
        """Detect if a dataflow belongs to the Comext database.

        Args:
            dataflow: Dataflow identifier.

        Returns:
            True if the dataflow starts with ``'DS-'``.
        """
        return dataflow.upper().startswith("DS-")

    def _get_comext_client(self) -> APIClient:
        """Get or create the Comext API client (lazy initialisation).

        Returns:
            ``APIClient`` instance for the Comext endpoint.
        """
        if self._comext_client is None:
            comext_url = self._COMEXT_BASE_URLS[self.api_version]
            self._comext_client = APIClient(
                base_url=comext_url, timeout=self._timeout
            )
        return self._comext_client

    def _get_api_client(self, dataflow: str) -> APIClient:
        """Return the appropriate API client for a given dataflow.

        Args:
            dataflow: Dataflow identifier.

        Returns:
            Standard or Comext ``APIClient``.
        """
        if self._is_comext_dataset(dataflow):
            return self._get_comext_client()
        return self.api_client

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Configuration
    # ──────────────────────────────────────────────────────────────────

    def _load_rate_limiter(self) -> Optional[RateLimiter]:
        """Load rate limiter from ``parameters/eurostat.json``.

        Returns:
            ``RateLimiter`` instance or *None* if configuration not found.
        """
        try:
            params_path = (
                Path(__file__).parents[3] / "parameters" / "eurostat.json"
            )
            if params_path.exists():
                with open(params_path, "r", encoding="utf-8") as f:
                    config = json.load(f)
                if "RATE_LIMIT" in config:
                    rate_config = config["RATE_LIMIT"]
                    return RateLimiter(
                        requests=rate_config.get("requests", 30),
                        unit=rate_config.get("unit", "minutes"),
                        count=rate_config.get("count", 1),
                    )
            logger.debug("No RATE_LIMIT configuration found")
            return None
        except Exception as e:
            logger.warning(f"Failed to load rate limiter config: {e}")
            return None

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Parsing des réponses
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_csv_response(text: str) -> pd.DataFrame:
        """Parse an SDMX-CSV response.

        Args:
            text: CSV response text.

        Returns:
            Parsed DataFrame.

        Raises:
            ValueError: If CSV parsing fails.
        """
        try:
            return pd.read_csv(StringIO(text))
        except Exception as e:
            logger.error(f"CSV parsing failed: {e}")
            raise ValueError(f"Failed to parse CSV response: {e}")

    @staticmethod
    def _parse_tsv_response(text: str) -> pd.DataFrame:
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
            df = pd.read_csv(StringIO(text), sep="\t")
            index_col = df.columns[0]

            # Colonnes de périodes (contiennent des chiffres)
            period_cols = [
                col
                for col in df.columns[1:]
                if any(char.isdigit() for char in col)
            ]

            # Extraction des dimensions depuis la première colonne
            dimensions_split = df[index_col].str.split(",", expand=True)
            dim_names = [f"DIM_{i}" for i in range(len(dimensions_split.columns))]
            dimensions_split.columns = dim_names

            # Construction du DataFrame long
            df_wide = pd.concat(
                [dimensions_split, df[period_cols].copy()], axis=1
            )
            df_long = df_wide.melt(
                id_vars=dim_names,
                var_name="TIME_PERIOD",
                value_name="value",
            )

            # Nettoyage des valeurs (suppression des flags)
            df_long["value"] = df_long["value"].astype(str).str.strip()
            df_long["value"] = pd.to_numeric(df_long["value"], errors="coerce")

            return df_long
        except Exception as e:
            logger.error(f"TSV parsing failed: {e}")
            raise ValueError(f"Failed to parse TSV response: {e}")

    @staticmethod
    def _parse_json_response(data: Dict[str, Any]) -> pd.DataFrame:
        """Parse a JSON-stat 2.0 response.

        Args:
            data: JSON-stat dictionary.

        Returns:
            Parsed DataFrame.

        Raises:
            ValueError: If JSON parsing fails.
        """
        try:
            dimensions = data.get("dimension", {})
            observations = data.get("observation", {})

            rows = []
            for obs_key, value in observations.items():
                indices = list(map(int, obs_key.split(":")))
                row: Dict[str, Any] = {}

                for i, (dim_name, dim_info) in enumerate(dimensions.items()):
                    if i < len(indices):
                        dim_idx = indices[i]
                        if "category" in dim_info and "index" in dim_info["category"]:
                            categories = dim_info["category"]["index"]
                            if dim_idx in categories:
                                row[dim_name] = categories[dim_idx]

                row["value"] = value
                rows.append(row)

            return pd.DataFrame(rows)
        except Exception as e:
            logger.error(f"JSON parsing failed: {e}")
            raise ValueError(f"Failed to parse JSON response: {e}")

    def _parse_structure_response(
        self, xml_content: str, dataflow: str
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
            root = ET.fromstring(xml_content)

            # Tentative SDMX 3.0 puis fallback 2.1
            namespaces = self._SDMX3_NS
            structure_elem = root.find(".//str:DataStructure", namespaces)
            if structure_elem is None:
                namespaces = self._SDMX21_NS
                structure_elem = root.find(".//str:DataStructure", namespaces)

            if structure_elem is None:
                raise ValueError(
                    "DataStructure element not found in XML response"
                )

            # Extraction des dimensions
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
                    description = None

                    desc_elem = dim.find(".//com:Description", namespaces)
                    if desc_elem is not None and desc_elem.text:
                        description = desc_elem.text

                    dimensions.append(
                        DimensionInfo(
                            name=dim_id,
                            position=int(position),
                            description=description,
                        )
                    )

            return DataflowStructure(
                agency=AGENCY_ID,
                dataflow=dataflow,
                num_dimensions=len(dimensions),
                dimensions=dimensions,
                description=None,
            )
        except Exception as e:
            logger.error(f"Structure XML parsing failed: {e}")
            raise ValueError(f"Failed to parse structure response: {e}")

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Normalisation, filtrage, doublons
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _normalize_dimensions(
        dimensions: Optional[Dict[str, Union[str, List[str]]]],
    ) -> Optional[Dict[str, List[str]]]:
        """Normalize dimension values to ``Dict[str, List[str]]``.

        Args:
            dimensions: Input dimensions (may contain strings or lists).

        Returns:
            Normalised dimensions or *None*.
        """
        if not dimensions:
            return None
        return {
            k: [v] if isinstance(v, str) else v
            for k, v in dimensions.items()
        }

    @staticmethod
    def _filter_dataframe_by_dimensions(
        df: pd.DataFrame,
        filters: Optional[Dict[str, Union[str, List[str]]]],
    ) -> pd.DataFrame:
        """Post-filter a DataFrame by dimension values.

        Args:
            df: Input DataFrame.
            filters: Filters to apply.

        Returns:
            Filtered DataFrame.
        """
        if not filters or df.empty:
            return df
        result = df.copy()
        for col, values in filters.items():
            if col in result.columns:
                if isinstance(values, str):
                    values = [values]
                result = result[result[col].isin(values)]
        return result

    @staticmethod
    def _check_duplicates(
        df: pd.DataFrame,
        dimensions: Optional[Dict[str, Union[str, List[str]]]],
        structure: Optional[DataflowStructure],
        on_duplicate: DuplicateHandling,
    ) -> None:
        """Check for and optionally report duplicate rows.

        Args:
            df: DataFrame to check.
            dimensions: Dimension names.
            structure: Dataflow structure (used for column names).
            on_duplicate: Handling strategy.

        Raises:
            ValueError: If ``on_duplicate='raise'`` and duplicates found.
        """
        if df.empty or not dimensions:
            return

        dim_cols = (
            [d.name for d in structure.dimensions]
            if structure
            else list(dimensions.keys())
        )
        available_cols = [c for c in dim_cols if c in df.columns]
        if not available_cols:
            return

        check_cols = available_cols + (
            ["TIME_PERIOD"] if "TIME_PERIOD" in df.columns else []
        )
        duplicates = df[check_cols].duplicated().sum()

        if duplicates > 0:
            message = f"Found {duplicates} duplicate rows"
            if on_duplicate == "raise":
                raise ValueError(message)
            elif on_duplicate == "warn":
                logger.warning(message)

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Chargement de structure à la demande
    # ──────────────────────────────────────────────────────────────────

    def _ensure_structure(
        self, dataflow: str, version: str = "*"
    ) -> DataflowStructure:
        """Load and cache a dataflow structure.

        Args:
            dataflow: Dataflow identifier.
            version: Dataflow version.

        Returns:
            ``DataflowStructure`` instance.

        Raises:
            ValueError: If the structure cannot be retrieved.
        """
        # Vérification dans le cache
        key = f"{dataflow}::{version}"
        cached = self.structure_registry.get(key)
        if cached:
            return cached

        # Chargement via API si non cachée
        if self.auto_fetch_structure:
            structure = self.get_dataflow_structure(dataflow, version)
            self.register_structure(structure)
            return structure

        raise ValueError(f"Structure not found for {dataflow}::{version}")

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Requêtes splitées
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _generate_request_combinations(
        dimensions: Dict[str, List[str]],
        split_dims: List[str],
        max_combinations: int,
    ) -> List[Dict[str, List[str]]]:
        """Generate dimension combinations for split requests.

        Args:
            dimensions: Normalised dimensions.
            split_dims: Dimensions to split.
            max_combinations: Maximum allowed combinations.

        Returns:
            List of dimension dictionaries, one per combination.

        Raises:
            ValueError: If combinations exceed *max_combinations*.
        """
        split_dims_set = set(split_dims)
        split_dict = {k: v for k, v in dimensions.items() if k in split_dims_set}
        keep_dict = {k: v for k, v in dimensions.items() if k not in split_dims_set}

        split_keys = list(split_dict.keys())
        split_values = [split_dict[k] for k in split_keys]
        combinations = list(itertools.product(*split_values))

        if len(combinations) > max_combinations:
            raise ValueError(
                f"Split combinations ({len(combinations)}) exceed "
                f"max allowed ({max_combinations})"
            )

        result = []
        for combo in combinations:
            dims = keep_dict.copy()
            for key, val in zip(split_keys, combo):
                dims[key] = [val]
            result.append(dims)
        return result

    def _execute_split_requests(
        self,
        dataflow: str,
        version: str,
        request_combinations: List[Dict[str, List[str]]],
        start_period: Optional[str],
        end_period: Optional[str],
        last_n_observations: Optional[int],
        first_n_observations: Optional[int],
        response_format: EurostatResponseFormat,
        compress: bool,
        attributes: Optional[str],
        measures: Optional[str],
    ) -> pd.DataFrame:
        """Execute multiple split requests and concatenate results.

        Args:
            dataflow: Dataflow identifier.
            version: Dataflow version.
            request_combinations: List of dimension combinations.
            start_period: Start period.
            end_period: End period.
            last_n_observations: Number of last observations.
            first_n_observations: Number of first observations.
            response_format: Response format.
            compress: Whether to compress.
            attributes: Attributes to include.
            measures: Measures to include.

        Returns:
            Concatenated DataFrame from all requests.
        """
        dfs: list[pd.DataFrame] = []
        for dims in request_combinations:
            try:
                df = self.get_data(
                    dataflow=dataflow,
                    version=version,
                    dimensions=dims,
                    start_period=start_period,
                    end_period=end_period,
                    last_n_observations=last_n_observations,
                    first_n_observations=first_n_observations,
                    format=response_format,
                    compress=compress,
                    attributes=attributes,
                    measures=measures,
                    on_duplicate="ignore",
                    split_dimensions=None,
                )
                dfs.append(df)
            except Exception as e:
                logger.error(f"Split request failed for {dims}: {e}")
                continue

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()