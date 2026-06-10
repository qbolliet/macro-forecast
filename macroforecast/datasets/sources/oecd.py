"""OECD data client.

This module provides a high-level client for querying OECD data through
their SDMX API and converting responses to pandas DataFrames.
"""
# Importation des modules
# Modules de base
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from io import StringIO
import logging
from typing import Optional, Dict, List, Any, Union, Literal, Tuple
import json
import warnings
from pathlib import Path
import xml.etree.ElementTree as ET
import pandas as pd
import operator
from functools import reduce
import itertools

# Utilitaires internes au package pour la requête de données au format SDMX
from ..core.client import AbstractSDMXClient, APIClient
from ..core.sdmx import (
    DimensionAtObservation,
    DuplicateHandling,
    SDMXEndpointBuilder,
    SDMXResponseFormat,
    SDMXVersion,
)
from ..core.structures import (
    DataflowStructure,
    DataflowStructureRegistry,
    DimensionInfo,
)
from ..core.rate_limiter import RateLimiter

# Initialisation du logger
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Types et énumérations
# ──────────────────────────────────────────────────────────────────────

# DuplicateHandling est défini dans core.sdmx et importé ci-dessus

# Énumération des formats de réponse pour les requêtes de données OECD
class OECDResponseFormat(SDMXResponseFormat):
    """Response format options for the OECD API.

    Attributes:
        JSON: JSON format (jsondata parameter).
        CSV: CSV format without labels (csvfile parameter).
        CSV_LABELS: CSV format with labels (csvfilewithlabels parameter).
        XML: XML generic data format (genericdata parameter).
    """

    JSON = "json"
    CSV = "csv"
    CSV_LABELS = "csv_labels"
    XML = "xml"


# ──────────────────────────────────────────────────────────────────────
# Dataclass de requête interne (DTO)
# ──────────────────────────────────────────────────────────────────────

# Classe spécifiant les paramètres d'une requête de données OECD
@dataclass
class OECDDataQuery:
    """OECD SDMX data query parameters.

    Internal DTO used by :class:`OECDEndpointBuilder` and
    :class:`OECDClient` to carry all parameters for a single API request.

    Args:
        agency: Agency identifier (e.g., ``'OECD.SDD.STES'``).
        dataflow: Dataflow identifier (e.g., ``'DSD_KEI@DF_KEI'``).
        version: Dataflow version (default: ``'+'`` for latest).
        dimensions: Dimension position → values mapping.
        start_period: Start time period (inclusive).
        end_period: End time period (inclusive).
        last_n_observations: Number of recent observations to retrieve.
        format: Response format.
        sdmx_version: SDMX API version to use.
        dimension_at_observation: Dimension to present at observation level.
        num_dimensions: Total number of dimensions (for URL padding).
        attributes: Attributes to include (``"dsd"``, ``"all"``, ``"none"``).
        measures: Measures to include (``"all"``, ``"none"``).

    Example:
        >>> query = OECDDataQuery(
        ...     agency="OECD",
        ...     dataflow="KEI",
        ...     dimensions={0: ["FRA", "DEU"], 1: ["PRINTO01"]},
        ...     num_dimensions=7,
        ... )
    """

    # Attributs obligatoires
    agency: str
    dataflow: str

    # Attributs optionnels avec valeurs par défaut
    version: str = "1.0"
    dimensions: Dict[int, List[str]] = None
    start_period: Optional[str] = None
    end_period: Optional[str] = None
    last_n_observations: Optional[int] = None
    format: OECDResponseFormat = OECDResponseFormat.JSON
    sdmx_version: SDMXVersion = SDMXVersion.V1
    dimension_at_observation: DimensionAtObservation = DimensionAtObservation.ALL_DIMENSIONS
    num_dimensions: Optional[int] = None
    attributes: Optional[str] = None
    measures: Optional[str] = None

    def __post_init__(self):
        if self.dimensions is None:
            self.dimensions = {}

    # Méthode convertissant les attributs en dictionnaire
    def to_dict(self) -> Dict[str, Any]:
        """Convert query to dictionary representation.

        Returns:
            Dictionary containing all query parameters.
        """
        return {
            "agency": self.agency,
            "dataflow": self.dataflow,
            "version": self.version,
            "dimensions": self.dimensions,
            "start_period": self.start_period,
            "end_period": self.end_period,
            "last_n_observations": self.last_n_observations,
            "format": self.format.value,
            "sdmx_version": self.sdmx_version.value,
            "dimension_at_observation": self.dimension_at_observation.value,
            "num_dimensions": self.num_dimensions,
            "attributes": self.attributes,
            "measures": self.measures,
        }


# ──────────────────────────────────────────────────────────────────────
# Endpoint builder OECD
# ──────────────────────────────────────────────────────────────────────

# Mapping des formats vers les valeurs de paramètre API OECD
_OECD_FORMAT_PARAM_MAP = {
    OECDResponseFormat.JSON: "jsondata",
    OECDResponseFormat.CSV: "csvfile",
    OECDResponseFormat.CSV_LABELS: "csvfilewithlabels",
    OECDResponseFormat.XML: "genericdata",
}


# Classe de base abstraite pour les endpoint builders OCDE
class OECDEndpointBuilder(SDMXEndpointBuilder):
    """Base endpoint builder for the OECD SDMX API.

    Common logic (HTTP headers, Accept MIME types, structure query
    parameters) is implemented here; URL paths, query parameters and the
    positional dimension key are version-specific and implemented by the
    :class:`OECDEndpointBuilderV1` and :class:`OECDEndpointBuilderV2`
    subclasses.

    Attributes:
        sdmx_version: OECD SDMX API version associated with this builder.

    Note:
        Do not instantiate this base class directly — use the registry
        :data:`_OECD_ENDPOINT_BUILDERS` or the V1/V2 subclasses.
    """

    # Version d'API associée au builder (surchargée par les sous-classes)
    sdmx_version: SDMXVersion

    # Construction des headers HTTP (Accept basé sur le format et la version)
    def build_headers(
        self,
        accept_encoding: Optional[str] = None,
        accept_language: Optional[str] = None,
        response_format: Optional[OECDResponseFormat] = None,
    ) -> Dict[str, str]:
        """Build HTTP request headers for the OECD API.

        Args:
            accept_encoding: Value for the ``Accept-Encoding`` header.
                Defaults to ``"gzip, deflate"`` when ``None``.
            accept_language: Value for the ``Accept-Language`` header.
            response_format: Desired response format. Controls the
                ``Accept`` header value.

        Returns:
            HTTP headers dictionary.
        """
        fmt = response_format or OECDResponseFormat.CSV_LABELS
        headers: Dict[str, str] = {
            "Accept": self.get_accept_header(fmt, self.sdmx_version),
            "Accept-Encoding": accept_encoding or "gzip, deflate",
        }
        if accept_language is not None:
            headers["Accept-Language"] = accept_language
        return headers

    # Construction des paramètres de requête de structure (identiques v1/v2)
    def build_structure_params(
        self,
        references: Optional[str] = "all",
        detail: Optional[str] = "referencepartial",
        format: Optional[str] = None,
        format_version: Optional[str] = None,
        compress: Optional[str] = None,
    ) -> Dict[str, str]:
        """Build query parameters for an OECD structure request.

        Args:
            references: Related artefacts to embed (default: ``"all"``).
            detail: Level of detail (default: ``"referencepartial"``).
            format: Ignored — OECD uses the ``Accept`` header.
            format_version: Ignored.
            compress: Ignored.

        Returns:
            Query-parameter dictionary.
        """
        params: Dict[str, str] = {}
        if references is not None:
            params["references"] = references
        if detail is not None:
            params["detail"] = detail
        return params

    @staticmethod
    def get_accept_header(
        format: OECDResponseFormat,
        version: SDMXVersion,
    ) -> str:
        """Get appropriate Accept header for a data query format and SDMX version.

        Args:
            format: Desired response format.
            version: SDMX API version.

        Returns:
            ``Accept`` header value string.
        """
        if format == OECDResponseFormat.JSON:
            if version == SDMXVersion.V2:
                return "application/vnd.sdmx.data+json; charset=utf-8; version=2"
            return "application/vnd.sdmx.data+json; charset=utf-8; version=1.0"
        elif format in (OECDResponseFormat.CSV, OECDResponseFormat.CSV_LABELS):
            if version == SDMXVersion.V2:
                return "application/vnd.sdmx.data+csv; charset=utf-8; version=2"
            return "application/vnd.sdmx.data+csv; charset=utf-8"
        elif format == OECDResponseFormat.XML:
            return "application/vnd.sdmx.structurespecificdata+xml; charset=utf-8; version=2.1"
        return "application/json"

    @staticmethod
    def get_structure_accept_header(version: SDMXVersion) -> str:
        """Get the Accept header for an OECD structure JSON query.

        Args:
            version: SDMX API version.

        Returns:
            ``Accept`` header value string for the structure endpoint.
        """
        # Format SDMX-JSON de structure : seule la version 1.0 est proposée par
        # l'OCDE (cf. liste des types acceptés renvoyée dans les réponses 406),
        # indépendamment de la version v1/v2 de l'API REST (qui ne concerne que
        # le chemin d'URL). Demander version=2 provoque une erreur 406.
        return "application/vnd.sdmx.structure+json; charset=utf-8; version=1.0"

    # ── Méthodes spécifiques à la version, à implémenter par les sous-classes ──

    @staticmethod
    def build_dimension_filter(
        dimensions: Dict[int, List[str]],
        num_dimensions: Optional[int] = None,
    ) -> str:
        """Build the positional dimension key string for the data URL.

        Implemented by version-specific subclasses.

        Args:
            dimensions: Dimension position → list of values.
            num_dimensions: Total dimension count (for wildcard padding).

        Returns:
            Dimension filter string (e.g., ``"FRA.M.LI"``).
        """
        raise NotImplementedError


# Builder concret pour l'API OCDE SDMX v1 (legacy REST)
class OECDEndpointBuilderV1(OECDEndpointBuilder):
    """Endpoint builder for the OECD SDMX v1 API (legacy REST format).

    URL patterns:
        data: ``/data/{agency},{dataflow},{version}/{key}``
        structure: ``/dataflow/{agency}/{dataflow}/{version}``

    The positional dimension key supports comma-separated multi-values
    inside a dimension (``"FRA,DEU.M.LI"``).
    """

    sdmx_version = SDMXVersion.V1

    def build_data_endpoint(
        self,
        dataflow: str,
        agency: str,
        version: str,
        key: Optional[str] = None,
    ) -> str:
        """Build the URL path for a data query.

        Args:
            dataflow: Dataflow identifier.
            agency: Agency identifier.
            version: Dataflow version.
            key: Pre-built positional dimension filter, or ``None`` for the
                ``"all"`` wildcard.

        Returns:
            URL path segment.
        """
        dim_filter = key or "all"
        return f"data/{agency},{dataflow},{version}/{dim_filter}"

    def build_data_params(
        self,
        *,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        first_n_observations: Optional[int] = None,
        compress: bool = False,
        dimensions: Optional[Dict[str, List[str]]] = None,
        response_format: Optional[OECDResponseFormat] = None,
        response_format_version: Optional[str] = None,
        lang: Optional[str] = None,
        labels: Optional[str] = None,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        return_data: Optional[str] = None,
        dimension_at_observation: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build query parameters for an OECD v1 data request.

        Recognised parameters: ``start_period``, ``end_period``,
        ``last_n_observations``, ``response_format``,
        ``dimension_at_observation``. Other arguments are accepted for
        interface compatibility and silently ignored.

        Returns:
            Query-parameter dictionary.
        """
        fmt = response_format or OECDResponseFormat.CSV_LABELS
        params: Dict[str, Any] = {
            "dimensionAtObservation": (
                dimension_at_observation
                or DimensionAtObservation.ALL_DIMENSIONS.value
            ),
            "format": _OECD_FORMAT_PARAM_MAP[fmt],
        }
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period
        if last_n_observations:
            params["lastNObservations"] = last_n_observations
        return params

    def build_structure_endpoint(
        self,
        resource_type: Any,
        resource_id: str,
        agency: str,
        version: Optional[str],
    ) -> str:
        """Build the URL path for a structure query.

        Args:
            resource_type: Ignored — OECD v1 structure queries use a fixed
                ``dataflow`` path format.
            resource_id: Dataflow identifier.
            agency: Agency identifier.
            version: Dataflow version (defaults to ``"+"``).

        Returns:
            URL path segment.
        """
        v = version or "+"
        return f"dataflow/{agency}/{resource_id}/{v}"

    @staticmethod
    def build_dimension_filter(
        dimensions: Dict[int, List[str]],
        num_dimensions: Optional[int] = None,
    ) -> str:
        """Build the positional dimension filter string for SDMX v1.

        Format: ``value1,value2.value3.value4`` (comma-separated multi-values
        within a dimension, dot-separated dimensions).

        Args:
            dimensions: Dimension position → list of values.
            num_dimensions: Total dimension count (for wildcard padding).

        Returns:
            Dimension filter string.
        """
        if not dimensions:
            if num_dimensions:
                return ".".join([""] * num_dimensions)
            return "all"
        max_dim = max(dimensions.keys())
        total_dims = num_dimensions if num_dimensions else max_dim + 1
        parts = [",".join(dimensions[i]) if i in dimensions else "" for i in range(total_dims)]
        return ".".join(parts)


# Builder concret pour l'API OCDE SDMX v2 (current REST)
class OECDEndpointBuilderV2(OECDEndpointBuilder):
    """Endpoint builder for the OECD SDMX v2 API (current REST format).

    URL patterns:
        data: ``/v2/data/dataflow/{agency}/{dataflow}/{version}/{key}``
        structure: ``/v2/structure/dataflow/{agency}/{dataflow}/{version}``

    SDMX v2 does not support comma-separated multi-values in the positional
    key. Use the ``split_dimensions`` parameter of
    :meth:`OECDClient.get_data` to iterate on multi-value dimensions.
    """

    sdmx_version = SDMXVersion.V2

    def build_data_endpoint(
        self,
        dataflow: str,
        agency: str,
        version: str,
        key: Optional[str] = None,
    ) -> str:
        """Build the URL path for a data query.

        Args:
            dataflow: Dataflow identifier.
            agency: Agency identifier.
            version: Dataflow version.
            key: Pre-built positional dimension filter, or ``None`` for the
                ``"*"`` wildcard.

        Returns:
            URL path segment.
        """
        dim_filter = key or "*"
        return f"v2/data/dataflow/{agency}/{dataflow}/{version}/{dim_filter}"

    def build_data_params(
        self,
        *,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        first_n_observations: Optional[int] = None,
        compress: bool = False,
        dimensions: Optional[Dict[str, List[str]]] = None,
        response_format: Optional[OECDResponseFormat] = None,
        response_format_version: Optional[str] = None,
        lang: Optional[str] = None,
        labels: Optional[str] = None,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        return_data: Optional[str] = None,
        dimension_at_observation: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build query parameters for an OECD v2 data request.

        v2 encodes the time-period filter via ``c[TIME_PERIOD]=ge:…+le:…``
        rather than ``startPeriod``/``endPeriod``, and additionally supports
        ``attributes`` and ``measures`` query parameters.

        Returns:
            Query-parameter dictionary.
        """
        fmt = response_format or OECDResponseFormat.CSV_LABELS
        params: Dict[str, Any] = {
            "dimensionAtObservation": (
                dimension_at_observation
                or DimensionAtObservation.ALL_DIMENSIONS.value
            ),
            "format": _OECD_FORMAT_PARAM_MAP[fmt],
        }
        # Filtre temporel encodé dans c[TIME_PERIOD]
        if start_period and end_period:
            params["c[TIME_PERIOD]"] = f"ge:{start_period}+le:{end_period}"
        elif start_period:
            params["c[TIME_PERIOD]"] = f"ge:{start_period}"
        elif end_period:
            params["c[TIME_PERIOD]"] = f"le:{end_period}"
        if attributes:
            params["attributes"] = attributes
        if measures:
            params["measures"] = measures
        if last_n_observations:
            params["lastNObservations"] = last_n_observations
        return params

    def build_structure_endpoint(
        self,
        resource_type: Any,
        resource_id: str,
        agency: str,
        version: Optional[str],
    ) -> str:
        """Build the URL path for a structure query.

        Args:
            resource_type: Ignored — OECD v2 structure queries use a fixed
                ``dataflow`` path format.
            resource_id: Dataflow identifier.
            agency: Agency identifier.
            version: Dataflow version (defaults to ``"+"``).

        Returns:
            URL path segment.
        """
        v = version or "+"
        return f"v2/structure/dataflow/{agency}/{resource_id}/{v}"

    @staticmethod
    def build_dimension_filter(
        dimensions: Dict[int, List[str]],
        num_dimensions: Optional[int] = None,
    ) -> str:
        """Build the positional dimension filter string for SDMX v2.

        Each dimension must carry exactly one value or be a wildcard
        (``"*"``).

        Args:
            dimensions: Dimension position → list of values (each list must
                contain exactly one element).
            num_dimensions: Total dimension count (for wildcard padding).

        Returns:
            Dimension filter string.

        Raises:
            ValueError: If any dimension has more than one value.
        """
        if not dimensions:
            if num_dimensions:
                return ".".join(["*"] * num_dimensions)
            return "*"
        for position, values in dimensions.items():
            if len(values) > 1:
                raise ValueError(
                    f"SDMX v2 does not support multiple values for a single dimension. "
                    f"Dimension at position {position} has {len(values)} values: {values}. "
                    f"Use split_dimensions parameter in get_data() to handle multiple values."
                )
        max_dim = max(dimensions.keys())
        total_dims = num_dimensions if num_dimensions else max_dim + 1
        parts = [dimensions[i][0] if i in dimensions else "*" for i in range(total_dims)]
        return ".".join(parts)


# Registre des builders par version d'API (à l'image du registre Eurostat)
_OECD_ENDPOINT_BUILDERS: Dict[SDMXVersion, OECDEndpointBuilder] = {
    SDMXVersion.V1: OECDEndpointBuilderV1(),
    SDMXVersion.V2: OECDEndpointBuilderV2(),
}


# ──────────────────────────────────────────────────────────────────────
# Dataclass de requête publique
# ──────────────────────────────────────────────────────────────────────


# Classe représentant une requête de données
@dataclass
class OECDQueryRequest:
    """Represents an OECD data query request.

    This class encapsulates all parameters needed for a get_data() call,
    providing type safety and easier manipulation of query batches.

    Attributes:
        agency: Agency identifier (e.g., "OECD.SDD.STES")
        dataflow: Dataflow identifier (e.g., "DSD_KEI@DF_KEI")
        version: Dataflow version (default: "+")
        dimensions: Dimension filters
        start_period: Start period
        end_period: End period
        last_n_observations: Number of recent observations
        format: Response format
        dimension_at_observation: How to group observations
        attributes: Attributes to include
        measures: Measures to include
        on_duplicate: Duplicate handling strategy
        split_dimensions: Dimensions to split into separate requests
        max_split_combinations: Max allowed split combinations

    Example:
        >>> query = OECDQueryRequest(
        ...     agency="OECD.SDD.STES",
        ...     dataflow="DSD_KEI@DF_KEI",
        ...     dimensions={"REF_AREA": ["FRA", "DEU"], "FREQ": "M"},
        ... )
        >>> df = client.execute_query(query)
    """
    agency: str
    dataflow: str
    version: str = "+"
    dimensions: Optional[Dict[Union[int, str], Union[str, List[str]]]] = None
    start_period: Optional[str] = None
    end_period: Optional[str] = None
    last_n_observations: Optional[int] = None
    format: OECDResponseFormat = OECDResponseFormat.CSV_LABELS
    dimension_at_observation: DimensionAtObservation = DimensionAtObservation.ALL_DIMENSIONS
    attributes: Optional[str] = None
    measures: Optional[str] = None
    on_duplicate: DuplicateHandling = "warn"
    split_dimensions: Optional[List[Union[int, str]]] = None
    max_split_combinations: int = 100

    # Méthode de conversion des arguments en dictionnaire
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for get_data() kwargs.

        Returns:
            Dictionary of parameters for get_data() method.
        """
        return {
            "agency": self.agency,
            "dataflow": self.dataflow,
            "version": self.version,
            "dimensions": self.dimensions,
            "start_period": self.start_period,
            "end_period": self.end_period,
            "last_n_observations": self.last_n_observations,
            "format": self.format,
            "dimension_at_observation": self.dimension_at_observation,
            "attributes": self.attributes,
            "measures": self.measures,
            "on_duplicate": self.on_duplicate,
            "split_dimensions": self.split_dimensions,
            "max_split_combinations": self.max_split_combinations,
        }

    # Méthode d'extraction de la clé associée au dataflow
    def get_dataflow_key(self) -> str:
        """Get unique key for this dataflow.

        Returns:
            Key in format 'agency::dataflow::version'.
        """
        return f"{self.agency}::{self.dataflow}::{self.version}"


# Initialisation du client pour la requête de données
class OECDClient(AbstractSDMXClient):
    """High-level client for OECD data API.
    
    This client handles data retrieval from OECD's SDMX API and provides
    convenient methods to query economic indicators and convert them to
    pandas DataFrames.
    
    Args:
        base_url: OECD API base URL (default: SDMX public endpoint).
        timeout: Request timeout in seconds.
        sdmx_version: SDMX API version to use (v1 or v2).
        structure_registry: Optional registry for dimension name resolution.
        auto_fetch_structure: If True, fetch structure metadata when needed.
        
    Example:
        >>> client = OECDClient()
        >>> df = client.get_data(
        ...     dataflow="DSD_KEI@DF_KEI",
        ...     agency="OECD.SDD.STES",
        ...     dimensions={"REF_AREA": ["FRA"], "MEASURE": ["PRINTO01"]},
        ... )
    """
    # Initialisation de l'URL par défaut
    DEFAULT_BASE_URL = "https://sdmx.oecd.org/public/rest"
    
    # Initialisation
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = 60,
        sdmx_version: SDMXVersion = SDMXVersion.V2,
        structure_registry: Optional[DataflowStructureRegistry] = None,
        auto_fetch_structure: bool = True,
        rate_limiter: Optional[RateLimiter] = None,
        auto_load_rate_limit: bool = True,
    ):
        # Initialisation de la base (structure_registry, auto_fetch_structure,
        # rate_limiter via _load_rate_limiter)
        super().__init__(
            structure_registry=structure_registry,
            auto_fetch_structure=auto_fetch_structure,
            rate_limiter=rate_limiter,
            auto_load_rate_limit=auto_load_rate_limit,
        )

        # Attributs spécifiques OECD
        self.base_url = base_url
        self.sdmx_version = sdmx_version
        self.api_client = APIClient(base_url=base_url, timeout=timeout)
        # Sélection du builder versionné depuis le registre
        self.endpoint_builder: OECDEndpointBuilder = _OECD_ENDPOINT_BUILDERS[sdmx_version]

    # Méthode auxiliaire de chargement du rate limiter depuis le fichier de configuration
    def _load_rate_limiter(self) -> Optional[RateLimiter]:
        """Load rate limiter from parameters/oecd.json.

        Returns:
            RateLimiter instance or None if configuration not found.
        """
        try:
            # Construction du chemin vers le fichier de paramètres
            params_path = Path(__file__).parents[3] / "parameters" / "oecd.json"

            # Vérification de l'existence du fichier
            if params_path.exists():
                # Chargement du fichier JSON
                with open(params_path, "r", encoding="utf-8") as f:
                    config = json.load(f)

                # Extraction de la configuration du rate limiter
                if "RATE_LIMIT" in config:
                    # Logging
                    logger.info("Loading rate limiter from parameters/oecd.json")
                    return RateLimiter.from_dict(config["RATE_LIMIT"])

            # Logging si pas de configuration trouvée
            logger.debug("No RATE_LIMIT configuration found")
            return None

        except Exception as e:
            # Logging de l'erreur
            logger.warning(f"Could not load rate limiter: {e}")
            return None

    # Méthode de requête des données
    def get_data(
        self,
        agency: str,
        dataflow: str,
        version: str = "+",
        dimensions: Optional[Dict[Union[int, str], Union[str, List[str]]]] = None,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        format: OECDResponseFormat = OECDResponseFormat.CSV_LABELS,
        dimension_at_observation: DimensionAtObservation = DimensionAtObservation.ALL_DIMENSIONS,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        on_duplicate: DuplicateHandling = "warn",
        split_dimensions: Optional[List[Union[int, str]]] = None,
        max_split_combinations: int = 100,
    ) -> pd.DataFrame:
        """Retrieve data from OECD API.

        Args:
            agency: Agency identifier (e.g., "OECD.SDD.STES").
            dataflow: Dataflow identifier (e.g., "DSD_KEI@DF_KEI").
            version: Dataflow version (default: "+" for latest).
            dimensions: Dimension filters, can be:
                - Dict[int, str/List[str]]: dimension position -> values
                - Dict[str, str/List[str]]: dimension name -> values
            start_period: Start period (e.g., "2015", "2015-Q1").
            end_period: End period.
            last_n_observations: Number of recent observations.
            format: Response format (json, csv, csv_labels, xml).
            dimension_at_observation: How to group observations
                (AllDimensions for flat, TIME_PERIOD for series).
            attributes: Attributes to include ("dsd", "all", "none").
            measures: Measures to include ("all", "none").
            on_duplicate: How to handle duplicate rows when wildcards are used:
                - "ignore": Keep all rows without checking.
                - "warn": Log a warning if duplicates are found.
                - "raise": Raise an exception if duplicates are found.
            split_dimensions: List of dimensions (by position or name) for which multiple
                values should be handled in separate API requests. If None (default), all
                dimensions with multiple values will use wildcards (*) in the URL and be
                filtered post-retrieval. Use this parameter to trade off API calls vs
                response size (splitting reduces response size but increases API calls).
                Example: split_dimensions=["REF_AREA"] or split_dimensions=[0]
                NOTE: Internally converted to dimension NAMES for consistent filtering.
            max_split_combinations: Maximum number of requests allowed when splitting.
                Prevents accidental explosion of API calls. Default: 100

        Returns:
            DataFrame with the retrieved data.

        Raises:
            ValueError: If dataflow is not specified or dimension resolution fails.
            DuplicateRowsError: If on_duplicate="raise" and duplicates are found.
            
        Example:
            >>> # By position
            >>> df = client.get_data(
            ...     agency="OECD.SDD.STES",
            ...     dataflow="DSD_KEI@DF_KEI",
            ...     dimensions={
            ...         0: ["FRA", "DEU"],
            ...         2: ["PRINTO01"],
            ...     },
            ... )
            
            >>> # By name (requires structure metadata)
            >>> df = client.get_data(
            ...     agency="OECD.SDD.STES",
            ...     dataflow="DSD_KEI@DF_KEI",
            ...     dimensions={
            ...         "REF_AREA": ["FRA", "DEU"],
            ...         "MEASURE": ["PRINTO01"],
            ...     },
            ... )
        """
        # Vérification de la validité du "dataflow"
        if dataflow is None:
            raise ValueError("dataflow is required")

        # Récupération de la structure du dataflow si nécessaire
        structure = self._ensure_structure(agency=agency, dataflow=dataflow)

        # Normalisation des dimensions au format Dict[int, List[str]]
        normalized_dims = self._normalize_dimensions(
            dimensions=dimensions,
            agency=agency,
            dataflow=dataflow,
            structure=structure,
        )

        # Normalisation de split_dimensions (convertit tout en NOMS)
        split_dimension_names = self._normalize_split_dimensions(
            split_dimensions, structure, normalized_dims
        )

        # Génération des combinaisons (dims_for_url, dims_for_postfilter)
        request_combinations = self._generate_request_combinations(
            normalized_dims, split_dimension_names, structure, max_split_combinations
        )

        # Délégation à AbstractSDMXClient._execute_split_requests qui gère
        # uniformément le cas mono-requête et multi-requêtes (rate limiting,
        # post-filtrage, concaténation).
        logger.info(
            f"Fetching data from {dataflow} ({len(request_combinations)} request(s))"
        )
        df = self._execute_split_requests(
            request_combinations,
            agency=agency,
            dataflow=dataflow,
            version=version,
            structure=structure,
            format=format,
            dimension_at_observation=dimension_at_observation,
            start_period=start_period,
            end_period=end_period,
            last_n_observations=last_n_observations,
            attributes=attributes,
            measures=measures,
        )

        # Vérification des doublons sur le résultat final (déjà post-filtré)
        if on_duplicate != "ignore":
            self._check_duplicates(
                df,
                normalized_dims,
                structure,
                on_duplicate,
            )

        return df

    # Méthode d'exécution d'une requête QueryRequest
    def execute_query(self, query: OECDQueryRequest) -> pd.DataFrame:
        """Execute a OECDQueryRequest.

        Args:
            query: OECDQueryRequest object containing all parameters.

        Returns:
            DataFrame with the retrieved data.

        Example:
            >>> query = OECDQueryRequest(
            ...     agency="OECD.SDD.STES",
            ...     dataflow="DSD_KEI@DF_KEI",
            ...     dimensions={"REF_AREA": ["FRA"], "FREQ": "M"},
            ... )
            >>> df = client.execute_query(query)
        """
        return self.get_data(**query.to_dict())

    # Méthode auxiliaire de normalisation des dimensions du filtre sous la forme d'un dictionnaire position : valeur
    def _normalize_dimensions(
        self,
        dimensions: Optional[Dict[Union[int, str], Union[str, List[str]]]],
        agency: str,
        dataflow: str,
        structure: Optional[DataflowStructure] = None,
    ) -> Dict[int, List[str]]:
        """Normalize dimensions to Dict[int, List[str]] format.
        
        Args:
            dimensions: Input dimensions in various formats.
            agency: Agency identifier for dimension resolution.
            dataflow: Dataflow identifier for dimension resolution.
            structure: Optional structure for dimension resolution.
            
        Returns:
            Normalized dimensions with integer keys and list values.
            
        Raises:
            ValueError: If dimension names cannot be resolved.
        """
        # Cas où les dimensions ne sont pas spécifiées
        if dimensions is None:
            return {}
        
        # Vérification si des noms de dimensions sont utilisés
        has_string_keys = any(isinstance(k, str) for k in dimensions.keys())
        
        # Conversion en position
        if has_string_keys:
            # Utilisation du registre pour résoudre les noms
            return self.structure_registry.resolve_dimensions(
                agency,
                dataflow,
                dimensions,
            )
        
        # Cas où toutes les clés sont des entiers
        normalized = {}
        # Normalisation de la valeur sous forme de liste
        for key, value in dimensions.items():
            # Conversion de la valeur en liste si nécessaire
            if isinstance(value, str):
                normalized[key] = [value]
            else:
                normalized[key] = list(value)
        
        return normalized

    # Méthode auxiliaire de normalisation de split_dimensions en noms de dimensions
    def _normalize_split_dimensions(
        self,
        split_dimensions: Optional[List[Union[int, str]]],
        structure: Optional[DataflowStructure],
        dimensions: Dict[int, List[str]],
    ) -> List[str]:
        """Normalize split_dimensions to dimension NAMES and validate.

        Returns dimension names (not positions) because column positions
        in the output DataFrame vary by format (csvfile vs csvfilewithlabels),
        while dimension names remain consistent.

        Args:
            split_dimensions: List of dimension positions or names to split
            structure: Dataflow structure for name/position resolution
            dimensions: Normalized dimensions dict (position → values)

        Returns:
            List of unique dimension names to split

        Raises:
            ValueError: If dimension name/position cannot be resolved
            ValueError: If split dimension not in dimensions dict
            ValueError: If structure is None (required for name resolution)
        """
        # Cas où aucune dimension à split
        if split_dimensions is None:
            return []

        # Vérification de la structure
        if structure is None:
            raise ValueError(
                "Structure required when using split_dimensions. "
                "The structure could not be loaded for this dataflow."
            )

        # Initialisation de la liste des noms
        normalized_names: List[str] = []

        # Parcours des dimensions à split
        for dim_spec in split_dimensions:
            # Cas où c'est une position (int)
            if isinstance(dim_spec, int):
                # Conversion en nom via structure
                dim_name = structure.get_name(dim_spec)
                # Vérification que la position existe
                if dim_name is None:
                    raise ValueError(
                        f"Position {dim_spec} not found in structure. "
                        f"Valid positions: 0-{structure.num_dimensions-1}"
                    )
                # Vérification que la dimension est dans le filtre
                if dim_spec not in dimensions:
                    raise ValueError(
                        f"Dimension at position {dim_spec} ('{dim_name}') "
                        f"is not in the dimensions filter. "
                        f"Cannot split on dimension that is not filtered."
                    )
                normalized_names.append(dim_name)

            # Cas où c'est un nom (str)
            elif isinstance(dim_spec, str):
                # Vérification que le nom existe
                dim_position = structure.get_position(dim_spec)
                if dim_position is None:
                    # Liste des dimensions disponibles
                    available_names = [dim.name for dim in structure.dimensions]
                    raise ValueError(
                        f"Dimension name '{dim_spec}' not found in structure. "
                        f"Available dimensions: {available_names}"
                    )
                # Vérification que la dimension est dans le filtre
                if dim_position not in dimensions:
                    raise ValueError(
                        f"Dimension '{dim_spec}' (position {dim_position}) "
                        f"is not in the dimensions filter. "
                        f"Cannot split on dimension that is not filtered."
                    )
                normalized_names.append(dim_spec)

            # Type invalide
            else:
                raise ValueError(
                    f"Invalid type for split_dimensions element: {type(dim_spec)}. "
                    f"Expected int or str."
                )

        # Dédupliquer les noms (conversion en set puis list)
        unique_names = list(dict.fromkeys(normalized_names))  # Préserve l'ordre

        # Logging si déduplication
        if len(unique_names) < len(normalized_names):
            logger.debug(f"Deduplicated split_dimensions: {normalized_names} → {unique_names}")

        return unique_names

    # Méthode auxiliaire de génération des combinaisons de requêtes
    def _generate_request_combinations(
        self,
        dimensions: Dict[int, List[str]],
        split_dimension_names: List[str],
        structure: Optional[DataflowStructure],
        max_combinations: int = 100,
    ) -> List[Tuple[Dict[int, List[str]], Dict[str, List[str]]]]:
        """Generate request combinations and post-filter dimensions.

        Args:
            dimensions: Normalized dimensions dict (position → values)
            split_dimension_names: Dimension NAMES to split into separate requests
            structure: Dataflow structure for name/position mapping
            max_combinations: Maximum allowed combinations

        Returns:
            List of (dimensions_for_url, dimensions_for_postfilter) tuples
            - dimensions_for_url: Dict[int, List[str]] for URL construction
            - dimensions_for_postfilter: Dict[str, List[str]] for DataFrame filtering (by NAME)

        Raises:
            ValueError: If cartesian product exceeds max_combinations
        """
        # Conversion des noms en positions (structure garantie non-None si
        # split_dimension_names est non vide grâce à _normalize_split_dimensions)
        split_positions = (
            [structure.get_position(name) for name in split_dimension_names]
            if structure is not None
            else []
        )

        # Identification des dimensions à split (valeurs multiples et dans split_positions)
        split_dims: Dict[int, List[str]] = {}
        for pos, values in dimensions.items():
            if pos in split_positions and len(values) > 1:
                split_dims[pos] = values

        # Identification des dimensions multi-valeurs à ne pas split (pour filtre ex post)
        postfilter_dims: Dict[int, List[str]] = {}
        for pos, values in dimensions.items():
            if pos not in split_positions and len(values) > 1:
                postfilter_dims[pos] = values

        # Cas où aucune dimension à split
        if not split_dims:
            # Créer dims_for_url avec wildcards pour toutes les multi-value dims
            dims_for_url = dimensions.copy()
            for pos in postfilter_dims.keys():
                dims_for_url[pos] = ["*"]

            # Convertir postfilter_dims en noms (skip si pas de structure :
            # le post-filtrage par nom de colonne n'est alors pas possible)
            postfilter_dims_by_name: Dict[str, List[str]] = {}
            if structure is not None:
                for pos, values in postfilter_dims.items():
                    dim_name = structure.get_name(pos)
                    if dim_name:
                        postfilter_dims_by_name[dim_name] = values

            return [(dims_for_url, postfilter_dims_by_name)]

        # Calcul du nombre de combinaisons (produit cartésien)
        num_combinations = reduce(
            operator.mul,
            [len(values) for values in split_dims.values()],
            1
        )

        # Vérification de la limite
        if num_combinations > max_combinations:
            raise ValueError(
                f"Cartesian product would generate {num_combinations} requests, "
                f"exceeding max_split_combinations={max_combinations}. "
                f"Consider splitting fewer dimensions or filtering values."
            )

        # Génération des combinaisons
        combinations: List[Tuple[Dict[int, List[str]], Dict[str, List[str]]]] = []

        # Extraction des positions et valeurs pour le produit cartésien
        positions_to_split = list(split_dims.keys())
        values_lists = [split_dims[pos] for pos in positions_to_split]

        # Produit cartésien
        for combo_values in itertools.product(*values_lists):
            # Construction de dims_for_url
            dims_for_url = dimensions.copy()

            # Remplacement des dimensions split par leur valeur unique
            for pos, value in zip(positions_to_split, combo_values):
                dims_for_url[pos] = [value]

            # Remplacement des dimensions postfilter par wildcard
            for pos in postfilter_dims.keys():
                dims_for_url[pos] = ["*"]

            # Construction de dims_for_postfilter (par NOM)
            postfilter_dims_by_name: Dict[str, List[str]] = {}
            for pos, values in postfilter_dims.items():
                dim_name = structure.get_name(pos)
                if dim_name:
                    postfilter_dims_by_name[dim_name] = values

            # Ajout de la combinaison
            combinations.append((dims_for_url, postfilter_dims_by_name))

        # Logging
        logger.info(
            f"Generated {len(combinations)} request combinations "
            f"for split dimensions: {split_dimension_names}"
        )

        return combinations

    # Méthode auxiliaire de parsing d'une réponse au format json
    def _parse_json_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Parse SDMX-JSON response to DataFrame.
        
        Args:
            data: JSON response from OECD API.
            
        Returns:
            DataFrame with parsed data.
        """
        try:
            # Structure SDMX-JSON
            structure = data.get("structure", data.get("data", {}).get("structure", {}))
            dataSets = data.get("dataSets", data.get("data", {}).get("dataSets", []))
            
            # Cas où les données sont vides
            if not dataSets:
                logger.warning("No datasets found in response")
                return pd.DataFrame()
            
            # Extraction des dimensions et de leurs valeurs
            dimensions = structure.get("dimensions", {}).get("observation", [])
            dim_names = [dim["id"] for dim in dimensions]
            dim_values = {
                dim["id"]: [v["id"] for v in dim["values"]]
                for dim in dimensions
            }
            
            # Extraction des observations
            observations = dataSets[0].get("observations", {})
            
            # Initialisation de la liste des enregistrements
            records = []
            
            # Parcours des observations
            for obs_key, obs_data in observations.items():
                # Parsing de la clé (format: "0:1:2:3")
                indices = list(map(int, obs_key.split(":")))
                
                # Construction de l'enregistrement
                record = {}
                for i, dim_name in enumerate(dim_names):
                    if i < len(indices):
                        dim_index = indices[i]
                        record[dim_name] = dim_values[dim_name][dim_index]
                
                # Ajout de la valeur de l'observation
                if isinstance(obs_data, list) and len(obs_data) > 0:
                    record["value"] = obs_data[0]
                else:
                    record["value"] = obs_data
                
                records.append(record)
            
            # Conversion en DataFrame
            df = pd.DataFrame(records)
            
            # Logging
            logger.info(f"Parsed {len(df)} observations")
            return df
            
        except Exception as e:
            # Logging
            logger.error(f"Failed to parse JSON response: {e}")
            logger.debug(f"Response structure: {json.dumps(data, indent=2)[:1000]}")
            raise
    
    # Méthode d'extraction de la structure des métadonnées associées à un flux
    # /!\ Voir si ne pourrait pas être mis en commun dans le cas de plusieurs sources de données (par exemple en utilisant eurostat)
    def get_structure(
        self,
        agency: str,
        dataflow: str,
        version: str = "+",
    ) -> DataflowStructure:
        """Retrieve dataflow structure metadata.
        
        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            version: Dataflow version (default: "+" for latest).
            
        Returns:
            Structure metadata as dictionary.
            
        Raises:
            ValueError: If dataflow is not specified.
        """
        # Vérification que le flux de données est spécifié
        if dataflow is None:
            raise ValueError("dataflow is required")

        # Construction de l'URL et des paramètres de structure via le builder versionné
        endpoint = self.endpoint_builder.build_structure_endpoint(
            resource_type=None,
            resource_id=dataflow,
            agency=agency,
            version=version,
        )
        params = self.endpoint_builder.build_structure_params()

        # Construction des headers de requête (Accept dynamique selon la version)
        # /!\
        headers = {
            "Accept": self.endpoint_builder.get_structure_accept_header(self.sdmx_version),
        }
        
        # Exécution de la requête
        response = self.api_client.get(endpoint, params=params, headers=headers)
        return self.create_structure_from_api_response(agency=agency, dataflow=dataflow, api_response=response.json())
    
    # Fonction utilitaire pour créer une structure à partir des métadonnées API
    # /!\ Voir si ne pourrait pas être mis en commun dans le cas de plusieurs sources de données (par exemple en utilisant eurostat)
    def create_structure_from_api_response(
        self,
        agency: str,
        dataflow: str,
        api_response: Dict[str, Any],
    ) -> DataflowStructure:
        """Create a DataflowStructure from OECD API structure response.

        Parses the structure metadata returned by the OECD API and creates
        a DataflowStructure object. Supports both SDMX v1 (dimensions with
        inline names) and SDMX v2 (dimensions referencing concept schemes).

        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            api_response: JSON response from structure API endpoint.

        Returns:
            DataflowStructure instance.

        Raises:
            ValueError: If the response cannot be parsed.
        """
        try:
            data = api_response.get("data", api_response)

            # Construction de l'index concept_id → nom lisible
            concept_names: Dict[str, str] = {}
            for scheme in data.get("conceptSchemes", []):
                for concept in scheme.get("concepts", []):
                    concept_id = concept.get("id")
                    name = (
                        concept.get("names", {}).get("en")
                        or concept.get("name")
                    )
                    if concept_id and name:
                        concept_names[concept_id] = name

            structures = data.get("structures", data.get("structure", {}))
            dimensions_data = []

            # Format v1 : structure.dimensions.observation
            if "dimensions" in structures:
                dims = structures["dimensions"]
                if "observation" in dims:
                    dimensions_data = dims["observation"]
                elif isinstance(dims, list):
                    dimensions_data = dims

            # Format v2 : data.dataStructures
            elif "dataStructures" in data:
                ds_list = data["dataStructures"]
                if ds_list:
                    ds = ds_list[0]
                    components = ds.get("dataStructureComponents", {})
                    dim_list = components.get("dimensionList", {})
                    dimensions_data = dim_list.get("dimensions", [])

            dimensions = []
            for i, dim_data in enumerate(dimensions_data):
                dim_id = dim_data.get("id", dim_data.get("name", f"DIM_{i}"))
                position = dim_data.get("position", dim_data.get("keyPosition", i))

                # Résolution du nom : inline (v1) puis concept scheme (v2)
                dim_name = dim_data.get("name") or dim_data.get("names", {}).get("en")
                if not dim_name:
                    # Extraction de l'identifiant de concept depuis l'URN
                    # Ex. "...CS_STES(4.0).REF_AREA" → "REF_AREA"
                    concept_identity = dim_data.get("conceptIdentity", "")
                    concept_id = concept_identity.rsplit(".", 1)[-1] if concept_identity else dim_id
                    dim_name = concept_names.get(concept_id)

                dimensions.append(DimensionInfo(
                    name=dim_id,
                    position=position,
                    description=dim_name if dim_name != dim_id else None,
                ))

            # Tri par position
            dimensions.sort(key=lambda d: d.position)

            return DataflowStructure(
                agency=agency,
                dataflow=dataflow,
                num_dimensions=len(dimensions),
                dimensions=dimensions,
            )

        except Exception as e:
            logger.error(f"Error parsing structure: {e}")
            raise ValueError(f"Unable to parse structure: {e}")
    
    # Méthode de listing de tous les dataflows disponibles
    # /!\ Voir si ne pourrait pas être mis en commun dans le cas de plusieurs sources de données (par exemple en utilisant eurostat)
    def list_all_dataflows(self) -> pd.DataFrame:
        """List all available OECD dataflows.

        Retrieves the complete list of dataflows from OECD SDMX API
        and parses them into a pandas DataFrame. The API always returns
        JSON regardless of the Accept header, using a SDMX v2 structure
        format where dataflows are keyed by URN in a ``references`` dict.

        Returns:
            DataFrame with columns: dataflow, agency, version, name

        Example:
            >>> client = OECDClient()
            >>> df = client.list_all_dataflows()
            >>> df.head()
        """
        # Endpoint pour lister tous les dataflows
        endpoint = "dataflow/all"

        # Headers
        headers = None

        # Application du rate limiter si configuré
        if self.rate_limiter:
            self.rate_limiter.acquire()

        # Logging
        logger.info("Fetching list of all OECD dataflows")

        # Exécution de la requête
        response = self.api_client.get(endpoint, headers=headers)
        
        # Parsing XML
        root = ET.fromstring(response.content)

        # Namespaces SDMX
        namespaces = {
            'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
            'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
        }

        # Extraction des dataflows
        dataflows = []
        for df in root.findall('.//str:Dataflow', namespaces):
            # Extraction des attributs
            dataflow_id = df.get('id')
            agency_id = df.get('agencyID')
            version = df.get('version')

            # Extraction du nom
            name_elem = df.find('.//com:Name', namespaces)
            name = name_elem.text if name_elem is not None else None

            # Ajout à la liste
            dataflows.append({
                'dataflow': dataflow_id,
                'agency': agency_id,
                'version': version,
                'name': name
            })

        # Conversion en DataFrame
        df_result = pd.DataFrame(dataflows)

        # Logging
        logger.info(f"Found {len(df_result)} dataflows")

        return df_result

    # Méthode auxiliaire de parsing de la réponse JSON des dataflows
    def _parse_dataflows_json(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse OECD JSON structure response to extract dataflow list.

        Handles two response shapes:
        - SDMX v2 with ``references`` dict keyed by URN
          (``urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=AGENCY:ID(VERSION)``)
        - SDMX v1/v2 with ``data.dataflows`` list

        Args:
            data: Parsed JSON response from the OECD structure endpoint.

        Returns:
            List of dicts with keys ``dataflow``, ``agency``, ``version``, ``name``.
        """
        dataflows: List[Dict[str, Any]] = []

        # Format SDMX v2 : clés URN dans "references"
        if "references" in data and isinstance(data["references"], dict):
            for urn, obj in data["references"].items():
                # Filtrage des entrées qui ne sont pas des Dataflow
                if "Dataflow=" not in urn:
                    continue

                # Extraction agency, id et version depuis l'URN
                # Format : urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=AGENCY:ID(VERSION)
                try:
                    after_eq = urn.split("Dataflow=", 1)[1]
                    agency, rest = after_eq.split(":", 1)
                    dataflow_id = rest.split("(")[0]
                    version = rest.split("(")[1].rstrip(")") if "(" in rest else None
                except (IndexError, ValueError):
                    logger.debug(f"Could not parse URN: {urn}")
                    continue

                # Extraction du nom depuis l'objet référencé
                name = None
                if isinstance(obj, dict):
                    name = (
                        obj.get("name")
                        or obj.get("names", {}).get("en")
                        or obj.get("label")
                    )

                dataflows.append({
                    "dataflow": dataflow_id,
                    "agency": agency,
                    "version": version,
                    "name": name,
                })
            return dataflows

        # Format SDMX v1/v2 avec data.dataflows
        nested = data.get("data", data)
        for df_obj in nested.get("dataflows", []):
            dataflows.append({
                "dataflow": df_obj.get("id"),
                "agency": df_obj.get("agencyID") or df_obj.get("agency"),
                "version": df_obj.get("version"),
                "name": df_obj.get("name") or df_obj.get("names", {}).get("en"),
            })

        return dataflows

    # Méthode de filtrage des requêtes mises à jour
    def filter_updated_queries(
        self,
        queries: List[OECDQueryRequest],
        updated_since: Optional[Union[str, datetime]]=None,
    ) -> List[OECDQueryRequest]:
        """Filter queries to keep only those with data updated since a given date.

        This method queries the OECD ContentConstraint endpoint for each dataflow
        to determine if the data has been updated since the specified date.

        Args:
            queries: List of OECDQueryRequest objects to filter.
            updated_since: Date/datetime threshold. Only queries for dataflows
                          updated after this date will be returned.
                          Can be a string (ISO format), datetime object, or None.
                          If None, all queries are returned without filtering.

        Returns:
            Filtered list of OECDQueryRequest objects for updated dataflows only.
            If updated_since is None, returns all queries unchanged.

        Example:
            >>> # Get all queries without filtering
            >>> all_queries = client.filter_updated_queries(queries, updated_since=None)

            >>> # Filter by specific date
            >>> queries = [
            ...     OECDQueryRequest(agency="OECD.SDD.STES", dataflow="DSD_KEI@DF_KEI"),
            ...     OECDQueryRequest(agency="OECD.ELS.SPD", dataflow="DSD_SOCX_AGG@DF_SOCX_AGG"),
            ... ]
            >>> updated_queries = client.filter_updated_queries(
            ...     queries,
            ...     updated_since="2024-01-01"
            ... )
            >>> # Execute only updated queries
            >>> for query in updated_queries:
            ...     df = client.execute_query(query)
        """
        # Early return si aucun filtrage demandé
        if updated_since is None:
            # Logging
            logger.info(f"No filtering requested (updated_since=None), returning all {len(queries)} queries")
            return queries

        # Normalisation de la date
        if isinstance(updated_since, str):
            cutoff_date = datetime.fromisoformat(updated_since)
        else:
            cutoff_date = updated_since

        # Déduplication les dataflows (plusieurs queries peuvent avoir le même dataflow)
        unique_dataflows = {}
        for query in queries:
            key = query.get_dataflow_key()
            if key not in unique_dataflows:
                unique_dataflows[key] = query

        # Vérification des mises à jour pour chaque dataflow
        updated_dataflows = set()
        # Logging
        logger.info(f"Checking {len(unique_dataflows)} unique dataflows for updates since {cutoff_date}")

        # Parcours des dataflow
        for key, query in unique_dataflows.items():
            try:
                # Extraction de la dernière mise à jour du jeu de données
                last_updated = self._get_dataflow_last_update(
                    query.agency,
                    query.dataflow,
                    query.version
                )
                # Vérification que la mise à jour est postérieure à la date d'intéret
                if last_updated and last_updated > cutoff_date:
                    # Ajout à la liste des dataflows à requêter
                    updated_dataflows.add(key)
                    # Logging
                    logger.info(f"✓ {key} updated on {last_updated}")
                else:
                    # Logging
                    logger.debug(f"✗ {key} not updated (last: {last_updated})")

            except Exception as e:
                # Loggin
                logger.warning(f"Could not check update status for {key}: {e}")
                # En cas d'erreur, inclure la query par sécurité
                updated_dataflows.add(key)

        # Filtre les queries originales pour ne conserver que celles qui sont concernées par la mise à jour
        filtered_queries = [
            q for q in queries
            if q.get_dataflow_key() in updated_dataflows
        ]

        # Logging
        logger.info(
            f"Filtered {len(queries)} queries → {len(filtered_queries)} "
            f"with updates since {cutoff_date}"
        )

        return filtered_queries

    # Méthode auxiliaire d'extraction de la dernière date de mise à jour d'un dataflow
    def _get_dataflow_last_update(
        self,
        agency: str,
        dataflow: str,
        version: str = "+",
    ) -> Optional[datetime]:
        """Get the last update date for a dataflow via ContentConstraint.

        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            version: Dataflow version.

        Returns:
            Datetime of last update, or None if unavailable.

        Note:
            This method intentionally does **not** invoke the rate limiter:
            metadata queries are lightweight enough that the OECD 60
            requests/hour quota is unlikely to be hit by ordinary update
            checks. If batched across many dataflows in a tight loop, the
            caller should add its own throttling.
        """
        # Construction de l'identifiant ContentConstraint
        # Format: CR_A_{DATASET_ID} où DATASET_ID est extrait du dataflow
        # Ex: "DSD_KEI@DF_KEI" → dataset_id = "DF_KEI"
        if "@" in dataflow:
            dataset_id = dataflow.split("@")[1]
        else:
            dataset_id = dataflow

        constraint_id = f"CR_A_{dataset_id}"

        # Endpoint ContentConstraint
        endpoint = f"contentconstraint/{agency}/{constraint_id}"

        # Headers pour JSON (plus facile à parser)
        headers = {
            "Accept": "application/vnd.sdmx.structure+json;version=1.0.0",
            "Accept-Encoding": "gzip, deflate",
        }

        try:
            # Requête (pas de rate limiting pour metadata)
            response = self.api_client.get(endpoint, headers=headers)
            data = response.json()

            # Parsing la réponse pour extraire la date de mise à jour
            last_update = self._parse_contentconstraint_date(data)
            return last_update

        except Exception as e:
            logger.debug(f"Could not retrieve ContentConstraint for {agency}/{dataflow}: {e}")
            return None

    # Méthode auxiliaire de parsing de la date de mise à jour depuis la réponse ContentConstraint
    def _parse_contentconstraint_date(self, data: Dict[str, Any]) -> Optional[datetime]:
        """Parse ContentConstraint response to extract last update date.

        Args:
            data: JSON response from ContentConstraint endpoint.

        Returns:
            Datetime of last update, or None.
        """
        try:
            # Structure SDMX-JSON ContentConstraint:
            # {
            #   "meta": {
            #     "prepared": "2024-11-15T10:30:00Z",
            #     ...
            #   },
            #   "data": {
            #     "contentConstraints": [{
            #       "validFrom": "2024-01-01",
            #       "validTo": "2024-12-31",
            #       ...
            #     }]
            #   }
            # }

            # Stratégie 1: Chercher "prepared" dans meta
            if "meta" in data and "prepared" in data["meta"]:
                prepared_str = data["meta"]["prepared"]
                return datetime.fromisoformat(prepared_str.replace("Z", "+00:00"))

            # Stratégie 2: Chercher validFrom/validTo dans contentConstraints
            if "data" in data and "contentConstraints" in data["data"]:
                constraints = data["data"]["contentConstraints"]
                if constraints and "validTo" in constraints[0]:
                    valid_to_str = constraints[0]["validTo"]
                    return datetime.fromisoformat(valid_to_str)

            # Logging
            logger.warning("Could not find update date in ContentConstraint response")
            return None

        except Exception as e:
            # Logging
            logger.warning(f"Error parsing ContentConstraint date: {e}")
            return None

    # ──────────────────────────────────────────────────────────────────
    # Méthodes abstraites — Implémentations requises par AbstractSDMXClient
    # ──────────────────────────────────────────────────────────────────

    # Implémentation de l'abstraction : fetch de structure sans cache
    def _fetch_structure(
        self, agency: str, dataflow: str, **kwargs
    ) -> DataflowStructure:
        """Fetch structure from OECD API (no cache).

        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            **kwargs: Accepts ``version`` (ignored — OECD uses ``"+"``).

        Returns:
            Parsed ``DataflowStructure``.
        """
        # Délégation à get_structure qui gère l'appel API et le parsing
        return self.get_structure(agency, dataflow)

    # Implémentation de l'abstraction : exécution d'une seule requête de données
    def _execute_single_request(
        self,
        dims_for_request: Dict[int, List[str]],
        **request_kwargs,
    ) -> pd.DataFrame:
        """Execute a single OECD data request.

        Builds the data endpoint via :attr:`endpoint_builder`, executes the
        request, and parses the response.

        Args:
            dims_for_request: Dimension position → values for URL construction
                (``Dict[int, List[str]]``).
            **request_kwargs: Keyword arguments forwarded from
                ``_execute_split_requests`` or ``get_data``: ``agency``,
                ``dataflow``, ``version``, ``structure``, ``format``,
                ``dimension_at_observation``, ``start_period``,
                ``end_period``, ``last_n_observations``, ``attributes``,
                ``measures``.

        Returns:
            Parsed DataFrame for this single request.
        """
        agency: str = request_kwargs["agency"]
        dataflow: str = request_kwargs["dataflow"]
        version: str = request_kwargs.get("version", "+")
        structure: Optional[DataflowStructure] = request_kwargs.get("structure")
        fmt: OECDResponseFormat = request_kwargs.get("format", OECDResponseFormat.CSV_LABELS)
        dimension_at_observation: DimensionAtObservation = request_kwargs.get(
            "dimension_at_observation", DimensionAtObservation.ALL_DIMENSIONS
        )
        num_dimensions = structure.num_dimensions if structure else None

        # Construction du filtre de dimensions positionnel via le builder versionné
        dim_filter = self.endpoint_builder.build_dimension_filter(
            dims_for_request, num_dimensions
        )

        # Construction de l'endpoint, des paramètres et des headers
        endpoint = self.endpoint_builder.build_data_endpoint(
            dataflow=dataflow,
            agency=agency,
            version=version,
            key=dim_filter,
        )
        params = self.endpoint_builder.build_data_params(
            start_period=request_kwargs.get("start_period"),
            end_period=request_kwargs.get("end_period"),
            last_n_observations=request_kwargs.get("last_n_observations"),
            response_format=fmt,
            attributes=request_kwargs.get("attributes"),
            measures=request_kwargs.get("measures"),
            dimension_at_observation=(
                dimension_at_observation.value
                if isinstance(dimension_at_observation, DimensionAtObservation)
                else dimension_at_observation
            ),
        )
        headers = self.endpoint_builder.build_headers(response_format=fmt)

        # Exécution de la requête
        logger.debug(f"Single request: {endpoint}")
        response = self.api_client.get(endpoint, params=params, headers=headers)

        # Parsing de la réponse
        if fmt == OECDResponseFormat.JSON:
            return self._parse_json_response(response.json())
        elif fmt in (OECDResponseFormat.CSV, OECDResponseFormat.CSV_LABELS):
            return self._parse_csv_response(response.text)
        else:
            raise NotImplementedError(f"Format {fmt} not yet implemented")

    # ──────────────────────────────────────────────────────────────────

    # Méthode de fermeture de la session
    def close(self) -> None:
        """Close the client and release resources."""
        self.api_client.close()