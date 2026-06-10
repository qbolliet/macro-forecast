"""Eurostat query request dataclasses.

Version-specific DTOs encapsulating the parameters of an
:meth:`EurostatClient.get_data` call. :class:`EurostatQueryRequest` holds the
parameters shared by both API versions; :class:`EurostatQueryRequestV30` and
:class:`EurostatQueryRequestV21` add the version-specific fields.
"""
# Importation des modules
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from ...core.sdmx import DuplicateHandling
from .formats import DataDetail, EurostatResponseFormat


# Classe de base contenant les paramètres communs aux deux versions d'API
@dataclass
class EurostatQueryRequest:
    """Base class for Eurostat query requests.

    Contains all parameters shared by both SDMX 3.0 and 2.1 API versions.
    Use :class:`EurostatQueryRequestV30` or :class:`EurostatQueryRequestV21`
    directly to benefit from version-specific parameter typing.

    Attributes:
        dataflow: Dataflow identifier (e.g., ``"namq_10_gdp"``,
            ``"DS-045409"``).
        version: Dataflow version (default: ``"*"`` for latest).
        dimensions: Dimension filters as ``{name: value_or_list}``.
        start_period: Start period filter (ISO / SDMX format).
        end_period: End period filter.
        last_n_observations: Number of most-recent observations to return.
        first_n_observations: Number of first observations to return.
        format: Response format (default: CSV).
        compress: Whether to request gzip compression of the response.
        on_duplicate: Duplicate handling strategy.
        split_dimensions: Dimensions to split into separate sub-requests.
        max_split_combinations: Maximum allowed split combinations.
    """
    # Attributs communs aux deux versions
    dataflow: str
    version: str = "*"
    dimensions: Optional[Dict[str, Union[str, List[str]]]] = None
    start_period: Optional[str] = None
    end_period: Optional[str] = None
    last_n_observations: Optional[int] = None
    first_n_observations: Optional[int] = None
    format: EurostatResponseFormat = EurostatResponseFormat.CSV
    compress: bool = False
    on_duplicate: DuplicateHandling = "warn"
    split_dimensions: Optional[List[str]] = None
    max_split_combinations: int = 100

    # Méthode de conversion des paramètres communs en dictionnaire de kwargs
    def _base_dict(self) -> Dict[str, Any]:
        """Return the common parameters as a dictionary.

        Returns:
            Dictionary of parameters shared by both API versions.
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
            "on_duplicate": self.on_duplicate,
            "split_dimensions": self.split_dimensions,
            "max_split_combinations": self.max_split_combinations,
        }

    # Méthode d'extraction de la clé unique associée au dataflow
    def get_dataflow_key(self) -> str:
        """Get unique key for this dataflow.

        Returns:
            Key in format ``'dataflow::version'``.
        """
        return f"{self.dataflow}::{self.version}"


# Classe représentant une requête de données Eurostat via l'API SDMX 3.0
@dataclass
class EurostatQueryRequestV30(EurostatQueryRequest):
    """Query request for the Eurostat SDMX 3.0 API.

    Extends :class:`EurostatQueryRequest` with parameters specific to
    the SDMX 3.0 endpoint. Use this class when the client is configured with
    ``api_version=SDMXVersion.V3`` (the default).

    Attributes:
        attributes: Attribute selection string (e.g., ``"dataStructure"``).
        measures: Measure selection string (e.g., ``"OBS_VALUE"``).
        lang: Language code for label localisation (e.g., ``"en"``,
            ``"fr"``).
        labels: Label display mode (e.g., ``"name"``, ``"id"``).
        response_format_version: Format version string (e.g., ``"1.0"``).

    Example:
        >>> query = EurostatQueryRequestV30(
        ...     dataflow="namq_10_gdp",
        ...     dimensions={"GEO": ["FR", "DE"], "FREQ": "Q"},
        ...     lang="fr",
        ... )
        >>> df = client.execute_query(query)
    """
    # Attributs spécifiques SDMX 3.0
    attributes: Optional[str] = None
    measures: Optional[str] = None
    lang: Optional[str] = None
    labels: Optional[str] = None
    response_format_version: Optional[str] = None

    # Méthode de conversion des paramètres en dictionnaire de kwargs
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for ``get_data()`` kwargs.

        Returns:
            Dictionary of all parameters. SDMX 2.1-specific fields are
            included as ``None`` for compatibility with ``get_data()``.
        """
        return {
            **self._base_dict(),
            "attributes": self.attributes,
            "measures": self.measures,
            "lang": self.lang,
            "labels": self.labels,
            "response_format_version": self.response_format_version,
        }


# Classe représentant une requête de données Eurostat via l'API SDMX 2.1
@dataclass
class EurostatQueryRequestV21(EurostatQueryRequest):
    """Query request for the Eurostat SDMX 2.1 API (legacy).

    Extends :class:`EurostatQueryRequest` with parameters specific to
    the SDMX 2.1 endpoint. Use this class when the client is configured with
    ``api_version=SDMXVersion.V2_1``.

    Attributes:
        dimension_at_observation: Dimension serialised at observation
            level (e.g., ``"AllDimensions"`` for flat output,
            ``"TIME_PERIOD"`` for time series).
        detail: Data detail level (e.g., ``"dataonly"``,
            ``"serieskeysonly"``).

    Example:
        >>> query = EurostatQueryRequestV21(
        ...     dataflow="namq_10_gdp",
        ...     dimensions={"GEO": ["FR", "DE"], "FREQ": "Q"},
        ...     detail="dataonly",
        ... )
        >>> df = client_v21.execute_query(query)
    """
    # Attributs spécifiques SDMX 2.1
    dimension_at_observation: Optional[str] = None
    detail: Optional[DataDetail] = None

    # Méthode de conversion des paramètres en dictionnaire de kwargs
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for ``get_data()`` kwargs.

        Returns:
            Dictionary of all parameters. SDMX 3.0-specific fields are
            included as ``None`` for compatibility with ``get_data()``.
        """
        return {
            **self._base_dict(),
            "dimension_at_observation": self.dimension_at_observation,
            "detail": self.detail,
        }
