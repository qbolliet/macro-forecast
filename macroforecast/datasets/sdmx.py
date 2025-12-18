"""SDMX URL builder for OECD API.

This module constructs SDMX-compliant URLs according to OECD API specification.
Supports both SDMX API v1 and v2 formats.
"""
# Importation des modules
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple, Union
from enum import Enum


# Classe spécifiant les types de versions de l'API SDMX
class SDMXVersion(str, Enum):
    """SDMX API version."""
    V1 = "v1"
    V2 = "v2"


# Classe spécifiant les types des formats pouvant être retournés
class ResponseFormat(str, Enum):
    """Response format options.
    
    Attributes:
        JSON: JSON format (jsondata parameter)
        CSV: CSV format without labels (csvfile parameter)
        CSV_LABELS: CSV format with labels (csvfilewithlabels parameter)
        XML: XML generic data format (genericdata parameter)
    """
    JSON = "json"
    CSV = "csv"
    CSV_LABELS = "csv_labels"
    XML = "xml"


# Dictionnaire de mapping des formats vers les paramètres API
FORMAT_PARAM_MAP = {
    ResponseFormat.JSON: "jsondata",
    ResponseFormat.CSV: "csvfile",
    ResponseFormat.CSV_LABELS: "csvfilewithlabels",
    ResponseFormat.XML: "genericdata",
}


# Classe spécifiant les valeurs possibles pour dimensionAtObservation
class DimensionAtObservation(str, Enum):
    """Dimension at observation level options.
    
    Attributes:
        ALL_DIMENSIONS: Flat representation of observations
        TIME_PERIOD: Time series view with observations grouped by time
    """
    ALL_DIMENSIONS = "AllDimensions"
    TIME_PERIOD = "TIME_PERIOD"


# Classe spécifiant les paramètres d'une requête SDMX
@dataclass
class SDMXDataQuery:
    """SDMX data query parameters.
    
    Args:
        agency: Agency identifier (e.g., 'OECD.ENV.EPI')
        dataflow: Dataflow identifier (e.g., 'DSD_ECH@EXT_DROUGHT')
        version: Dataflow version (default: '1.0', use '+' for latest)
        dimensions: Dictionary mapping dimension positions to values
        start_period: Start time period (inclusive)
        end_period: End time period (inclusive)
        last_n_observations: Number of recent observations to retrieve
        format: Response format (json, csv, csv_labels, xml)
        sdmx_version: SDMX API version to use
        dimension_at_observation: Dimension to present at observation level
        num_dimensions: Total number of dimensions in the dataflow (for padding)
        attributes: Attributes to include (dsd, all, none)
        measures: Measures to include (all, none)
    
    Example:
        >>> query = SDMXDataQuery(
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
    dimensions: Dict[int, List[str]] = field(default_factory=dict)
    start_period: Optional[str] = None
    end_period: Optional[str] = None
    last_n_observations: Optional[int] = None
    format: ResponseFormat = ResponseFormat.JSON
    sdmx_version: SDMXVersion = SDMXVersion.V1
    dimension_at_observation: DimensionAtObservation = DimensionAtObservation.ALL_DIMENSIONS
    num_dimensions: Optional[int] = None
    attributes: Optional[str] = None  # "dsd", "all", "none" ou None
    measures: Optional[str] = None    # "all", "none" ou None
    
    # Méthode convertissant les attributs en dictionnaires
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


# Classe de construction des URL pour les formats de données SDMX
class SDMXURLBuilder:
    """Builder for SDMX-compliant URLs.
    
    This class constructs URLs according to OECD SDMX API specifications,
    supporting both v1 and v2 API versions.
    
    Example:
        >>> builder = SDMXURLBuilder()
        >>> query = SDMXDataQuery(
        ...     agency="OECD",
        ...     dataflow="KEI",
        ...     dimensions={0: ["FRA"], 1: ["PRINTO01"]},
        ...     num_dimensions=7,
        ... )
        >>> url, params = builder.build_data_url(query)
    """

    # Méthode de construction de l'URL
    @staticmethod
    def build_data_url(query: SDMXDataQuery) -> Tuple[str, Dict[str, Any]]:
        """Build data query URL and parameters.
        
        Args:
            query: SDMX data query parameters.
            
        Returns:
            Tuple of (endpoint_path, query_parameters).
        """
        # Distinction suivant la version de l'API SDMX
        if query.sdmx_version == SDMXVersion.V1:
            # Construction en utilisant la V1 de l'API
            return SDMXURLBuilder._build_v1_data_url(query)
        else:
            # Construction en utilisant la V2 de l'API
            return SDMXURLBuilder._build_v2_data_url(query)
    
    # Méthode auxiliaire de construction de l'URL de requête pour la première version
    @staticmethod
    def _build_v1_data_url(query: SDMXDataQuery) -> Tuple[str, Dict[str, Any]]:
        """Build SDMX v1 data URL.
        
        Format: /rest/data/<agency>,<dataflow>,<version>/<filter>
        
        Args:
            query: SDMX data query parameters.
            
        Returns:
            Tuple of (endpoint_path, query_parameters).
        """
        # Construction du filtre de dimensions
        dim_filter = SDMXURLBuilder._build_dimension_filter_v1(
            query.dimensions,
            query.num_dimensions,
        )
        
        # Construction du path
        path_parts = [
            "data",
            f"{query.agency},{query.dataflow},{query.version}",
            dim_filter,
        ]
        endpoint = "/".join(path_parts)
        
        # Construction des paramètres
        params = {}
        
        # Ajout du paramètre de début de période de requête
        if query.start_period:
            params["startPeriod"] = query.start_period
        
        # Ajout du paramètre de fin de période de requête
        if query.end_period:
            params["endPeriod"] = query.end_period
        
        # Ajout du paramètre des dernières observations
        if query.last_n_observations:
            params["lastNObservations"] = query.last_n_observations
        
        # Ajout du paramètre dimensionAtObservation
        params["dimensionAtObservation"] = query.dimension_at_observation.value
        
        # Ajout du paramètre de format
        params["format"] = FORMAT_PARAM_MAP[query.format]
        
        # Retourne l'URL et les paramètres de requête
        return endpoint, params
    
    # Méthode auxiliaire de construction de l'URL de requête pour la deuxième version
    @staticmethod
    def _build_v2_data_url(query: SDMXDataQuery) -> Tuple[str, Dict[str, Any]]:
        """Build SDMX v2 data URL.
        
        Format: /rest/v2/data/dataflow/<agency>/<dataflow>/<version>/<filter>
        
        Args:
            query: SDMX data query parameters.
            
        Returns:
            Tuple of (endpoint_path, query_parameters).
        """
        # Construction du filtre de dimensions
        dim_filter = SDMXURLBuilder._build_dimension_filter_v2(
            query.dimensions,
            query.num_dimensions,
        )
        
        # Construction du path
        path_parts = [
            "v2/data/dataflow",
            query.agency,
            query.dataflow,
            query.version,
            dim_filter,
        ]
        endpoint = "/".join(path_parts)
        
        # Construction des paramètres
        params = {}
        
        # Gestion du filtre temporel en v2
        if query.start_period and query.end_period:
            params["c[TIME_PERIOD]"] = f"ge:{query.start_period}+le:{query.end_period}"
        elif query.start_period:
            params["c[TIME_PERIOD]"] = f"ge:{query.start_period}"
        elif query.end_period:
            params["c[TIME_PERIOD]"] = f"le:{query.end_period}"
        
        # Ajout du paramètre des dernières observations
        if query.last_n_observations:
            params["lastNObservations"] = query.last_n_observations
        
        # Ajout du paramètre dimensionAtObservation
        params["dimensionAtObservation"] = query.dimension_at_observation.value
        
        # Ajout du paramètre de format
        params["format"] = FORMAT_PARAM_MAP[query.format]
        
        # Ajout du paramètre optionnel d'attributs
        if query.attributes:
            params["attributes"] = query.attributes
        
        # Ajout du paramètre optionnel de mesure
        if query.measures:
            params["measures"] = query.measures
        
        return endpoint, params
    
    # Méthode auxiliaire de construction du filtre de dimensions en V1
    @staticmethod
    def _build_dimension_filter_v1(
        dimensions: Dict[int, List[str]],
        num_dimensions: Optional[int] = None,
    ) -> str:
        """Build dimension filter for SDMX v1.
        
        Format: value1+value2.value3+value4
        Multiple values for same dimension: separated by '+'
        Different dimensions: separated by '.'
        All values: empty string between dots
        
        Args:
            dimensions: Dictionary mapping dimension position to list of values.
            num_dimensions: Total number of dimensions (for padding with empty strings).
            
        Returns:
            Formatted dimension filter string.
        """
        # Cas où aucune dimension n'est spécifiée
        if not dimensions:
            if num_dimensions:
                # Retourne le bon nombre de positions vides
                return ".".join([""] * num_dimensions)
            return "all"
        
        # Détermination du nombre de positions à générer
        max_dim = max(dimensions.keys())
        total_dims = num_dimensions if num_dimensions else max_dim + 1
        
        # Construction du filtre
        filter_parts = []
        for i in range(total_dims):
            if i in dimensions:
                # Jonction des valeurs de cette dimension avec '+'
                filter_parts.append("+".join(dimensions[i]))
            else:
                # Dimension non spécifiée = toutes les valeurs (chaîne vide en v1)
                filter_parts.append("")
        
        return ".".join(filter_parts)
    
    # Méthode auxiliaire de construction du filtre de dimensions en V2
    @staticmethod
    def _build_dimension_filter_v2(
        dimensions: Dict[int, List[str]],
        num_dimensions: Optional[int] = None,
    ) -> str:
        """Build dimension filter for SDMX v2.
        
        Format: value1.value2.value3
        Multiple values for same dimension: separated by '+' (or multiple requests)
        All values: '*'
        
        Args:
            dimensions: Dictionary mapping dimension position to list of values.
            num_dimensions: Total number of dimensions (for padding with '*').
            
        Returns:
            Formatted dimension filter string.
        """
        # Cas où aucune dimension n'est spécifiée
        if not dimensions:
            if num_dimensions:
                # Retourne le bon nombre de wildcards
                return ".".join(["*"] * num_dimensions)
            return "*"
        
        # Détermination du nombre de positions à générer
        max_dim = max(dimensions.keys())
        total_dims = num_dimensions if num_dimensions else max_dim + 1
        
        # Construction du filtre
        filter_parts = []
        for i in range(total_dims):
            if i in dimensions:
                # En v2, plusieurs valeurs sont séparées par '+'
                filter_parts.append("+".join(dimensions[i]))
            else:
                # Toutes les valeurs = '*'
                filter_parts.append("*")
        
        return ".".join(filter_parts)
    
    # Méthode de construction de la structure de l'URL de requête pour chaque version
    @staticmethod
    def build_structure_url(
        agency: str,
        dataflow: str,
        version: str = "1.0",
        sdmx_version: SDMXVersion = SDMXVersion.V1,
    ) -> Tuple[str, Dict[str, str]]:
        """Build structure query URL.
        
        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            version: Dataflow version.
            sdmx_version: SDMX API version.
            
        Returns:
            Tuple of (endpoint_path, query_parameters).
        """
        # Distinction suivant la version
        if sdmx_version == SDMXVersion.V1:
            endpoint = f"dataflow/{agency}/{dataflow}/{version}"
        else:
            endpoint = f"v2/structure/dataflow/{agency}/{dataflow}/{version}"
        
        # Construction des paramètres
        params = {
            "references": "all",
            "detail": "referencepartial",
        }
        
        return endpoint, params
    
    # Méthode de construction du header
    @staticmethod
    def get_accept_header(format: ResponseFormat, version: SDMXVersion) -> str:
        """Get appropriate Accept header for format and version.
        
        Args:
            format: Desired response format.
            version: SDMX API version.
            
        Returns:
            Accept header value.
        """
        # Distinction suivant le format de la réponse attendu
        if format == ResponseFormat.JSON:
            if version == SDMXVersion.V2:
                return "application/vnd.sdmx.data+json; charset=utf-8; version=2"
            return "application/vnd.sdmx.data+json; charset=utf-8; version=1.0"
        
        elif format in (ResponseFormat.CSV, ResponseFormat.CSV_LABELS):
            if version == SDMXVersion.V2:
                return "application/vnd.sdmx.data+csv; charset=utf-8; version=2"
            return "application/vnd.sdmx.data+csv; charset=utf-8"
        
        elif format == ResponseFormat.XML:
            return "application/vnd.sdmx.structurespecificdata+xml; charset=utf-8; version=2.1"
        
        return "application/json"