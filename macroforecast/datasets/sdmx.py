"""SDMX URL builder for OECD API.

This module constructs SDMX-compliant URLs according to OECD API specification.
Supports both SDMX API v1 and v2 formats.
"""
# Importation des modules
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum


# Classe spécifiant les types de s versions de l'API SDMX
class SDMXVersion(str, Enum):
    """SDMX API version."""
    V1 = "v1"
    V2 = "v2"


# Classe spécifiant les types des formats pouvant être retournés
class ResponseFormat(str, Enum):
    """Response format options."""
    JSON = "json"
    CSV = "csv"
    XML = "xml"


# Classe spécifiant les paramètres d'une requête SDMX
@dataclass
class SDMXDataQuery:
    """SDMX data query parameters.
    
    Attributes:
        agency: Agency identifier (e.g., 'OECD.ENV.EPI')
        dataflow: Dataflow identifier (e.g., 'DSD_ECH@EXT_DROUGHT')
        version: Dataflow version (default: '1.0', use '+' for latest)
        dimensions: Dictionary mapping dimension positions to values
        start_period: Start time period (inclusive)
        end_period: End time period (inclusive)
        last_n_observations: Number of recent observations to retrieve
        format: Response format (json, csv, xml)
        sdmx_version: SDMX API version to use
    
    Example:
        >>> query = SDMXDataQuery(
        ...     agency="OECD",
        ...     dataflow="KEI",
        ...     dimensions={0: ["FRA", "DEU"], 1: ["PRINTO01"]},
        ... )
    """
    # Initialisation des attributs
    agency: str
    dataflow: str
    version: str = "1.0"
    dimensions: Dict[int, List[str]] = field(default_factory=dict)
    start_period: Optional[str] = None
    end_period: Optional[str] = None
    last_n_observations: Optional[int] = None
    format: ResponseFormat = ResponseFormat.JSON
    sdmx_version: SDMXVersion = SDMXVersion.V1
    dimension_at_observation: str = "AllDimensions"
    
    # Méthode convertissant les attributs en dictionnaires
    def to_dict(self) -> Dict[str, Any]:
        """Convert query to dictionary representation."""
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
        }


# Classe de construction des URL pour les formats de données SDMX
# /!\ Cette classe est une Mixin avec uniquement des static methods, voir s'il ne vaut pas mieux en faire un ensemble de fonctions
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
        ... )
        >>> url, params = builder.build_data_url(query)
    """
    
    # Méthode de construction de l'URL
    @staticmethod
    def build_data_url(query: SDMXDataQuery) -> Tuple[str, Dict[str, Any]]:
        """Build data query URL and parameters.
        
        Args:
            query: SDMX data query parameters
            
        Returns:
            Tuple of (endpoint_path, query_parameters)
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
        """
        # Construction du path
        path_parts = [
            "data",
            f"{query.agency},{query.dataflow},{query.version}",
            SDMXURLBuilder._build_dimension_filter_v1(query.dimensions),
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
        
        # Ajout du parmaètre des dernières observations
        if query.last_n_observations:
            params["lastNObservations"] = query.last_n_observations
        
        # /!\ A ajouter en argument de la requête ?
        # Ajout du paramètre de dimension de l'observation
        params["dimensionAtObservation"] = query.dimension_at_observation
        
        # Format de réponse
        if query.format == ResponseFormat.JSON:
            params["format"] = "jsondata"
        elif query.format == ResponseFormat.CSV:
            params["format"] = "csv"
        
        # Retourne l'URL et les paramètres de requête
        return endpoint, params
    
    # Méthode auxiliaire de construction de l'URL de requête pour la deuxième version
    @staticmethod
    def _build_v2_data_url(query: SDMXDataQuery) -> Tuple[str, Dict[str, Any]]:
        """Build SDMX v2 data URL.
        
        Format: /rest/v2/data/dataflow/<agency>/<dataflow>/<version>/<filter>
        """
        # Construction du path
        path_parts = [
            "v2/data/dataflow",
            query.agency,
            query.dataflow,
            query.version,
            SDMXURLBuilder._build_dimension_filter_v2(query.dimensions),
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
        
        # /!\ A ajouter en argument de la requête ?
        # Ajout des attributs
        params["attributes"] = "dsd"
        # Ajout des mesures
        params["measures"] = "all"
        
        # Retourne l'URL et les paramètres de requête
        return endpoint, params
    
    # Méthode auxiliaire de construction du filtre de dimensions en V1
    @staticmethod
    def _build_dimension_filter_v1(dimensions: Dict[int, List[str]]) -> str:
        """Build dimension filter for SDMX v1.
        
        Format: value1+value2..value3+value4
        Multiple values for same dimension: separated by '+'
        Different dimensions: separated by '.'
        All values: empty string between dots
        
        Args:
            dimensions: Dictionary mapping dimension position to list of values
            
        Returns:
            Formatted dimension filter string
        """
        # Valeur par défaut si aucune dimension n'est spécifiée
        if not dimensions:
            return "all"
        
        # Recherche de la dimension maximale pour savoir combien de dimensions on a
        max_dim = max(dimensions.keys()) if dimensions else 0
        
        # Construction du filtre
        # Initialisation de la liste des parties du filtre
        filter_parts = []
        # Parcours des dimensions du filtre
        for i in range(max_dim + 1):
            # Vérification si est dans les dimensions
            if i in dimensions:
                # Jonction des valeurs de cette dimension avec '+'
                filter_parts.append("+".join(dimensions[i]))
            else:
                # Dimension non spécifiée = toutes les valeurs
                filter_parts.append("")
        
        return ".".join(filter_parts)
    
    # Méthode auxiliaire de construction du filtre de dimensions en V2
    @staticmethod
    def _build_dimension_filter_v2(dimensions: Dict[int, List[str]]) -> str:
        """Build dimension filter for SDMX v2.
        
        Format: value1.value2.value3
        Only one value per dimension, use '*' for all values
        
        Args:
            dimensions: Dictionary mapping dimension position to list of values
            
        Returns:
            Formatted dimension filter string
        """
        # Valeur par défaut
        if not dimensions:
            return "*"
        
        # Recherche de la dimension maximale
        max_dim = max(dimensions.keys()) if dimensions else 0
        
        # Construction du filtre
        # Initialisation de la liste des parties du filtre
        filter_parts = []
        # Parcours des dimensions
        for i in range(max_dim + 1):
            # Vérification qu'ets bien dans les dimensions
            if i in dimensions:
                # En v2, on prend seulement la première valeur
                # Si plusieurs valeurs, il faudrait faire plusieurs requêtes
                filter_parts.append(dimensions[i][0])
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
            agency: Agency identifier
            dataflow: Dataflow identifier
            version: Dataflow version
            sdmx_version: SDMX API version
            
        Returns:
            Tuple of (endpoint_path, query_parameters)
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
            format: Desired response format
            version: SDMX API version
            
        Returns:
            Accept header value
        """
        # Distinction suivant le format de la réponse attendu
        if format == ResponseFormat.JSON:
            if version == SDMXVersion.V2:
                return "application/vnd.sdmx.data+json; charset=utf-8; version=2"
            return "application/vnd.sdmx.data+json; charset=utf-8; version=1.0"
        
        elif format == ResponseFormat.CSV:
            if version == SDMXVersion.V2:
                return "application/vnd.sdmx.data+csv; charset=utf-8; version=2"
            return "application/vnd.sdmx.data+csv; charset=utf-8"
        
        elif format == ResponseFormat.XML:
            return "application/vnd.sdmx.structurespecificdata+xml; charset=utf-8; version=2.1"
        
        return "application/json"