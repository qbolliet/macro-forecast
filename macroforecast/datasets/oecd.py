"""OECD data client.

This module provides a high-level client for querying OECD data through
their SDMX API and converting responses to pandas DataFrames.
"""
# Importation des modules
# Modules de base
from io import StringIO
import logging
from typing import Optional, Dict, List, Any, Union
import json
import pandas as pd

# Utilitaires internes au package pour la requête de données au format SDMX
from .client import APIClient
from .sdmx import (
    SDMXURLBuilder,
    SDMXDataQuery,
    SDMXVersion,
    ResponseFormat,
)

# Initialisation du logger
logger = logging.getLogger(__name__)


# Initialisation du client pour la requête de données
class OECDClient:
    """High-level client for OECD data API.
    
    This client handles data retrieval from OECD's SDMX API and provides
    convenient methods to query economic indicators and convert them to
    pandas DataFrames.
    
    Args:
        base_url: OECD API base URL (default: SDMX public endpoint)
        timeout: Request timeout in seconds
        sdmx_version: SDMX API version to use (v1 or v2)
        
    Example:
        >>> client = OECDClient()
        >>> df = client.get_data(
        ...     dataflow="KEI",
        ...     dimensions={"LOCATION": ["FRA"], "INDICATOR": ["PRINTO01"]},
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
    ):
        # Initialisation des attributs
        self.base_url = base_url
        self.sdmx_version = sdmx_version
        self.api_client = APIClient(base_url=base_url, timeout=timeout)
        self.url_builder = SDMXURLBuilder()
    
    # Méthode de requête des données
    def get_data(
        self,
        agency: str = "OECD",
        dataflow: str = None,
        version: str = "1.0",
        dimensions: Optional[Dict[Union[int, str], Union[str, List[str]]]] = None,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        format: ResponseFormat = ResponseFormat.JSON,
    ) -> pd.DataFrame:
        """Retrieve data from OECD API.
        
        Args:
            agency: Agency identifier (default: "OECD")
            dataflow: Dataflow identifier (e.g., "KEI", "MEI")
            version: Dataflow version (default: "1.0", use "+" for latest)
            dimensions: Dimension filters, can be:
                - Dict[int, List[str]]: dimension position -> values
                - Dict[str, str/List[str]]: dimension name -> values
            start_period: Start period (e.g., "2015", "2015-Q1")
            end_period: End period
            last_n_observations: Number of recent observations
            format: Response format (json, csv, xml)
            
        Returns:
            DataFrame with the retrieved data
            
        Example:
            >>> # By position (0=countries, 1=indicators, 2=frequency)
            >>> df = client.get_data(
            ...     dataflow="KEI",
            ...     dimensions={
            ...         0: ["FRA", "DEU"],
            ...         1: ["PRINTO01"],
            ...         2: ["M"]
            ...     },
            ... )
            
            >>> # By name (not yet implemented)
            >>> df = client.get_data(
            ...     dataflow="KEI",
            ...     dimensions={
            ...         "LOCATION": ["FRA", "DEU"],
            ...         "INDICATOR": ["PRINTO01"],
            ...         "FREQUENCY": ["M"]
            ...     },
            ... )
        """
        # Vérification de la validité du "dataflow"
        if dataflow is None:
            raise ValueError("dataflow is required")
        
        # Normalisation des dimensions au format Dict[int, List[str]]
        normalized_dims = self._normalize_dimensions(dimensions)
        
        # Construction de la requête
        query = SDMXDataQuery(
            agency=agency,
            dataflow=dataflow,
            version=version,
            dimensions=normalized_dims,
            start_period=start_period,
            end_period=end_period,
            last_n_observations=last_n_observations,
            format=format,
            sdmx_version=self.sdmx_version,
        )
        
        # Construction de l'URL et des paramètres
        endpoint, params = self.url_builder.build_data_url(query)
        
        # Headers appropriés pour le format
        headers = {
            "Accept": self.url_builder.get_accept_header(format, self.sdmx_version),
            "Accept-Encoding": "gzip, deflate",
        }
        
        # Logging
        logger.info(f"Fetching data from {dataflow}")
        logger.debug(f"Endpoint: {endpoint}")
        logger.debug(f"Parameters: {params}")
        
        # Exécution de la requête
        response = self.api_client.get(endpoint, params=params, headers=headers)
        
        # Parsing de la réponse
        if format == ResponseFormat.JSON:
            return self._parse_json_response(response.json())
        elif format == ResponseFormat.CSV:
            return self._parse_csv_response(response.text)
        else:
            raise NotImplementedError(f"Format {format} not yet implemented")
    
    # Méthode auxiliaire de normalisation des dimensions
    def _normalize_dimensions(
        self,
        dimensions: Optional[Dict[Union[int, str], Union[str, List[str]]]],
    ) -> Dict[int, List[str]]:
        """Normalize dimensions to Dict[int, List[str]] format.
        
        Args:
            dimensions: Input dimensions in various formats
            
        Returns:
            Normalized dimensions with integer keys and list values
        """
        # Cas où les dimensions ne sont pas spécifiées
        if dimensions is None:
            return {}
        
        # Initialisation du dictionnaire résultat
        normalized = {}
        
        # Parcours des dimensions
        for key, value in dimensions.items():
            # Conversion de la clé en int si nécessaire
            if isinstance(key, str):
                # TODO: implémenter la résolution des noms de dimensions
                # Pour l'instant, on suppose que les clés sont déjà des positions
                raise NotImplementedError(
                    "Dimension names not yet supported, use positions (0, 1, 2, ...)"
                )
            
            # Conversion de la valeur en liste si nécessaire
            if isinstance(value, str):
                normalized[key] = [value]
            else:
                normalized[key] = list(value)
        
        return normalized
    
    # Méthode auxiliaire de parsing d'une réponse au format json
    def _parse_json_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Parse SDMX-JSON response to DataFrame.
        
        Args:
            data: JSON response from OECD API
            
        Returns:
            DataFrame with parsed data
        """
        try:
            # Structure SDMX-JSON
            structure = data.get("structure", {})
            dataSets = data.get("dataSets", [])
            
            # Cas où les données sont vides
            if not dataSets:
                # Logging
                logger.warning("No datasets found in response")
                return pd.DataFrame()
            
            # Extration des dimensions et de leurs valeurs
            dimensions = structure.get("dimensions", {}).get("observation", [])
            dim_names = [dim["id"] for dim in dimensions]
            dim_values = {dim["id"]: [v["id"] for v in dim["values"]] for dim in dimensions}
            
            # Extration des observations
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
                
                # Ajout à la liste des enregistrements
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
    
    # Méthode auxiliaire de parsing d'une réponse au format csv
    def _parse_csv_response(self, text: str) -> pd.DataFrame:
        """Parse SDMX-CSV response to DataFrame.
        
        Args:
            text: CSV text from OECD API
            
        Returns:
            DataFrame with parsed data
        """
        try:
            # Lecture du jeu de données
            df = pd.read_csv(StringIO(text))
            # Logging
            logger.info(f"Parsed {len(df)} rows from CSV")
            return df
            
        except Exception as e:
            # Logging
            logger.error(f"Failed to parse CSV response: {e}")
            raise
    
    # Méthode d'extraction de la structure des métadonnées associées à un flux
    def get_structure(
        self,
        agency: str = "OECD",
        dataflow: str = None,
        version: str = "1.0",
    ) -> Dict[str, Any]:
        """Retrieve dataflow structure metadata.
        
        Args:
            agency: Agency identifier
            dataflow: Dataflow identifier
            version: Dataflow version
            
        Returns:
            Structure metadata as dictionary
        """
        # Vérification que le flux de données est spécifié
        if dataflow is None:
            raise ValueError("dataflow is required")
        
        # Construction de l'url et des paramètres de requête
        endpoint, params = self.url_builder.build_structure_url(
            agency=agency,
            dataflow=dataflow,
            version=version,
            sdmx_version=self.sdmx_version,
        )
        
        # Construction des headers de requête
        headers = {
            "Accept": "application/vnd.sdmx.structure+json; charset=utf-8; version=1.0",
        }
        
        # Exécution de la requête
        response = self.api_client.get(endpoint, params=params, headers=headers)
        return response.json()
    
    # Méthode de fermeture de la session
    def close(self):
        """Close the client and release resources."""
        self.api_client.close()
    
    # Entrée dans le client
    def __enter__(self):
        """Context manager entry."""
        return self
    
    # Sortie du client
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()