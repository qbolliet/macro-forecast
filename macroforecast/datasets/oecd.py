"""OECD data client.

This module provides a high-level client for querying OECD data through
their SDMX API and converting responses to pandas DataFrames.
"""
# Importation des modules
# Modules de base
from io import StringIO
import logging
from typing import Optional, Dict, List, Any, Union, Literal
import json
import warnings
from pathlib import Path
import xml.etree.ElementTree as ET
import pandas as pd

# Utilitaires internes au package pour la requête de données au format SDMX
from .client import APIClient
from .sdmx import (
    SDMXURLBuilder,
    SDMXDataQuery,
    SDMXVersion,
    ResponseFormat,
    DimensionAtObservation,
)
from .structures import (
    DataflowStructureRegistry,
    DataflowStructure,
    DimensionInfo,
)
from .rate_limiter import RateLimiter

# Initialisation du logger
logger = logging.getLogger(__name__)


# Types pour la gestion des doublons
DuplicateHandling = Literal["ignore", "warn", "raise"]


# Initialisation du client pour la requête de données
class OECDClient:
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
        # Initialisation des attributs
        self.base_url = base_url
        self.sdmx_version = sdmx_version
        self.auto_fetch_structure = auto_fetch_structure
        self.api_client = APIClient(base_url=base_url, timeout=timeout)
        self.url_builder = SDMXURLBuilder()

        # Registre des structures de dataflows
        self.structure_registry = structure_registry or DataflowStructureRegistry()

        # Chargement automatique du rate limiter si demandé
        if auto_load_rate_limit and rate_limiter is None:
            rate_limiter = self._load_rate_limiter()

        # Rate limiter pour respecter les limites API
        self.rate_limiter = rate_limiter

    # Méthode auxiliaire de chargement du rate limiter depuis le fichier de configuration
    def _load_rate_limiter(self) -> Optional[RateLimiter]:
        """Load rate limiter from parameters/oecd.json.

        Returns:
            RateLimiter instance or None if configuration not found.
        """
        try:
            # Construction du chemin vers le fichier de paramètres
            params_path = Path(__file__).parent.parent.parent / "parameters" / "oecd.json"

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
        format: ResponseFormat = ResponseFormat.CSV_LABELS,
        dimension_at_observation: DimensionAtObservation = DimensionAtObservation.ALL_DIMENSIONS,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        on_duplicate: DuplicateHandling = "warn",
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

        # Application du rate limiter si configuré
        if self.rate_limiter:
            self.rate_limiter.acquire()

        # Récupération de la structure du dataflow si nécessaire
        structure = self._ensure_structure(agency=agency, dataflow=dataflow)
        
        # Détermination du nombre de dimensions
        num_dimensions = structure.num_dimensions if structure else None
        
        # Normalisation des dimensions au format Dict[int, List[str]]
        normalized_dims = self._normalize_dimensions(
            dimensions=dimensions,
            agency=agency,
            dataflow=dataflow,
            structure=structure,
        )
        
        # Détermination des dimensions avec wildcards pour la vérification des doublons
        wildcard_positions = self._get_wildcard_positions(
            normalized_dims,
            num_dimensions,
        )
        
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
            dimension_at_observation=dimension_at_observation,
            num_dimensions=num_dimensions,
            attributes=attributes,
            measures=measures,
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
            df = self._parse_json_response(response.json())
        elif format in (ResponseFormat.CSV, ResponseFormat.CSV_LABELS):
            df = self._parse_csv_response(response.text)
        else:
            raise NotImplementedError(f"Format {format} not yet implemented")
        
        # Vérification des doublons si des wildcards sont utilisés
        if wildcard_positions and on_duplicate != "ignore":
            self._check_duplicates(
                df,
                normalized_dims,
                structure,
                on_duplicate,
            )
        
        return df
    
    # Méthode auxiliaire de vérification de la disponibilité des méta-données pour le dataflow
    def _ensure_structure(
        self,
        agency: str,
        dataflow: str,
    ) -> Optional[DataflowStructure]:
        """Ensure structure metadata is available for the dataflow.
        
        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            
        Returns:
            DataflowStructure or None if not available.
        """
        # Vérification si la structure est déjà enregistrée
        if self.structure_registry.has(agency, dataflow):
            return self.structure_registry.get(agency, dataflow)
        
        # Récupération automatique si activée
        if self.auto_fetch_structure:
            try:
                # Logging
                logger.info(f"Fetching structure for {agency}::{dataflow}")
                # Récupération de la structure par appel API
                structure = self.get_structure(agency, dataflow)
                # Enregistrement de la structure
                self.structure_registry.register(structure)
                return structure
            except Exception as e:
                # Logging
                logger.warning(
                    f"Failed to fetch structure for {agency}::{dataflow}: {e}"
                )
        
        return None
    
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
    
    # Méthode auxiliaire d'extraction des dimensions qui ne sont pas explicitement filtrées
    def _get_wildcard_positions(
        self,
        dimensions: Dict[int, List[str]],
        num_dimensions: Optional[int],
    ) -> List[int]:
        """Get positions that will use wildcards (not explicitly filtered).
        
        Args:
            dimensions: Normalized dimension filters.
            num_dimensions: Total number of dimensions.
            
        Returns:
            List of positions that are not explicitly filtered.
        """
        # Cas où les dimensions ne sont pas spécifiées
        if num_dimensions is None:
            return []
        # Extraction des positions filtrées
        filtered_positions = set(dimensions.keys())
        # Retourne le complémentaire de ces dimensions
        return [i for i in range(num_dimensions) if i not in filtered_positions]
    
    # Méthode auxiliaire de vérification des éventuels duplicats induits par les dimensions non filtrées
    def _check_duplicates(
        self,
        df: pd.DataFrame,
        dimensions: Dict[int, List[str]],
        structure: Optional[DataflowStructure],
        on_duplicate: DuplicateHandling,
    ) -> None:
        """Check for duplicate rows based on filtered dimensions.
        
        Args:
            df: DataFrame to check.
            dimensions: Dimension filters used in the query.
            structure: Dataflow structure for column name mapping.
            on_duplicate: How to handle duplicates ("warn" or "raise").
            
        Raises:
            DuplicateRowsError: If on_duplicate="raise" and duplicates found.
        """
        # Vérification liminaire que le DataFrame est non vide
        if df.empty:
            return
        
        # Détermination des colonnes à utiliser pour la vérification
        check_columns = []
        
        # Récupération des noms des dimensions filtrées
        for position in dimensions.keys():
            if structure:
                # Extraction du nom associé à la position
                dim_name = structure.get_name(position)
                # Ajout de la dimension de filtre si elle est bien comprise 
                if dim_name and dim_name in df.columns:
                    check_columns.append(dim_name)
        
        # Ajout de 'TIME_PERIOD' si présent
        if "TIME_PERIOD" in df.columns:
            check_columns.append("TIME_PERIOD")
        
        # Si aucune colonne identifiée, utilisation de toutes les colonnes sauf 'value'
        if not check_columns:
            check_columns = [
                col for col in df.columns
                if col.lower() not in ("value", "obs_value", "obsvalue")
            ]
        
        # Vérification des doublons
        duplicates = df.duplicated(subset=check_columns, keep=False)
        num_duplicates = duplicates.sum()
        
        # Renvoi d'un message si des duplicats sont identifiés
        if num_duplicates > 0:
            # Construction du message
            dup_df = df[duplicates].head(10)
            message = (
                f"Found {num_duplicates} duplicate rows for columns {check_columns}. "
                f"This may indicate that undesired values are included via wildcards (*). "
                f"Examples:\n{dup_df.to_string()}"
            )
            # Cas d'erreur
            if on_duplicate == "raise":
                raise ValueError(message)
            # Warning
            else:
                # Warning
                warnings.warn(message, UserWarning)
                # Logging
                logger.warning(message)
    
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
        return self.create_structure_from_api_response(agency=agency, dataflow=dataflow, api_response=response.json())
    
    # Fonction utilitaire pour créer une structure à partir des métadonnées API
    def create_structure_from_api_response(
        self,
        agency: str,
        dataflow: str,
        api_response: Dict[str, Any],
    ) -> DataflowStructure:
        """Create a DataflowStructure from OECD API structure response.
        
        This function parses the structure metadata returned by the OECD API
        and creates a DataflowStructure object.
        
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
            # Extraction des dimensions depuis la réponse
            # La structure varie selon la version de l'API
            data = api_response.get("data", api_response)
            structures = data.get("structures", data.get("structure", {}))
            
            # Recherche des dimensions
            dimensions_data = []
            
            # Format v1: structure.dimensions.observation
            if "dimensions" in structures:
                dims = structures["dimensions"]
                if "observation" in dims:
                    dimensions_data = dims["observation"]
                elif isinstance(dims, list):
                    dimensions_data = dims
            
            # Format v2: peut varier
            elif "dataStructures" in data:
                ds_list = data["dataStructures"]
                if ds_list:
                    # Extraction des données de dimensions
                    ds = ds_list[0]
                    components = ds.get("dataStructureComponents", {})
                    dim_list = components.get("dimensionList", {})
                    dimensions_data = dim_list.get("dimensions", [])
            
            # Construction des DimensionInfo
            dimensions = []
            # Parcours des dimensions
            for i, dim_data in enumerate(dimensions_data):
                # Extraction de l'identifiant de la dimension
                dim_id = dim_data.get("id", dim_data.get("name", f"DIM_{i}"))
                # Extraction du nom d ela dimension
                dim_name = dim_data.get("name", dim_id)
                # Extraction de la position de la dimension
                position = dim_data.get("position", dim_data.get("keyPosition", i))
                # Ajout des informations de la dimension
                dimensions.append(DimensionInfo(
                    name=dim_id,
                    position=position,
                    description=dim_name if dim_name != dim_id else None,
                ))
            
            # Tri par position
            dimensions.sort(key=lambda d: d.position)
            # Création de la structure
            return DataflowStructure(
                agency=agency,
                dataflow=dataflow,
                num_dimensions=len(dimensions),
                dimensions=dimensions,
            )
            
        except Exception as e:
            # Logging
            logger.error(f"Error parsing structure: {e}")
            raise ValueError(f"Unable to parse structure: {e}")
    
    # Méthode d'enregistrement de la structure d'un dataflow
    def register_structure(self, structure: DataflowStructure) -> None:
        """Register a dataflow structure for dimension name resolution.
        
        Args:
            structure: DataflowStructure to register.
        """
        self.structure_registry.register(structure)
    
    # Méthode de listing de tous les dataflows disponibles
    def list_all_dataflows(self) -> pd.DataFrame:
        """List all available OECD dataflows.

        Retrieves the complete list of dataflows from OECD SDMX API
        and parses them into a pandas DataFrame.

        Returns:
            DataFrame with columns: dataflow, agency, version, name

        Example:
            >>> client = OECDClient()
            >>> df = client.list_all_dataflows()
            >>> df.head()
        """
        # Endpoint pour lister tous les dataflows
        endpoint = "dataflow/all"

        # Headers pour XML
        headers = {"Accept": "application/xml"}

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