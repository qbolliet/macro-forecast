"""Eurostat data client.

This module provides a high-level client for querying Eurostat data through
their SDMX 3.0 API and converting responses to pandas DataFrames.
"""
# Importation des modules
# Modules de base
from dataclasses import dataclass, field
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
from ..core.client import APIClient
from ..core.structures import (
    DataflowStructureRegistry,
    DataflowStructure,
    DimensionInfo,
)
from ..core.rate_limiter import RateLimiter

# Initialisation du logger
logger = logging.getLogger(__name__)


# Types pour la gestion des doublons
DuplicateHandling = Literal["ignore", "warn", "raise"]

# Types pour les paramètres de requêtes de structure SDMX 3.0
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


# Énumération des formats de réponse Eurostat
class EurostatResponseFormat(str, Enum):
    """Response format for Eurostat SDMX API.

    Attributes:
        CSV: SDMX-CSV format (default)
        TSV: Tab-separated values format (legacy Eurostat)
        JSON: JSON-stat 2.0 format
        XML: SDMX-ML 3.0 XML format
    """
    CSV = "csvdata"
    TSV = "tsv"
    JSON = "json"
    XML = "structurespecificdata"


# Classe représentant une requête de données Eurostat
@dataclass
class EurostatQueryRequest:
    """Represents an Eurostat data query request.

    This class encapsulates all parameters needed for a get_data() call,
    providing type safety and easier manipulation of query batches.

    Attributes:
        dataflow: Dataflow identifier (e.g., "namq_10_gdp", "DS-045409")
        version: Dataflow version (default: "*" for latest)
        dimensions: Dimension filters as Dict[str, Union[str, List[str]]]
        start_period: Start period filter
        end_period: End period filter
        last_n_observations: Number of recent observations
        first_n_observations: Number of first observations
        format: Response format
        compress: Whether to compress the response
        attributes: Attributes to include
        measures: Measures to include
        on_duplicate: Duplicate handling strategy
        split_dimensions: Dimensions to split into separate requests
        max_split_combinations: Max allowed split combinations

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

    # Méthode de conversion des arguments en dictionnaire
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for get_data() kwargs.

        Returns:
            Dictionary of parameters for get_data() method.
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

    # Méthode d'extraction de la clé associée au dataflow
    def get_dataflow_key(self) -> str:
        """Get unique key for this dataflow.

        Returns:
            Key in format 'dataflow::version'.
        """
        return f"{self.dataflow}::{self.version}"


# Initialisation du client pour la requête de données Eurostat
class EurostatClient:
    """High-level client for Eurostat SDMX 3.0 API.

    This client handles data retrieval from Eurostat's SDMX 3.0 API and provides
    convenient methods to query economic indicators and convert them to
    pandas DataFrames.

    Args:
        base_url: Eurostat API base URL (default: standard dissemination endpoint).
        timeout: Request timeout in seconds.
        structure_registry: Optional registry for dimension name resolution.
        auto_fetch_structure: If True, fetch structure metadata when needed.
        rate_limiter: Optional rate limiter for API requests.
        auto_load_rate_limit: If True, load rate limiter from parameters/eurostat.json.

    Example:
        >>> client = EurostatClient()
        >>> df = client.get_data(
        ...     dataflow="namq_10_gdp",
        ...     dimensions={"GEO": ["FR"], "FREQ": "Q"},
        ... )
    """
    # Initialisation de l'URL par défaut (dissemination SDMX 3.0)
    DEFAULT_BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0"
    # Initialisation de l'URL Comext (pour datasets DS-*)
    COMEXT_BASE_URL = "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/3.0"

    # Namespaces SDMX 3.0
    SDMX3_NS = {
        'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v3_0/message',
        'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v3_0/structure',
        'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v3_0/common',
    }
    # Namespaces SDMX 2.1 (fallback)
    SDMX21_NS = {
        'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
        'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
        'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
    }

    # Initialisation
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = 90,
        structure_registry: Optional[DataflowStructureRegistry] = None,
        auto_fetch_structure: bool = True,
        rate_limiter: Optional[RateLimiter] = None,
        auto_load_rate_limit: bool = True,
    ):
        # Initialisation des attributs
        self.base_url = base_url
        self.auto_fetch_structure = auto_fetch_structure
        self.api_client = APIClient(base_url=base_url, timeout=timeout)
        # Client API pour Comext (lazy initialization)
        self._comext_client: Optional[APIClient] = None

        # Registre des structures de dataflows
        self.structure_registry = structure_registry or DataflowStructureRegistry()

        # Chargement automatique du rate limiter si demandé
        if auto_load_rate_limit and rate_limiter is None:
            rate_limiter = self._load_rate_limiter()

        # Rate limiter pour respecter les limites API
        self.rate_limiter = rate_limiter

    # Méthode auxiliaire de chargement du rate limiter depuis le fichier de configuration
    def _load_rate_limiter(self) -> Optional[RateLimiter]:
        """Load rate limiter from parameters/eurostat.json.

        Returns:
            RateLimiter instance or None if configuration not found.
        """
        try:
            # Construction du chemin vers le fichier de paramètres
            params_path = Path(__file__).parents[3] / "parameters" / "eurostat.json"

            # Vérification de l'existence du fichier
            if params_path.exists():
                # Chargement du fichier JSON
                with open(params_path, "r", encoding="utf-8") as f:
                    config = json.load(f)

                # Extraction de la configuration du rate limiter
                if "RATE_LIMIT" in config:
                    rate_config = config["RATE_LIMIT"]
                    return RateLimiter(
                        requests=rate_config.get("requests", 30),
                        unit=rate_config.get("unit", "minutes"),
                        count=rate_config.get("count", 1),
                    )
            # Logging si pas de configuration trouvée
            logger.debug("No RATE_LIMIT configuration found")
            # Retour de None si le fichier n'existe pas ou pas de configuration rate limit
            return None
        # Gestion des erreurs de chargement
        except Exception as e:
            # Logging de l'erreur
            logger.warning(f"Failed to load rate limiter config: {e}")
            return None

    # Propriété pour accéder au client Comext (lazy initialization)
    def _get_comext_client(self) -> APIClient:
        """Get or create Comext API client.

        Returns:
            APIClient instance for Comext API.
        """
        # Création du client Comext si nécessaire
        if self._comext_client is None:
            self._comext_client = APIClient(
                base_url=self.COMEXT_BASE_URL,
                timeout=self.api_client.session.timeout if hasattr(self.api_client.session, 'timeout') else 90
            )
        return self._comext_client

    # Méthode de détection des datasets Comext
    def _is_comext_dataset(self, dataflow: str) -> bool:
        """Detect if dataflow is a Comext dataset.

        Args:
            dataflow: Dataflow identifier

        Returns:
            True if dataflow starts with 'DS-'
        """
        # Détection du préfixe DS- (Comext datasets)
        return dataflow.upper().startswith("DS-")

    # Méthode de sélection du client API approprié
    def _get_api_client(self, dataflow: str) -> APIClient:
        """Get appropriate API client for the dataflow.

        Args:
            dataflow: Dataflow identifier

        Returns:
            APIClient instance (standard or Comext)
        """
        # Sélection du client Comext pour les datasets DS-*
        if self._is_comext_dataset(dataflow):
            return self._get_comext_client()
        return self.api_client


    # Méthode de construction des paramètres de dimensions
    def _build_dimension_params(self, dimensions: Optional[Dict[str, Union[str, List[str]]]]) -> Dict[str, str]:
        """Build dimension query parameters in format c[DIM]=val1,val2.

        Args:
            dimensions: Dictionary mapping dimension names to values

        Returns:
            Query parameters dictionary
        """
        # Initialisation du dictionnaire de paramètres
        params = {}
        # Parcours des dimensions
        if dimensions:
            for dim_name, dim_values in dimensions.items():
                # Normalisation des valeurs en liste
                if isinstance(dim_values, str):
                    dim_values = [dim_values]
                # Construction du paramètre c[DIM]=val1,val2
                params[f"c[{dim_name.upper()}]"] = ",".join(dim_values)
        return params

    # Méthode de construction du paramètre de période temporelle
    def _build_time_period_param(
        self,
        start_period: Optional[str],
        end_period: Optional[str]
    ) -> Optional[str]:
        """Build TIME_PERIOD query parameter.

        Args:
            start_period: Start period in SDMX format
            end_period: End period in SDMX format

        Returns:
            TIME_PERIOD parameter value or None
        """
        # Initialisation de la liste des conditions
        conditions = []
        # Ajout de la condition de début
        if start_period:
            conditions.append(f"ge:{start_period}")
        # Ajout de la condition de fin
        if end_period:
            conditions.append(f"le:{end_period}")
        # Retour du paramètre ou None
        return "+".join(conditions) if conditions else None

    # Méthode de construction de tous les paramètres de requête de données
    def _build_data_params(
        self,
        dimensions: Optional[Dict[str, Union[str, List[str]]]],
        start_period: Optional[str],
        end_period: Optional[str],
        last_n_observations: Optional[int],
        first_n_observations: Optional[int],
        format: EurostatResponseFormat,
        compress: bool,
        attributes: Optional[str],
        measures: Optional[str],
    ) -> Dict[str, str]:
        """Build all query parameters for data request.

        Args:
            dimensions: Dimension filters
            start_period: Start period
            end_period: End period
            last_n_observations: Number of last observations
            first_n_observations: Number of first observations
            format: Response format
            compress: Whether to compress response
            attributes: Attributes to include
            measures: Measures to include

        Returns:
            Complete query parameters dictionary
        """
        # Initialisation du dictionnaire de paramètres
        params = {}

        # Ajout des paramètres de dimensions
        params.update(self._build_dimension_params(dimensions))

        # Ajout du paramètre de période temporelle
        time_param = self._build_time_period_param(start_period, end_period)
        if time_param:
            params["c[TIME_PERIOD]"] = time_param

        # Ajout des paramètres d'observations
        if last_n_observations is not None:
            params["lastNObservations"] = str(last_n_observations)
        if first_n_observations is not None:
            params["firstNObservations"] = str(first_n_observations)

        # Ajout du format et de la compression
        params["format"] = format.value
        params["compress"] = "true" if compress else "false"

        # Ajout des paramètres optionnels d'attributs et mesures
        if attributes:
            params["attributes"] = attributes
        if measures:
            params["measures"] = measures

        return params

    # Méthode de construction du header Accept selon le format
    def _get_accept_header(self, format: EurostatResponseFormat) -> str:
        """Get Accept header for the response format.

        Args:
            format: Response format

        Returns:
            Accept header value
        """
        # Mapping des formats aux content-types
        format_mapping = {
            EurostatResponseFormat.CSV: "application/vnd.sdmx.data+csv;version=3.0.0",
            EurostatResponseFormat.TSV: "text/tab-separated-values",
            EurostatResponseFormat.JSON: "application/vnd.sdmx.data+json;version=3.0.0",
            EurostatResponseFormat.XML: "application/vnd.sdmx.structurespecificdata+xml;version=3.0.0",
        }
        return format_mapping.get(format, "text/csv")

    # Méthode de parsing de réponse CSV
    def _parse_csv_response(self, text: str) -> pd.DataFrame:
        """Parse SDMX-CSV response format.

        Args:
            text: CSV response text

        Returns:
            Parsed DataFrame

        Raises:
            ValueError: If CSV parsing fails
        """
        # Parsing simple du CSV avec pandas
        try:
            df = pd.read_csv(StringIO(text))
            return df
        # Gestion des erreurs de parsing
        except Exception as e:
            logger.error(f"CSV parsing failed: {e}")
            raise ValueError(f"Failed to parse CSV response: {e}")

    # Méthode de parsing de réponse TSV Eurostat
    def _parse_tsv_response(self, text: str) -> pd.DataFrame:
        """Parse Eurostat TSV format (wide format with flags).

        The format has a specific structure:
        - First row: dimensions separated by commas, then \\TIME_PERIOD, then periods
        - Values may include flags (p, e, etc.)

        Args:
            text: TSV response text

        Returns:
            Parsed DataFrame in long format

        Raises:
            ValueError: If TSV parsing fails
        """
        try:
            # Lecture du fichier TSV
            df = pd.read_csv(StringIO(text), sep="\t")

            # Identification de la colonne TIME_PERIOD (colonne d'index)
            # La première colonne contient les dimensions séparées par des virgules
            index_col = df.columns[0]

            # Extraction des dimensions et périodes
            dimension_cols = []
            period_cols = []

            for col in df.columns[1:]:
                # Les colonnes de périodes sont des dates (ex: 2023-Q4, 2024-Q1)
                if any(char.isdigit() for char in col):
                    period_cols.append(col)
                else:
                    dimension_cols.append(col)

            # Fusion du DataFrame vers le format long (tidy)
            # Extraction des dimensions à partir de la première colonne
            dimensions_split = df[index_col].str.split(",", expand=True)

            # Attribution des noms de colonnes aux dimensions
            if not dimension_cols:
                dimension_cols = [f"DIM_{i}" for i in range(len(dimensions_split.columns))]

            dimensions_split.columns = dimension_cols[:len(dimensions_split.columns)]

            # Fusion des dimensions avec le reste du DataFrame
            df_reset = df[period_cols].copy()
            df_reset = pd.concat([dimensions_split, df_reset], axis=1)

            # Fusion vers le format long
            id_vars = list(dimensions_split.columns)
            df_long = df_reset.melt(id_vars=id_vars, var_name="TIME_PERIOD", value_name="value")

            # Nettoyage des valeurs (suppression des flags)
            df_long["value"] = df_long["value"].astype(str).str.strip()
            df_long["value"] = pd.to_numeric(df_long["value"], errors="coerce")

            return df_long
        # Gestion des erreurs de parsing
        except Exception as e:
            logger.error(f"TSV parsing failed: {e}")
            raise ValueError(f"Failed to parse TSV response: {e}")

    # Méthode de parsing de réponse JSON-stat 2.0
    def _parse_json_response(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Parse JSON-stat 2.0 response format.

        Args:
            data: JSON-stat 2.0 dictionary

        Returns:
            Parsed DataFrame

        Raises:
            ValueError: If JSON parsing fails
        """
        try:
            # Extraction des dimensions et observations
            dimensions = data.get("dimension", {})
            observations = data.get("observation", {})

            # Construction du DataFrame
            rows = []
            for obs_key, value in observations.items():
                # Parsing de la clé d'observation (format: "0:1:2:...")
                indices = list(map(int, obs_key.split(":")))
                row = {}

                # Mapping des indices aux valeurs de dimensions
                for i, (dim_name, dim_info) in enumerate(dimensions.items()):
                    if i < len(indices):
                        dim_idx = indices[i]
                        if "category" in dim_info and "index" in dim_info["category"]:
                            categories = dim_info["category"]["index"]
                            if dim_idx in categories:
                                row[dim_name] = categories[dim_idx]

                # Ajout de la valeur d'observation
                row["value"] = value
                rows.append(row)

            df = pd.DataFrame(rows)
            return df
        # Gestion des erreurs de parsing
        except Exception as e:
            logger.error(f"JSON parsing failed: {e}")
            raise ValueError(f"Failed to parse JSON response: {e}")

    # Méthode de parsing de réponse XML SDMX-ML 3.0
    def _parse_structure_response(self, xml_content: str, dataflow: str) -> DataflowStructure:
        """Parse SDMX-ML 3.0 structure response and extract dimensions.

        Args:
            xml_content: XML response content
            dataflow: Dataflow identifier

        Returns:
            DataflowStructure instance

        Raises:
            ValueError: If XML parsing fails
        """
        try:
            # Parsing du document XML
            root = ET.fromstring(xml_content)

            # Tentative avec les namespaces SDMX 3.0
            namespaces = self.SDMX3_NS
            structure_elem = root.find(".//str:DataStructure", namespaces)

            # Fallback vers SDMX 2.1 si nécessaire
            if structure_elem is None:
                namespaces = self.SDMX21_NS
                structure_elem = root.find(".//str:DataStructure", namespaces)

            # Gestion du cas où la structure est introuvable
            if structure_elem is None:
                raise ValueError("DataStructure element not found in XML response")

            # Extraction des dimensions
            dimensions = []
            dimension_list = structure_elem.find(".//str:DimensionList", namespaces)

            if dimension_list is not None:
                for i, dim in enumerate(dimension_list.findall("str:Dimension", namespaces)):
                    dim_id = dim.get("id")
                    position = dim.get("position", str(i))
                    description = None

                    # Extraction de la description si disponible
                    desc_elem = dim.find(".//com:Description", namespaces)
                    if desc_elem is not None and desc_elem.text:
                        description = desc_elem.text

                    dimensions.append(
                        DimensionInfo(
                            name=dim_id,
                            position=int(position),
                            description=description
                        )
                    )

            # Création de la structure de dataflow
            structure = DataflowStructure(
                agency="ESTAT",
                dataflow=dataflow,
                num_dimensions=len(dimensions),
                dimensions=dimensions,
                description=None
            )

            return structure
        # Gestion des erreurs de parsing
        except Exception as e:
            logger.error(f"Structure XML parsing failed: {e}")
            raise ValueError(f"Failed to parse structure response: {e}")

    # Méthode d'assurance de la disponibilité de la structure d'un dataflow
    def _ensure_structure(self, dataflow: str, version: str = "*") -> DataflowStructure:
        """Load and cache dataflow structure.

        Args:
            dataflow: Dataflow identifier
            version: Dataflow version

        Returns:
            DataflowStructure instance

        Raises:
            ValueError: If structure cannot be retrieved
        """
        # Vérification du cache d'abord
        key = f"{dataflow}::{version}"
        cached = self.structure_registry.get(key)
        if cached:
            return cached

        # Chargement de la structure via API si non cachée
        if self.auto_fetch_structure:
            structure = self.get_structure(dataflow, version)
            self.register_structure(structure)
            return structure

        # Lève une erreur si la structure n'est pas disponible
        raise ValueError(f"Structure not found for {dataflow}::{version}")

    # Méthode de normalisation des dimensions (conversion str -> List[str])
    def _normalize_dimensions(
        self,
        dimensions: Optional[Dict[str, Union[str, List[str]]]],
        structure: Optional[DataflowStructure] = None
    ) -> Optional[Dict[str, List[str]]]:
        """Normalize dimensions to Dict[str, List[str]].

        Args:
            dimensions: Input dimensions (may contain strings or lists)
            structure: Optional structure for validation

        Returns:
            Normalized dimensions or None

        Raises:
            ValueError: If dimension names are invalid
        """
        if not dimensions:
            return None

        # Normalisation des dimensions
        normalized = {}
        for dim_name, dim_values in dimensions.items():
            # Validation du nom de dimension si la structure est disponible
            if structure:
                valid_names = [d.name for d in structure.dimensions]
                if dim_name not in valid_names:
                    raise ValueError(f"Invalid dimension name: {dim_name}")

            # Conversion en liste si nécessaire
            if isinstance(dim_values, str):
                dim_values = [dim_values]

            normalized[dim_name] = dim_values

        return normalized

    # Méthode de génération des combinaisons de split_dimensions
    def _generate_request_combinations(
        self,
        dimensions: Dict[str, List[str]],
        split_dims: Optional[List[str]],
        max_combinations: int
    ) -> List[Dict[str, List[str]]]:
        """Generate dimension combinations for split requests.

        Args:
            dimensions: Normalized dimensions
            split_dims: Dimensions to split
            max_combinations: Maximum allowed combinations

        Returns:
            List of dimension dictionaries

        Raises:
            ValueError: If combinations exceed max allowed
        """
        # Si pas de split_dimensions, retour des dimensions complètes
        if not split_dims:
            return [dimensions]

        # Sélection des dimensions à splitter
        split_dims_set = set(split_dims)
        split_dict = {k: v for k, v in dimensions.items() if k in split_dims_set}
        keep_dict = {k: v for k, v in dimensions.items() if k not in split_dims_set}

        # Génération du produit cartésien
        split_keys = list(split_dict.keys())
        split_values = [split_dict[k] for k in split_keys]

        combinations = list(itertools.product(*split_values))

        # Vérification du nombre de combinaisons
        if len(combinations) > max_combinations:
            raise ValueError(
                f"Split combinations ({len(combinations)}) exceed max allowed ({max_combinations})"
            )

        # Construction des dictionnaires de dimensions
        result = []
        for combo in combinations:
            dims = keep_dict.copy()
            for key, val in zip(split_keys, combo):
                dims[key] = [val]
            result.append(dims)

        return result

    # Méthode d'exécution de requêtes splitées
    def _execute_split_requests(
        self,
        dataflow: str,
        version: str,
        request_combinations: List[Dict[str, List[str]]],
        start_period: Optional[str],
        end_period: Optional[str],
        last_n_observations: Optional[int],
        first_n_observations: Optional[int],
        format: EurostatResponseFormat,
        compress: bool,
        attributes: Optional[str],
        measures: Optional[str],
    ) -> pd.DataFrame:
        """Execute multiple split requests and concatenate results.

        Args:
            dataflow: Dataflow identifier
            version: Dataflow version
            request_combinations: List of dimension combinations
            start_period: Start period
            end_period: End period
            last_n_observations: Number of last observations
            first_n_observations: Number of first observations
            format: Response format
            compress: Whether to compress
            attributes: Attributes to include
            measures: Measures to include

        Returns:
            Concatenated DataFrame from all requests
        """
        # Initialisation de la liste des résultats
        dfs = []

        # Exécution de chaque requête splitée
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
                    format=format,
                    compress=compress,
                    attributes=attributes,
                    measures=measures,
                    on_duplicate="ignore",  # Évite les logs de doublons répétés
                    split_dimensions=None,  # Désactive le split récursif
                )
                dfs.append(df)
            # Gestion des erreurs par requête
            except Exception as e:
                logger.error(f"Split request failed for {dims}: {e}")
                continue

        # Concaténation des résultats
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            return pd.DataFrame()

    # Méthode de filtrage post-API du DataFrame
    def _filter_dataframe_by_dimensions(
        self,
        df: pd.DataFrame,
        filters: Optional[Dict[str, Union[str, List[str]]]]
    ) -> pd.DataFrame:
        """Post-filter DataFrame by dimension values.

        Args:
            df: Input DataFrame
            filters: Filters to apply

        Returns:
            Filtered DataFrame
        """
        if not filters or df.empty:
            return df

        # Application des filtres
        result = df.copy()
        for col, values in filters.items():
            if col in result.columns:
                # Normalisation des valeurs
                if isinstance(values, str):
                    values = [values]
                # Application du filtre
                result = result[result[col].isin(values)]

        return result

    # Méthode de vérification des doublons
    def _check_duplicates(
        self,
        df: pd.DataFrame,
        dimensions: Optional[Dict[str, Union[str, List[str]]]],
        structure: Optional[DataflowStructure],
        on_duplicate: DuplicateHandling
    ) -> None:
        """Check for duplicates in the DataFrame.

        Args:
            df: DataFrame to check
            dimensions: Dimension names
            structure: Dataflow structure
            on_duplicate: Handling strategy

        Raises:
            ValueError: If on_duplicate='raise' and duplicates found
        """
        if df.empty or not dimensions:
            return

        # Identification des colonnes de dimensions
        dim_cols = [d.name for d in structure.dimensions] if structure else list(dimensions.keys())
        available_cols = [c for c in dim_cols if c in df.columns]

        if not available_cols:
            return

        # Vérification des doublons
        duplicates = df[available_cols + ["TIME_PERIOD"]].duplicated().sum() if "TIME_PERIOD" in df.columns else 0

        if duplicates > 0:
            message = f"Found {duplicates} duplicate rows"
            if on_duplicate == "raise":
                raise ValueError(message)
            elif on_duplicate == "warn":
                logger.warning(message)

    # Méthode publique de récupération de la structure d'un dataflow
    def get_structure(self, dataflow: str, version: str = "~") -> DataflowStructure:
        """Get structure metadata for a dataflow.

        Queries the SDMX 3.0 ``/structure/datastructure/`` endpoint to retrieve
        the Data Structure Definition (DSD) associated with the given dataflow.

        Args:
            dataflow: Dataflow identifier (e.g., ``"namq_10_gdp"``).
            version: Dataflow version. Use ``"~"`` for the latest (default).

        Returns:
            DataflowStructure with dimension information.

        Raises:
            ValueError: If structure cannot be retrieved.
        """
        # Construction de l'URL (endpoint datastructure, pas dataflow)
        endpoint = f"/structure/datastructure/ESTAT/{dataflow.upper()}/{version}"

        # Sélection du client API
        client = self._get_api_client(dataflow)

        # Requête de la structure
        try:
            response = client.get(endpoint)
            structure = self._parse_structure_response(response.text, dataflow)
            return structure
        # Gestion des erreurs
        except Exception as e:
            logger.error(f"Failed to get structure for {dataflow}: {e}")
            raise ValueError(f"Failed to get structure for {dataflow}: {e}")

    # Méthode privée générique de requête d'artefacts structurels SDMX
    def _fetch_structure_artefact(
        self,
        artefact_type: str,
        resource_id: str,
        agency: str = "ESTAT",
        version: str = "~",
        detail: StructureDetail = "full",
        references: StructureReferences = "none",
        dataflow: Optional[str] = None,
    ) -> str:
        """Fetch a raw SDMX structure artefact and return its XML content.

        Builds and executes a request to:
        ``/structure/{artefact_type}/{agency}/{resource_id}/{version}``

        Args:
            artefact_type: SDMX artefact type (e.g., ``"codelist"``,
                ``"datastructure"``, ``"dataflow"``, ``"dataconstraint"``).
            resource_id: Artefact identifier.
            agency: Maintaining agency (default: ``"ESTAT"``).
            version: Artefact version. Use ``"~"`` for the latest (default).
            detail: Level of detail in the response (default: ``"full"``).
                See ``StructureDetail`` for allowed values.
            references: Whether to include referenced artefacts
                (default: ``"none"``). See ``StructureReferences`` for
                allowed values.
            dataflow: Optional dataflow identifier used only to select the
                correct API client (standard vs. Comext).

        Returns:
            Raw XML response text.

        Raises:
            ValueError: If the request fails.
        """
        # Construction de l'endpoint générique
        endpoint = f"/structure/{artefact_type}/{agency}/{resource_id}/{version}"
        params: Dict[str, str] = {"detail": detail, "references": references}

        # Sélection du client API (Comext si nécessaire)
        client = self._get_api_client(dataflow or resource_id)

        try:
            response = client.get(endpoint, params=params)
            return response.text
        except Exception as e:
            logger.error(f"Failed to fetch {artefact_type}/{resource_id}: {e}")
            raise ValueError(
                f"Failed to fetch {artefact_type} '{resource_id}': {e}"
            )

    # Méthode publique de récupération d'une codelist
    def get_codelist(
        self,
        codelist_id: str,
        agency: str = "ESTAT",
        version: str = "~",
        detail: StructureDetail = "full",
        references: StructureReferences = "none",
    ) -> str:
        """Retrieve a codelist from the SDMX 3.0 structure API.

        Queries ``/structure/codelist/{agency}/{codelist_id}/{version}``.
        Codelists define the controlled vocabulary (allowed codes) for a
        given dimension (e.g., ``CL_GEO`` for geographic areas).

        Args:
            codelist_id: Codelist identifier (e.g., ``"CL_GEO"``).
            agency: Maintaining agency (default: ``"ESTAT"``).
            version: Codelist version. Use ``"~"`` for the latest (default).
            detail: Level of detail in the response (default: ``"full"``).
            references: Whether to include referenced artefacts
                (default: ``"none"``).

        Returns:
            Raw SDMX-ML XML response text.

        Raises:
            ValueError: If the codelist cannot be retrieved.

        Example:
            >>> xml = client.get_codelist("CL_GEO")
            >>> xml_with_parents = client.get_codelist(
            ...     "CL_GEO", references="parents"
            ... )
        """
        return self._fetch_structure_artefact(
            artefact_type="codelist",
            resource_id=codelist_id,
            agency=agency,
            version=version,
            detail=detail,
            references=references,
        )

    # Méthode publique de récupération d'un dataflow
    def get_dataflow(
        self,
        dataflow_id: str,
        agency: str = "ESTAT",
        version: str = "~",
        detail: StructureDetail = "full",
        references: StructureReferences = "none",
    ) -> str:
        """Retrieve a dataflow definition from the SDMX 3.0 structure API.

        Queries ``/structure/dataflow/{agency}/{dataflow_id}/{version}``.
        A dataflow provides a reference to the Data Structure Definition (DSD)
        that applies to a particular statistical domain.

        Use ``references="children"`` to also retrieve the associated DSD, or
        ``references="descendants"`` to additionally include the codelists.

        Args:
            dataflow_id: Dataflow identifier (e.g., ``"namq_10_gdp"``).
            agency: Maintaining agency (default: ``"ESTAT"``).
            version: Dataflow version. Use ``"~"`` for the latest (default).
            detail: Level of detail in the response (default: ``"full"``).
            references: Whether to include referenced artefacts
                (default: ``"none"``). Supported values for dataflow:
                ``"none"``, ``"children"`` (returns DF + DSD),
                ``"descendants"`` (returns DF + DSD + CS + CL).

        Returns:
            Raw SDMX-ML XML response text.

        Raises:
            ValueError: If the dataflow cannot be retrieved.

        Example:
            >>> xml = client.get_dataflow("namq_10_gdp")
            >>> xml_with_dsd = client.get_dataflow(
            ...     "namq_10_gdp", references="descendants"
            ... )
        """
        return self._fetch_structure_artefact(
            artefact_type="dataflow",
            resource_id=dataflow_id,
            agency=agency,
            version=version,
            detail=detail,
            references=references,
            dataflow=dataflow_id,
        )

    # Méthode publique de récupération d'une dataconstraint
    def get_dataconstraint(
        self,
        constraint_id: str,
        agency: str = "ESTAT",
        version: str = "~",
        detail: StructureDetail = "full",
        references: StructureReferences = "none",
    ) -> str:
        """Retrieve a data constraint from the SDMX 3.0 structure API.

        Queries ``/structure/dataconstraint/{agency}/{constraint_id}/{version}``.
        Data constraints define the valid combinations of dimension values for
        a given dataset or dataflow (i.e., the actual content constraint).

        Args:
            constraint_id: Constraint identifier (typically the same as the
                dataflow identifier).
            agency: Maintaining agency (default: ``"ESTAT"``).
            version: Constraint version. Use ``"~"`` for the latest (default).
            detail: Level of detail in the response (default: ``"full"``).
            references: Whether to include referenced artefacts
                (default: ``"none"``). Supported values: ``"none"``,
                ``"parents"``, ``"parentsandsiblings"``, ``"children"``,
                ``"descendants"``, ``"all"``.

        Returns:
            Raw SDMX-ML XML response text.

        Raises:
            ValueError: If the constraint cannot be retrieved.

        Example:
            >>> xml = client.get_dataconstraint("namq_10_gdp")
            >>> xml_full = client.get_dataconstraint(
            ...     "namq_10_gdp", references="children"
            ... )
        """
        return self._fetch_structure_artefact(
            artefact_type="dataconstraint",
            resource_id=constraint_id,
            agency=agency,
            version=version,
            detail=detail,
            references=references,
            dataflow=constraint_id,
        )

    # Méthode publique d'enregistrement d'une structure pré-chargée
    def register_structure(self, structure: DataflowStructure) -> None:
        """Register a pre-loaded structure.

        Args:
            structure: DataflowStructure to register
        """
        # Enregistrement de la structure dans le registre
        self.structure_registry.register(structure)
        logger.info(f"Registered structure for {structure.dataflow}")

    # Méthode publique de récupération de données (méthode principale)
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
            dataflow: Dataflow identifier
            version: Dataflow version (default: "*" for latest)
            dimensions: Dimension filters
            start_period: Start period
            end_period: End period
            last_n_observations: Number of last observations
            first_n_observations: Number of first observations
            format: Response format (default: CSV)
            compress: Whether to compress (default: False)
            attributes: Attributes to include
            measures: Measures to include
            on_duplicate: Duplicate handling strategy
            split_dimensions: Dimensions to split into separate requests
            max_split_combinations: Maximum split combinations allowed

        Returns:
            DataFrame with retrieved data

        Raises:
            ValueError: If data retrieval fails
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
        # Gestion des erreurs de structure (non fatale)
        except Exception as e:
            logger.warning(f"Could not load structure: {e}")

        # Gestion du split_dimensions
        if split_dimensions and normalized_dims:
            request_combinations = self._generate_request_combinations(
                normalized_dims,
                split_dimensions,
                max_split_combinations
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
                measures
            )

        # Construction de l'endpoint
        endpoint = self._build_data_endpoint(dataflow, version)

        # Construction des paramètres
        params = self._build_data_params(
            normalized_dims,
            start_period,
            end_period,
            last_n_observations,
            first_n_observations,
            format,
            compress,
            attributes,
            measures
        )

        # Sélection du client API
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
        # Gestion des erreurs
        except Exception as e:
            logger.error(f"Data retrieval failed: {e}")
            raise ValueError(f"Failed to retrieve data from {dataflow}: {e}")

    # Méthode publique d'exécution d'un objet query
    def execute_query(self, query: EurostatQueryRequest) -> pd.DataFrame:
        """Execute an EurostatQueryRequest.

        Args:
            query: EurostatQueryRequest instance

        Returns:
            DataFrame with retrieved data

        Raises:
            ValueError: If query execution fails
        """
        # Exécution de la requête avec les paramètres de la query
        return self.get_data(**query.to_dict())

    # Méthode publique de listage de tous les dataflows
    def list_all_dataflows(
        self,
        detail: StructureDetail = "allstubs",
        lang: str = "en",
    ) -> pd.DataFrame:
        """List all available Eurostat dataflows.

        Queries the SDMX 3.0 structure endpoint using the wildcard syntax
        ``/structure/dataflow/ESTAT/*/~``, which is the correct way to
        enumerate all published dataflows.

        Args:
            detail: Level of detail in the response (default: ``"allstubs"``).
                ``"allstubs"`` returns only the identifier and name for each
                dataflow, which is significantly more efficient than ``"full"``
                for a catalogue listing.
            lang: Language code for name labels (default: ``"en"``).
                Accepted values: ``"en"``, ``"fr"``, ``"de"``.

        Returns:
            DataFrame with columns: ``dataflow``, ``agency``, ``version``,
            ``name``.

        Raises:
            ValueError: If dataflows cannot be retrieved.

        Example:
            >>> df = client.list_all_dataflows()
            >>> df_fr = client.list_all_dataflows(lang="fr")
        """
        # Endpoint wildcard SDMX 3.0 pour la liste complète des dataflows ESTAT
        endpoint = "/structure/dataflow/ESTAT/*/~"
        params: Dict[str, str] = {"detail": detail, "lang": lang}

        try:
            response = self.api_client.get(endpoint, params=params)

            # Parsing du XML : tentative SDMX 3.0 puis fallback SDMX 2.1
            root = ET.fromstring(response.text)
            namespaces = self.SDMX3_NS
            dataflow_elems = root.findall(".//str:Dataflow", namespaces)
            if not dataflow_elems:
                namespaces = self.SDMX21_NS
                dataflow_elems = root.findall(".//str:Dataflow", namespaces)

            # Extraction des métadonnées de chaque dataflow
            dataflows = []
            for df_elem in dataflow_elems:
                df_id = df_elem.get("id")
                df_version = df_elem.get("version", "~")

                # Extraction du nom (première balise Name disponible)
                df_name = None
                name_elem = df_elem.find(".//com:Name", namespaces)
                if name_elem is not None and name_elem.text:
                    df_name = name_elem.text

                dataflows.append({
                    "dataflow": df_id,
                    "agency": "ESTAT",
                    "version": df_version,
                    "name": df_name,
                })

            df = pd.DataFrame(dataflows)
            logger.info(f"Retrieved {len(df)} dataflows")
            return df
        # Gestion des erreurs
        except Exception as e:
            logger.error(f"Failed to list dataflows: {e}")
            raise ValueError(f"Failed to list dataflows: {e}")

    # Méthode de fermeture des ressources
    def close(self) -> None:
        """Close API client connections.

        This method should be called when the client is no longer needed
        to ensure proper cleanup of network resources.
        """
        # Fermeture du client API standard
        if self.api_client:
            self.api_client.close()
        # Fermeture du client Comext si initialisé
        if self._comext_client:
            self._comext_client.close()
        logger.info("Eurostat client closed")

    # Méthodes de context manager
    def __enter__(self) -> "EurostatClient":
        """Context manager entry.

        Returns:
            Self for use in with statement
        """
        return self

    # Sortie du context manager
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit.

        Args:
            exc_type: Exception type if raised
            exc_val: Exception value if raised
            exc_tb: Exception traceback if raised
        """
        self.close()
