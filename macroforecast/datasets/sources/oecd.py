"""OECD data client.

This module provides a high-level client for querying OECD data through
their SDMX API and converting responses to pandas DataFrames.
"""
# Importation des modules
# Modules de base
from dataclasses import dataclass
from datetime import datetime
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
from ..core.sdmx import (
    SDMXURLBuilder,
    SDMXDataQuery,
    SDMXVersion,
    ResponseFormat,
    DimensionAtObservation,
)
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


# Classe représentant une requête de données
@dataclass
class QueryRequest:
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
        >>> query = QueryRequest(
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
    format: ResponseFormat = ResponseFormat.CSV_LABELS
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

        # Normalisation de split_dimensions (convertit tout en NOMS)
        split_dimension_names = self._normalize_split_dimensions(
            split_dimensions, structure, normalized_dims
        )

        # Génération des combinaisons de requêtes
        request_combinations = self._generate_request_combinations(
            normalized_dims, split_dimension_names, structure, max_split_combinations
        )

        # Headers appropriés pour le format
        headers = {
            "Accept": self.url_builder.get_accept_header(format, self.sdmx_version),
            "Accept-Encoding": "gzip, deflate",
        }

        # Branchement selon le nombre de combinaisons
        if len(request_combinations) == 1:
            # Une seule requête (comportement par défaut ou toutes dims single-value)
            dims_for_url, dims_for_postfilter = request_combinations[0]

            # Construction de la requête
            query = SDMXDataQuery(
                agency=agency,
                dataflow=dataflow,
                version=version,
                dimensions=dims_for_url,
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

            # IMPORTANT: Filtrer AVANT check_duplicates
            if dims_for_postfilter:
                df = self._filter_dataframe_by_dimensions(df, dims_for_postfilter)

            # Vérification des doublons (sur df filtré)
            if on_duplicate != "ignore":
                # Détermination des dimensions avec wildcards pour la vérification des doublons
                wildcard_positions = self._get_wildcard_positions(
                    dims_for_url,
                    num_dimensions,
                )
                if wildcard_positions:
                    self._check_duplicates(
                        df,
                        normalized_dims,
                        structure,
                        on_duplicate,
                    )

            return df

        else:
            # Requêtes multiples
            logger.info(f"Fetching data from {dataflow} using {len(request_combinations)} split requests")
            df = self._execute_split_requests(
                request_combinations,
                agency, dataflow, version, structure,
                start_period, end_period, last_n_observations,
                format, dimension_at_observation, attributes, measures,
                headers
            )
            # Note: _execute_split_requests applique déjà _filter_dataframe_by_dimensions
            # sur chaque réponse individuelle avant concaténation

            # Vérification des doublons sur résultat final (déjà filtré)
            if on_duplicate != "ignore":
                self._check_duplicates(
                    df,
                    normalized_dims,
                    structure,
                    on_duplicate,
                )

            return df

    # Méthode d'exécution d'une requête QueryRequest
    def execute_query(self, query: QueryRequest) -> pd.DataFrame:
        """Execute a QueryRequest.

        Args:
            query: QueryRequest object containing all parameters.

        Returns:
            DataFrame with the retrieved data.

        Example:
            >>> query = QueryRequest(
            ...     agency="OECD.SDD.STES",
            ...     dataflow="DSD_KEI@DF_KEI",
            ...     dimensions={"REF_AREA": ["FRA"], "FREQ": "M"},
            ... )
            >>> df = client.execute_query(query)
        """
        return self.get_data(**query.to_dict())

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
        structure: DataflowStructure,
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
        # Conversion des noms en positions
        split_positions = [
            structure.get_position(name)
            for name in split_dimension_names
        ]

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

            # Convertir postfilter_dims en noms
            postfilter_dims_by_name: Dict[str, List[str]] = {}
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

    # Méthode auxiliaire de filtrage du DataFrame selon des valeurs de dimensions
    def _filter_dataframe_by_dimensions(
        self,
        df: pd.DataFrame,
        dimension_filters: Dict[str, List[str]],
    ) -> pd.DataFrame:
        """Filter DataFrame by dimension values after retrieval.

        Args:
            df: DataFrame to filter
            dimension_filters: Dict mapping dimension NAME to list of allowed values

        Returns:
            Filtered DataFrame
        """
        # Cas où le DataFrame est vide
        if df.empty:
            return df

        # Cas où il n'y a pas de filtres
        if not dimension_filters:
            return df

        # Initialisation du masque (toutes les lignes acceptées)
        mask = pd.Series([True] * len(df), index=df.index)

        # Parcours des dimensions à filtrer
        for dim_name, allowed_values in dimension_filters.items():
            # Vérification que la colonne existe
            if dim_name not in df.columns:
                logger.warning(
                    f"Dimension column '{dim_name}' not found in DataFrame. "
                    f"Available columns: {list(df.columns)}. Skipping this filter."
                )
                continue

            # Application du filtre
            mask &= df[dim_name].isin(allowed_values)

        # Application du masque
        filtered_df = df[mask]

        # Logging si des lignes ont été filtrées
        if len(filtered_df) < len(df):
            logger.info(
                f"Filtered {len(df) - len(filtered_df)} rows by dimensions "
                f"{list(dimension_filters.keys())}"
            )

        return filtered_df

    # Méthode auxiliaire d'exécution de requêtes multiples
    def _execute_split_requests(
        self,
        request_combinations: List[Tuple[Dict[int, List[str]], Dict[str, List[str]]]],
        agency: str,
        dataflow: str,
        version: str,
        structure: Optional[DataflowStructure],
        start_period: Optional[str],
        end_period: Optional[str],
        last_n_observations: Optional[int],
        format: ResponseFormat,
        dimension_at_observation: DimensionAtObservation,
        attributes: Optional[str],
        measures: Optional[str],
        headers: Dict[str, str],
    ) -> pd.DataFrame:
        """Execute multiple API requests and concatenate results.

        Args:
            request_combinations: List of (dims_for_url, dims_for_postfilter) tuples
                - dims_for_url: Dict[int, List[str]] for URL construction
                - dims_for_postfilter: Dict[str, List[str]] dimension names → values
            agency: Agency identifier
            dataflow: Dataflow identifier
            version: Dataflow version
            structure: Dataflow structure
            start_period: Start period
            end_period: End period
            last_n_observations: Number of recent observations
            format: Response format
            dimension_at_observation: How to group observations
            attributes: Attributes to include
            measures: Measures to include
            headers: HTTP headers

        Returns:
            Concatenated DataFrame from all requests

        Raises:
            ValueError: If all requests failed or returned empty results
        """
        # Initialisation de la liste des DataFrames
        all_dataframes: List[pd.DataFrame] = []
        errors: List[str] = []

        # Logging
        logger.info(f"Executing {len(request_combinations)} API requests")

        # Extraction du nombre de dimensions (pour la requête)
        num_dimensions = structure.num_dimensions if structure else None

        # Parcours des combinaisons
        for i, (dims_url, dims_postfilter) in enumerate(request_combinations):
            # Application du rate limiter
            if self.rate_limiter:
                self.rate_limiter.acquire()

            # Construction de la requête
            query = SDMXDataQuery(
                agency=agency,
                dataflow=dataflow,
                version=version,
                dimensions=dims_url,
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

            # Construction de l'URL
            endpoint, params = self.url_builder.build_data_url(query)

            # Logging progress tous les 10 requêtes
            if (i + 1) % 10 == 0 or i == 0 or i == len(request_combinations) - 1:
                logger.info(f"Processing request {i+1}/{len(request_combinations)}")
                logger.debug(f"Endpoint: {endpoint}")

            # Exécution de la requête avec gestion d'erreur
            try:
                # Exécution
                response = self.api_client.get(endpoint, params=params, headers=headers)

                # Parsing de la réponse
                if format == ResponseFormat.JSON:
                    df = self._parse_json_response(response.json())
                elif format in (ResponseFormat.CSV, ResponseFormat.CSV_LABELS):
                    df = self._parse_csv_response(response.text)
                else:
                    raise NotImplementedError(f"Format {format} not yet implemented")

                # Vérification que le DataFrame n'est pas vide
                if not df.empty:
                    # Application du post-filtre si nécessaire
                    if dims_postfilter:
                        df = self._filter_dataframe_by_dimensions(df, dims_postfilter)

                    # Ajout à la liste si toujours non vide après filtrage
                    if not df.empty:
                        all_dataframes.append(df)
                    else:
                        logger.debug(f"Request {i+1} returned empty after filtering")
                else:
                    logger.debug(f"Request {i+1} returned empty DataFrame")

            except Exception as e:
                # Logging de l'erreur avec contexte
                dim_values_str = ", ".join(
                    f"{pos}={values[0] if len(values) == 1 else values}"
                    for pos, values in sorted(dims_url.items())
                )
                error_msg = f"Request {i+1} failed for dimensions [{dim_values_str}]: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
                continue

        # Vérification qu'au moins une requête a réussi
        if not all_dataframes:
            error_summary = "\n".join(errors) if errors else "All requests returned empty results"
            raise ValueError(
                f"All {len(request_combinations)} requests failed or returned empty results.\n"
                f"Errors encountered:\n{error_summary}"
            )

        # Logging des résultats
        if errors:
            logger.warning(
                f"{len(errors)} out of {len(request_combinations)} requests failed. "
                f"Successfully retrieved {len(all_dataframes)} DataFrames."
            )

        # Concaténation des DataFrames
        logger.info(f"Concatenating {len(all_dataframes)} DataFrames")
        result = pd.concat(all_dataframes, ignore_index=True)

        logger.info(f"Final result: {len(result)} rows")

        return result

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
    # /!\ Voir si ne pourrait pas être mis en commun dans le cas de plusieurs sources de données (par exemple en utilisant eurostat)
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
    # /!\ Voir si ne pourrait pas être mis en commun dans le cas de plusieurs sources de données (par exemple en utilisant eurostat)
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

    # Méthode de filtrage des requêtes mises à jour
    def filter_updated_queries(
        self,
        queries: List[QueryRequest],
        updated_since: Optional[Union[str, datetime]]=None,
    ) -> List[QueryRequest]:
        """Filter queries to keep only those with data updated since a given date.

        This method queries the OECD ContentConstraint endpoint for each dataflow
        to determine if the data has been updated since the specified date.

        Args:
            queries: List of QueryRequest objects to filter.
            updated_since: Date/datetime threshold. Only queries for dataflows
                          updated after this date will be returned.
                          Can be a string (ISO format), datetime object, or None.
                          If None, all queries are returned without filtering.

        Returns:
            Filtered list of QueryRequest objects for updated dataflows only.
            If updated_since is None, returns all queries unchanged.

        Example:
            >>> # Get all queries without filtering
            >>> all_queries = client.filter_updated_queries(queries, updated_since=None)

            >>> # Filter by specific date
            >>> queries = [
            ...     QueryRequest(agency="OECD.SDD.STES", dataflow="DSD_KEI@DF_KEI"),
            ...     QueryRequest(agency="OECD.ELS.SPD", dataflow="DSD_SOCX_AGG@DF_SOCX_AGG"),
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