"""Generic API client and abstract SDMX client base.

This module provides:
- ``APIClient``: generic HTTP client with retry logic.
- ``AbstractSDMXClient``: abstract base class mutualising logic shared by all
  SDMX provider clients (structure registry, duplicate checking, split-request
  execution, CSV parsing, context manager, etc.).
"""
# Importation des modules
from abc import ABC, abstractmethod
from io import StringIO
import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin
import warnings

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Imports internes — éviter les imports circulaires en utilisant TYPE_CHECKING
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .rate_limiter import RateLimiter
    from .sdmx import DuplicateHandling
    from .structures import DataflowStructure, DataflowStructureRegistry

# Initialisation du logger
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Client HTTP générique
# ──────────────────────────────────────────────────────────────────────

# Classe permettant d'effectuer des requêtes API avec 'requests'
class APIClient:
    """Generic HTTP client with retry logic and error handling.

    This class handles HTTP requests with automatic retry on failures,
    connection pooling, and timeout management.

    Args:
        base_url: Base URL for API requests.
        timeout: Request timeout in seconds (default: 30).
        max_retries: Maximum number of retry attempts (default: 3).
        backoff_factor: Backoff factor for retries (default: 0.5).
        headers: Additional headers to include in requests.

    Example:
        >>> client = APIClient("https://api.example.com")
        >>> response = client.get("/data", params={"key": "value"})
    """

    # Initialisation
    def __init__(
        self,
        base_url: str,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        headers: Optional[Dict[str, str]] = None,
    ):
        # Initialisation des attributs
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = self._create_session(max_retries, backoff_factor)
        self.default_headers = {}
        if headers:
            self.default_headers.update(headers)

    # Méthode auxiliaire de création de la session 'request'
    def _create_session(self, max_retries: int, backoff_factor: float) -> requests.Session:
        """Create a session with retry configuration.

        Args:
            max_retries: Maximum number of retry attempts.
            backoff_factor: Backoff factor between retries.

        Returns:
            Configured requests Session object.
        """
        # Initialisation d'une session requests
        session = requests.Session()
        # Initialisation d'une stratégie de retry
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        # Ajout de la stratégie à la session
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    # Méthode de requête "GET"
    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        """Make a GET request.

        Args:
            endpoint: API endpoint (relative to base_url).
            params: Query parameters.
            headers: Additional headers for this request.

        Returns:
            Response object.

        Raises:
            requests.exceptions.RequestException: On request failure.
        """
        # Création de l'URL de requête
        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))
        # Création des headers
        request_headers = self.default_headers.copy()
        if headers:
            request_headers.update(headers)
        
        # Logging
        logger.debug(f"GET request to {url} with params: {params}")
        try:
            # Excution de la requête
            response = self.session.get(
                url,
                params=params,
                headers=request_headers,
                timeout=self.timeout,
            )
            # Statut de la requête
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            # Logging
            logger.error(f"HTTP error: {e}")
            logger.error(f"Response content: {e.response.text[:500]}")
            raise
        except requests.exceptions.RequestException as e:
            # Logging
            logger.error(f"Request failed: {e}")
            raise

    # Méthode de fermeture de la session
    def close(self):
        """Close the session and clean up resources."""
        self.session.close()

    # Constructeur d'entrée comme context manager
    def __enter__(self):
        """Context manager entry."""
        return self

    # Constructeur de sortie comme contexte manager
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# ──────────────────────────────────────────────────────────────────────
# Client SDMX abstrait — logique mutualisée entre tous les providers
# ──────────────────────────────────────────────────────────────────────

class AbstractSDMXClient(ABC):
    """Abstract base class for SDMX provider clients.

    Provides the shared logic common to all SDMX provider clients :

    - Structure registry management (``register_structure``, ``_ensure_structure``)
    - Split-request execution loop (``_execute_split_requests``)
    - Post-request DataFrame filtering (``_filter_dataframe_by_dimensions``)
    - Duplicate detection (``_check_duplicates``)
    - CSV response parsing (``_parse_csv_response``)
    - Context manager protocol

    Subclasses must implement:
    - ``get_data``: provider-specific data retrieval entry point.
    - ``close``: release provider-specific resources.
    - ``_load_rate_limiter``: load rate-limit config from provider JSON file.
    - ``_fetch_structure``: fetch a ``DataflowStructure`` from the provider API.
    - ``_execute_single_request``: execute one API request for a given
      dimension combination and return a parsed DataFrame.

    Args:
        structure_registry: Pre-populated registry of dataflow structures.
            A new empty registry is created if ``None``.
        auto_fetch_structure: If ``True``, automatically query the provider
            API to retrieve dimension metadata when a dataflow structure is
            not yet in the registry.
        rate_limiter: Rate limiter instance. If ``None`` and
            ``auto_load_rate_limit`` is ``True``, the provider's JSON
            configuration file is read via ``_load_rate_limiter``.
        auto_load_rate_limit: Whether to attempt loading the rate limiter
            automatically from the provider config file.

    Example:
        >>> class MyClient(AbstractSDMXClient):
        ...     def get_data(self, ...): ...
        ...     def close(self): ...
        ...     def _load_rate_limiter(self): return None
        ...     def _fetch_structure(self, agency, dataflow, **kwargs): ...
        ...     def _execute_single_request(self, dims, **kwargs): ...
    """

    # Initialisation
    def __init__(
        self,
        structure_registry: Optional["DataflowStructureRegistry"] = None,
        auto_fetch_structure: bool = True,
        rate_limiter: Optional["RateLimiter"] = None,
        auto_load_rate_limit: bool = True,
    ):
        # Import local pour éviter la circularité au niveau module
        from .structures import DataflowStructureRegistry as _Registry

        # Initialisation des attributs
        # Registre des structures de dataflows
        self.structure_registry: "DataflowStructureRegistry" = (
            structure_registry if structure_registry is not None else _Registry()
        )
        self.auto_fetch_structure = auto_fetch_structure

        # Chargement automatique du rate limiter si demandé
        if auto_load_rate_limit and rate_limiter is None:
            rate_limiter = self._load_rate_limiter()
        self.rate_limiter: Optional["RateLimiter"] = rate_limiter

    # Méthodes abstraites
    # Méthode abstraite de requête des données
    @abstractmethod
    def get_data(self, *args, **kwargs) -> pd.DataFrame:
        """Retrieve data from the provider API.

        Provider-specific signature. Implementations should handle dimension
        normalisation, structure auto-fetch, rate limiting, split requests,
        and duplicate checking.

        Returns:
            DataFrame with the retrieved data.
        """

    # Méthode abstraite de fermeture de la connexion
    @abstractmethod
    def close(self) -> None:
        """Release provider-specific resources (HTTP sessions, etc.)."""

    # Méthode abstraite de chargement du rate-limiter
    @abstractmethod
    def _load_rate_limiter(self) -> Optional["RateLimiter"]:
        """Load rate limiter from the provider-specific JSON config file.

        Returns:
            ``RateLimiter`` instance, or ``None`` if no configuration found.
        """

    # Méthode abstraire de requête de la structure d'un dataflow
    @abstractmethod
    def _fetch_structure(
        self,
        agency: str,
        dataflow: str,
        **kwargs,
    ) -> "DataflowStructure":
        """Fetch a dataflow structure from the provider API (no cache).

        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            **kwargs: Provider-specific keyword arguments (e.g. ``version``
                for Eurostat).

        Returns:
            Parsed ``DataflowStructure``.

        Raises:
            Exception: On HTTP or parsing errors.
        """

    # Méthode abstraite d'exécution d'une requête
    @abstractmethod
    def _execute_single_request(
        self,
        dims_for_request: Dict,
        **request_kwargs,
    ) -> pd.DataFrame:
        """Execute one API request for a given dimension combination.

        Called by ``_execute_split_requests`` for each entry in the
        Cartesian product of split dimensions.

        Args:
            dims_for_request: Dimension values for this specific request
                (format is provider-specific: ``Dict[str, List[str]]`` for
                Eurostat, ``Dict[int, List[str]]`` for OECD).
            **request_kwargs: Additional provider-specific parameters
                forwarded from ``_execute_split_requests``.

        Returns:
            Parsed DataFrame (before post-filtering).
        """

    # Structure registry
    # Méthode d'enregistrement d'une structure dans le registre
    def register_structure(self, structure: "DataflowStructure") -> None:
        """Register a dataflow structure for dimension name resolution.

        Args:
            structure: ``DataflowStructure`` to store in the registry.
        """
        # Enregistrement de la structure dans le registre
        self.structure_registry.register(structure)
        # Logging
        logger.info(f"Registered structure for {structure.dataflow}")

    # Méthode d'extraction d'une structure d'un registre si elle existe et de téléchargement sinon
    def _ensure_structure(
        self,
        agency: str,
        dataflow: str,
        **kwargs,
    ) -> Optional["DataflowStructure"]:
        """Return the structure for a dataflow, fetching it if necessary.

        Checks the registry first. If not found and ``auto_fetch_structure``
        is ``True``, calls ``_fetch_structure`` and caches the result.

        Args:
            agency: Agency identifier used as the registry key.
            dataflow: Dataflow identifier used as the registry key.
            **kwargs: Forwarded to ``_fetch_structure`` (e.g. ``version``).

        Returns:
            ``DataflowStructure`` if available, ``None`` otherwise.
        """
        # Vérification du cache
        if self.structure_registry.has(agency, dataflow):
            return self.structure_registry.get(agency, dataflow)

        # Récupération automatique si activée
        if self.auto_fetch_structure:
            try:
                # Logging
                logger.info(f"Fetching structure for {agency}::{dataflow}")
                # Requête de la structure
                structure = self._fetch_structure(agency, dataflow, **kwargs)
                # Enregistrement de la structure dans le registre
                self.structure_registry.register(structure)
                return structure
            except Exception as e:
                # Logging
                logger.warning(
                    f"Failed to fetch structure for {agency}::{dataflow}: {e}"
                )

        return None

    #  Méthode auxiliaire d'exécution de requêtes multiples
    def _execute_split_requests(
        self,
        request_combinations: List[Tuple[Dict, Dict]],
        **request_kwargs,
    ) -> pd.DataFrame:
        """Execute multiple API requests and concatenate the results.

        Iterates over the Cartesian product of split dimensions. For each
        combination, acquires the rate limiter, delegates the HTTP call to
        ``_execute_single_request``, applies post-request dimension filtering,
        and collects the resulting DataFrames.

        Args:
            request_combinations: List of ``(dims_for_request,
                dims_for_postfilter)`` tuples produced by
                ``_generate_request_combinations``.
                - ``dims_for_request``: passed verbatim to
                  ``_execute_single_request``.
                - ``dims_for_postfilter``: ``Dict[str, List[str]]``
                  (dimension name → allowed values) applied after retrieval.
            **request_kwargs: Additional keyword arguments forwarded to
                ``_execute_single_request`` for every sub-request.

        Returns:
            Concatenated DataFrame from all successful sub-requests.

        Raises:
            ValueError: If every sub-request failed or returned an empty
                DataFrame.
        """
        # Initialisation de la liste des jeux de données requêtés
        all_dataframes: List[pd.DataFrame] = []
        # Initialisation de la liste des erreurs
        errors: List[str] = []
        # Calcul du nombre de combinaisons
        n = len(request_combinations)

        # Logging
        logger.info(f"Executing {n} split API requests")

        # Parcours des requêtes
        for i, (dims_for_request, dims_for_postfilter) in enumerate(request_combinations):
            # Application du rate limiter avant chaque sous-requête
            if self.rate_limiter:
                self.rate_limiter.acquire()

            # Logging de progression tous les 10 requêtes
            if (i + 1) % 10 == 0 or i == 0 or i == n - 1:
                logger.info(f"Processing request {i + 1}/{n}")

            try:
                # Exécution de la requête
                df = self._execute_single_request(dims_for_request, **request_kwargs)

                # Post-filtrage par dimensions si nécessaire
                if not df.empty and dims_for_postfilter:
                    df = self._filter_dataframe_by_dimensions(df, dims_for_postfilter)

                # Ajout du DataFrame à la liste des jeux de données requêtés si non vide
                if not df.empty:
                    all_dataframes.append(df)
                else:
                    # Logging
                    logger.debug(f"Request {i + 1} returned empty after filtering")

            except Exception as e:
                # Construction du message d'erreur
                error_msg = f"Request {i + 1}/{n} failed: {e}"
                # Logging
                logger.error(error_msg)
                # Ajout de l'erreur à la liste
                errors.append(error_msg)

        # Vérification qu'au moins une requête a réussi
        if not all_dataframes:
            # Construction du message d'erreur
            error_summary = (
                "\n".join(errors) if errors else "All requests returned empty results"
            )
            raise ValueError(
                f"All {n} split requests failed or returned empty results.\n"
                f"Errors:\n{error_summary}"
            )

        # Logging si les requêtes ont partiellement échoué
        if errors:
            # Logging
            logger.warning(
                f"{len(errors)} out of {n} requests failed. "
                f"Successfully retrieved {len(all_dataframes)} DataFrames."
            )

        # Logging
        logger.info(f"Concatenating {len(all_dataframes)} DataFrames")
        # Concaténation des jeux de données
        result = pd.concat(all_dataframes, ignore_index=True)
        # Logging
        logger.info(f"Split requests result: {len(result)} rows")

        return result

    # Méthode de filtrage post-requête
    @staticmethod
    def _filter_dataframe_by_dimensions(
        df: pd.DataFrame,
        dimension_filters: Dict[str, List[str]],
    ) -> pd.DataFrame:
        """Filter a DataFrame to retain only allowed dimension values.

        Applied after retrieval when wildcard dimensions would return more
        data than requested (e.g. OECD ``*`` wildcard, or multi-value
        non-split dimensions).

        Args:
            df: DataFrame to filter.
            dimension_filters: Mapping of dimension column name to allowed
                values (``{dim_name: [value, ...]})``).

        Returns:
            Filtered DataFrame (same columns, subset of rows).
        """
        # Vérification que le jeu de données est non vide et que des dimensions de filtre sont fournies
        if df.empty or not dimension_filters:
            return df

        # Masque cumulatif : toutes les conditions doivent être vraies
        mask = pd.Series([True] * len(df), index=df.index)

        # Parcours des filtres de dimension
        for dim_name, allowed_values in dimension_filters.items():
            # Vérification que la dimension de filtre est comprise dans le jeu de données
            if dim_name not in df.columns:
                # Logging
                logger.warning(
                    f"Dimension column '{dim_name}' not found in DataFrame. "
                    f"Available columns: {list(df.columns)}. Skipping filter."
                )
                continue
            # Mise à jour du masque
            mask &= df[dim_name].isin(allowed_values)

        # Filtre du jeu de données
        filtered_df = df[mask]

        # Logging des lignes filtrées
        if len(filtered_df) < len(df):
            logger.info(
                f"Filtered {len(df) - len(filtered_df)} rows by dimensions "
                f"{list(dimension_filters.keys())}"
            )

        return filtered_df

    # Méthode auxiliaire de détection des doublons
    @staticmethod
    def _check_duplicates(
        df: pd.DataFrame,
        dimensions: Union[Dict[int, Any], Dict[str, Any]],
        structure: Optional["DataflowStructure"],
        on_duplicate: "DuplicateHandling",
    ) -> None:
        """Detect and handle duplicate rows in the result DataFrame.

        Identifies the relevant key columns (filtered dimension columns plus
        ``TIME_PERIOD``) and checks for duplicate combinations. Duplicate
        rows usually indicate that wildcard dimensions returned unexpected
        extra dimension values that were not post-filtered.

        Args:
            df: DataFrame to check.
            dimensions: Dimension filter dict used in the query. Keys may be
                integer positions (OECD) or string names (Eurostat); the
                method handles both automatically.
            structure: Dataflow structure used to resolve int positions to
                column names. May be ``None`` if unavailable.
            on_duplicate: Strategy — ``"ignore"`` (no check), ``"warn"``
                (log a warning), or ``"raise"`` (raise ``ValueError``).

        Raises:
            ValueError: If ``on_duplicate="raise"`` and duplicates are found.
        """
        # Vérification que le jeu de données est non vide
        if df.empty:
            return

        # Détermination des colonnes de vérification
        check_columns: List[str] = []

        # Parcours des dimensions de filtre
        for key in dimensions.keys():
            if isinstance(key, int):
                # Clé positionnelle (OECD) → résolution en nom via structure
                if structure:
                    dim_name = structure.get_name(key)
                    if dim_name and dim_name in df.columns:
                        check_columns.append(dim_name)
            else:
                # Clé nominale (Eurostat)
                if key in df.columns:
                    check_columns.append(str(key))

        # Fallback : toutes les colonnes sauf la valeur observée
        if not check_columns:
            check_columns = [
                col for col in df.columns
                if col.lower() not in ("value", "obs_value", "obsvalue")
            ]

        # Détection des duplicats
        duplicates = df.duplicated(subset=check_columns, keep=False)
        # Comptage des duplicats
        num_duplicates = int(duplicates.sum())

        # Affichage des duplicats
        if num_duplicates > 0:
            # Extraction des 10 premières lignes
            dup_df = df[duplicates].sort_values(check_columns).head(10)
            # Construction du message
            message = (
                f"Found {num_duplicates} duplicate rows for columns "
                f"{check_columns}. This may indicate that undesired values "
                f"are included via wildcards (*). Examples:\n{dup_df.to_string()}"
            )
            # Affichage d'un message d'erreur
            if on_duplicate == "raise":
                raise ValueError(message)
            else:
                # Warning
                warnings.warn(message, UserWarning)
                # Logging
                logger.warning(message)

    # Méthode de parsing du CSV de réponse
    @staticmethod
    def _parse_csv_response(text: str) -> pd.DataFrame:
        """Parse a CSV-formatted API response into a DataFrame.

        Args:
            text: Raw CSV text from the API response.

        Returns:
            DataFrame with the parsed data.

        Raises:
            ValueError: If the CSV cannot be parsed.
        """
        try:
            # Lecture du CSV
            df = pd.read_csv(StringIO(text))
            # Logging
            logger.info(f"Parsed {len(df)} rows from CSV")
            return df
        except Exception as e:
            # Logging
            logger.error(f"Failed to parse CSV response: {e}")
            # Erreur
            raise ValueError(f"Failed to parse CSV response: {e}") from e

    # Constructeur d'entrée dans le context manager
    def __enter__(self) -> "AbstractSDMXClient":
        """Context manager entry."""
        return self

    # Constructeur de sortie du contexte manager
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
