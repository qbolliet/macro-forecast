"""Generic API client with retry and error handling.

This module provides a base HTTP client for making API requests with
automatic retry logic and comprehensive error handling.
"""
# Importation des modules
# Modules de base
import logging
from typing import Dict, Optional, Any
from urllib.parse import urljoin
# Modules de requête API
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Initialisation du logger
logger = logging.getLogger(__name__)


# Classe permettant d'effectuer des requêtes API avec 'requests'
class APIClient:
    """Generic HTTP client with retry logic and error handling.
    
    This class handles HTTP requests with automatic retry on failures,
    connection pooling, and timeout management.
    
    Args:
        base_url: Base URL for API requests
        timeout: Request timeout in seconds (default: 30)
        max_retries: Maximum number of retry attempts (default: 3)
        backoff_factor: Backoff factor for retries (default: 0.5)
        headers: Additional headers to include in requests
    
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
        # URL de la requête
        self.base_url = base_url.rstrip("/")
        # Timout de la requête
        self.timeout = timeout
        # Session de requête
        self.session = self._create_session(max_retries, backoff_factor)
        
        # Configuration des headers par défaut
        self.default_headers = {}

        # Ajout des headers si spécifiés
        if headers:
            self.default_headers.update(headers)
    
    # Méthode auxiliaire de création de la session 'request'
    def _create_session(self, max_retries: int, backoff_factor: float) -> requests.Session:
        """Create a session with retry configuration.
        
        Args:
            max_retries: Maximum number of retry attempts
            backoff_factor: Backoff factor between retries
            
        Returns:
            Configured requests Session object
        """
        session = requests.Session()
        
        # Configuration de la stratégie de retry
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        """Make a GET request.
        
        Args:
            endpoint: API endpoint (relative to base_url)
            params: Query parameters
            headers: Additional headers for this request
            
        Returns:
            Response object
            
        Raises:
            requests.exceptions.RequestException: On request failure
        """
        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))
        
        # Fusion des headers
        request_headers = self.default_headers.copy()
        if headers:
            request_headers.update(headers)
        
        logger.debug(f"GET request to {url} with params: {params}")
        
        try:
            response = self.session.get(
                url,
                params=params,
                headers=request_headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            logger.error(f"Response content: {e.response.text[:500]}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def close(self):
        """Close the session and clean up resources."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()