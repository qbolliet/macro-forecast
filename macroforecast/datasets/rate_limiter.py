"""Rate limiter for API requests.

This module provides a rate limiter that enforces request rate limits
for API clients, supporting various time units.
"""
# Importation des modules
from typing import Literal, Dict, Any
from datetime import datetime, timedelta
from collections import deque
import time
import threading
import logging

# Initialisation du logger
logger = logging.getLogger(__name__)


# Type pour les unités de temps
TimeUnit = Literal["seconds", "minutes", "hours", "days", "weeks"]


# Classe de gestion du rate limiting
class RateLimiter:
    """Rate limiter for API requests.

    This class implements a sliding window rate limiter that tracks
    request timestamps and enforces a maximum number of requests
    per time period.

    Args:
        max_requests: Maximum number of requests allowed.
        time_unit: Unit of time period ('seconds', 'minutes', 'hours', 'days', 'weeks').
        time_count: Number of time units (e.g., 1 hour, 2 days).

    Example:
        >>> limiter = RateLimiter(max_requests=60, time_unit="hours", time_count=1)
        >>> limiter.acquire()  # Attends si nécessaire avant de continuer
        >>> # Exécuter la requête API ici
    """

    # Mapping des unités vers secondes
    _UNIT_TO_SECONDS = {
        "seconds": 1,
        "minutes": 60,
        "hours": 3600,
        "days": 86400,
        "weeks": 604800,
    }

    # Initialisation
    def __init__(
        self,
        max_requests: int,
        time_unit: TimeUnit,
        time_count: int = 1,
    ):
        """Initialize rate limiter.

        Args:
            max_requests: Maximum number of requests allowed.
            time_unit: Unit of time period.
            time_count: Number of time units.

        Raises:
            ValueError: If parameters are invalid.
        """
        # Validation des paramètres
        if max_requests <= 0:
            raise ValueError("max_requests must be positive")
        if time_count <= 0:
            raise ValueError("time_count must be positive")
        if time_unit not in self._UNIT_TO_SECONDS:
            raise ValueError(
                f"time_unit must be one of {list(self._UNIT_TO_SECONDS.keys())}"
            )

        # Initialisation des attributs
        self.max_requests = max_requests
        self.time_unit = time_unit
        self.time_count = time_count

        # Calcul de la fenêtre temporelle en secondes
        self.window_seconds = self._UNIT_TO_SECONDS[time_unit] * time_count

        # File des timestamps des requêtes
        self._request_times: deque[float] = deque()

        # Lock pour thread-safety
        self._lock = threading.Lock()

        # Logging
        logger.info(
            f"RateLimiter initialized: {max_requests} requests per "
            f"{time_count} {time_unit} ({self.window_seconds}s)"
        )

    # Méthode d'acquisition de données en respectant le quota de requêtes
    def acquire(self) -> None:
        """Wait if necessary, then record the request.

        This method blocks until a request can be made without
        exceeding the rate limit.

        Example:
            >>> limiter = RateLimiter(max_requests=10, time_unit="seconds", time_count=1)
            >>> limiter.acquire()
            >>> # La requête peut maintenant être effectuée
        """
        with self._lock:
            # Nettoyage des anciennes requêtes
            self._clean_old_requests()

            # Calcul du temps d'attente nécessaire
            wait_seconds = self._wait_time()

            # Attente si nécessaire
            if wait_seconds > 0:
                # Logging
                logger.debug(f"Rate limit reached, waiting {wait_seconds:.2f}s")
                # Attente
                time.sleep(wait_seconds)
                # Re-nettoyage après l'attente
                self._clean_old_requests()

            # Enregistrement de la nouvelle requête
            self._request_times.append(time.time())
            # Logging
            logger.debug(
                f"Request recorded ({len(self._request_times)}/{self.max_requests})"
            )

    # Méthode auxiliaire de suppression des requêtes trop anciennes
    def _clean_old_requests(self) -> None:
        """Remove requests older than the time window.

        Deletes all query records, allowing
        you to restart with an empty counter.
        """
        # Extraction du temps présent
        current_time = time.time()
        cutoff_time = current_time - self.window_seconds

        # Suppression des timestamps trop anciens
        while self._request_times and self._request_times[0] < cutoff_time:
            self._request_times.popleft()

    # Méthode de calcul du temps d'attente avant la prochaine requête
    def _wait_time(self) -> float:
        """Calculate how long to wait before next request.

        Returns:
            Number of seconds to wait (0 if no wait needed).
        """
        # Cas où la limite n'est pas atteinte
        if len(self._request_times) < self.max_requests:
            return 0.0

        # Calcul du temps d'attente jusqu'à ce que la requête la plus ancienne sorte de la fenêtre
        oldest_request = self._request_times[0]
        current_time = time.time()
        time_since_oldest = current_time - oldest_request
        wait_time = self.window_seconds - time_since_oldest

        # Retourne le temps d'attente (minimum 0)
        return max(0.0, wait_time)

    # Méthode de création d'une instance du RateLimiter à partir d'un dictionnaire de paramètres
    @staticmethod
    def from_dict(config: Dict[str, Any]) -> "RateLimiter":
        """Create RateLimiter from configuration dictionary.

        Args:
            config: Dictionary with keys 'requests', 'unit', 'count'.

        Returns:
            Configured RateLimiter instance.

        Raises:
            ValueError: If required keys are missing.

        Example:
            >>> config = {"requests": 60, "unit": "hours", "count": 1}
            >>> limiter = RateLimiter.from_dict(config)
        """
        # Validation de la présence des clés
        required_keys = {"requests", "unit"}
        missing_keys = required_keys - set(config.keys())
        if missing_keys:
            raise ValueError(f"Missing required keys: {missing_keys}")

        # Extraction des paramètres
        max_requests = config["requests"]
        time_unit = config["unit"]
        time_count = config.get("count", 1)

        # Création de l'instance
        return RateLimiter(
            max_requests=max_requests,
            time_unit=time_unit,
            time_count=time_count,
        )

    # Méthode de réinitialisation du rate limiter
    def reset(self) -> None:
        """Reset the rate limiter state.

        Deletes all query records, allowing
        you to restart with an empty counter.

        Example:
            >>> limiter = RateLimiter(max_requests=10, time_unit="seconds", time_count=1)
            >>> limiter.acquire()
            >>> limiter.reset()  # Réinitialise le compteur
        """
        with self._lock:
            # Réinitialisation
            self._request_times.clear()
            # Logging
            logger.debug("RateLimiter reset")

    # Méthode d'extraction du nombre de requêtes restantes pouvant être effectuées dans la fenêtre temporelle
    def get_remaining_requests(self) -> int:
        """Get number of remaining requests in current window.

        Returns:
            Number of requests that can be made immediately.

        Example:
            >>> limiter = RateLimiter(max_requests=10, time_unit="seconds", time_count=1)
            >>> remaining = limiter.get_remaining_requests()
        """
        with self._lock:
            # Nettoyage des anciennes requêtes
            self._clean_old_requests()
            return max(0, self.max_requests - len(self._request_times))

    # Représentation sous forme de chaîne de caractères
    def __repr__(self) -> str:
        """String representation of the RateLimiter."""
        return (
            f"RateLimiter(max_requests={self.max_requests}, "
            f"time_unit='{self.time_unit}', time_count={self.time_count})"
        )
