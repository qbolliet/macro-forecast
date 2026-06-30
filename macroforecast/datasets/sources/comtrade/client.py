"""UN Comtrade data client.

High-level client for querying UN Comtrade international-trade data and
converting responses to pandas DataFrames. Unlike Eurostat and OECD, UN
Comtrade does not follow the SDMX conventions, so this client wraps the
official ``comtradeapicall`` library rather than building SDMX endpoints.

It inherits from :class:`~macroforecast.datasets.core.client.APIClient` for the
shared HTTP plumbing (retry session, ``close``) and mirrors the SDMX clients'
shape: configuration (rate limiter, structures) is loaded from
``parameters/comtrade.json``, and the automated bulk download lives in a
dedicated script (``scripts/download_comtrade.py``) rather than in the client.

The methodology is available at:
https://comtradeapi.un.org/files/v1/app/wiki/MethodologyGuideforComtradePlus.pdf
"""
# Importation des modules
# Modules de base
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional, TYPE_CHECKING, Union

# Module de l'API UN Comtrade
import comtradeapicall
import numpy as np
import pandas as pd

# Modules du package
from ...core.client import APIClient
from ...core.rate_limiter import CompositeRateLimiter, RateLimiter, build_rate_limiter
from ...core.structures import DataflowStructure, DataflowStructureRegistry
from . import parsing
from .formats import (
    AGENCY_ID,
    EXPORT_FLOW_CODES,
    EXPORT_TO_IMPORT_CODE,
    EXPORT_TO_IMPORT_DESC,
    IMPORT_FLOW_CODES,
    IMPORT_TO_EXPORT_CODE,
    IMPORT_TO_EXPORT_DESC,
    VALID_FREQUENCIES,
)

if TYPE_CHECKING:
    from .queries import ComtradeQueryRequest

# Initialisation du logger
logger = logging.getLogger(__name__)


# Classe de récupération des données de commerce international du UN Comtrade
class ComtradeClient(APIClient):
    """High-level client for UN Comtrade international-trade data.

    Fetches, subdivides (to respect the per-call record limit) and aggregates
    tariffline trade data from UN Comtrade, exposing an API homogeneous with the
    SDMX clients (:class:`EurostatClient`, :class:`OECDClient`).

    Args:
        base_url: UN Comtrade API base URL (used for the inherited HTTP
            session; ``comtradeapicall`` builds its own request URLs).
        timeout: Request timeout in seconds.
        subscription_key: Comtrade subscription key. Falls back to the
            ``COMTRADE_SUBSCRIPTION_KEY`` environment variable when ``None``.
        structure_registry: Optional registry for dataflow structures. When
            ``None`` a new registry is created and populated from the
            ``STRUCTURES`` section of ``parameters/comtrade.json``.
        rate_limiter: Optional rate limiter. When ``None`` and
            ``auto_load_rate_limit`` is ``True``, it is built from the
            ``RATE_LIMIT`` section of ``parameters/comtrade.json`` (a list of
            limits → :class:`CompositeRateLimiter` enforcing 1 req/s and
            500 req/day).
        auto_load_rate_limit: Whether to load the rate limiter automatically.
        max_retries: Maximum number of HTTP retry attempts (inherited).
        backoff_factor: Backoff factor between retries (inherited).

    Examples:
        >>> client = ComtradeClient()
        >>> # Métadonnées d'une catégorie de référence
        >>> reporters = client.get_metadata(category="reporter")  # doctest: +SKIP
        >>> # Données tariffline
        >>> df, meta = client.get_data(
        ...     reporters="FRA", products=["010121"],
        ...     periods="2023", frequency="annual",
        ... )  # doctest: +SKIP
    """

    # Nom du fichier de configuration (parité avec PROVIDER_CONFIG_NAME des clients SDMX)
    PROVIDER_CONFIG_NAME = "comtrade"

    # URL de base par défaut de l'API UN Comtrade
    DEFAULT_BASE_URL = "https://comtradeapi.un.org"

    # Initialisation
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = 120,
        subscription_key: Optional[str] = None,
        structure_registry: Optional[DataflowStructureRegistry] = None,
        rate_limiter: Optional[Union[RateLimiter, CompositeRateLimiter]] = None,
        auto_load_rate_limit: bool = True,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ) -> None:
        # Initialisation de la couche HTTP partagée (session, retry, close)
        super().__init__(
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )

        # Chargement des paramètres consolidés (rate limit, limites, structures)
        self._parameters = self._load_parameters()

        # Clé de souscription (argument ou variable d'environnement)
        self.subscription_key = (
            subscription_key
            if subscription_key is not None
            else os.getenv("COMTRADE_SUBSCRIPTION_KEY")
        )

        # Rate limiter (argument ou chargement automatique depuis la configuration)
        if auto_load_rate_limit and rate_limiter is None:
            rate_limiter = self._load_rate_limiter()
        self.rate_limiter: Optional[Union[RateLimiter, CompositeRateLimiter]] = (
            rate_limiter
        )

        # Registre des structures (argument ou construction + chargement des paramètres)
        if structure_registry is not None:
            self.structure_registry = structure_registry
        else:
            self.structure_registry = DataflowStructureRegistry()
            # Chargement des structures déclarées (pas d'endpoint de structure côté API)
            self.structure_registry.load_from_dict(self._parameters)

        # Compteur d'appels API (à des fins de logging uniquement ; le quota est
        # garanti par le rate limiter)
        self.api_calls = 0

    # ──────────────────────────────────────────────────────────────────
    # Chargement de la configuration
    # ──────────────────────────────────────────────────────────────────

    # Méthode de chargement du fichier de paramètres consolidé
    def _load_parameters(self) -> dict:
        """Load ``parameters/comtrade.json`` (rate limit, limits, structures).

        Returns:
            Parsed configuration dictionary (empty dict when the file is
            missing or unreadable).
        """
        # Construction du chemin vers parameters/comtrade.json (racine du repo)
        params_path = (
            Path(__file__).parents[4]
            / "parameters"
            / f"{self.PROVIDER_CONFIG_NAME}.json"
        )
        try:
            # Lecture et parsing du fichier de configuration
            with open(params_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            # Échec non bloquant : configuration vide par défaut
            logger.warning(f"Could not load {params_path}: {e}")
            return {}

    # Méthode de chargement du rate limiter depuis la configuration
    def _load_rate_limiter(
        self,
    ) -> Optional[Union[RateLimiter, CompositeRateLimiter]]:
        """Build the rate limiter from the ``RATE_LIMIT`` configuration section.

        The section is a list of ``{requests, unit, count}`` limits, so a
        :class:`CompositeRateLimiter` enforcing every limit simultaneously is
        returned (cf. :func:`build_rate_limiter`).

        Returns:
            A rate limiter, or ``None`` when no configuration is found.
        """
        # Extraction de la section RATE_LIMIT
        config = self._parameters.get("RATE_LIMIT")
        if config is None:
            logger.debug("No RATE_LIMIT configuration found")
            return None
        try:
            logger.info(
                f"Loading rate limiter from "
                f"parameters/{self.PROVIDER_CONFIG_NAME}.json"
            )
            return build_rate_limiter(config)
        except Exception as e:
            # Échec non bloquant de chargement
            logger.warning(f"Could not load rate limiter: {e}")
            return None

    # Propriété d'URL de proxy formatée pour comtradeapicall
    @property
    def _proxy_url(self) -> Optional[str]:
        """Proxy URL passed to ``comtradeapicall`` (``None`` when unset)."""
        # Aucun proxy → None ; sinon préfixe http://
        if self.proxy is None:
            return None
        return f"http://{self.proxy}"

    # Méthode auxiliaire d'application du rate limiter avant un appel API
    def _acquire(self) -> None:
        """Block until the rate limiter allows the next API call (if any)."""
        # Application du rate limiter si configuré
        if self.rate_limiter is not None:
            self.rate_limiter.acquire()

    # ──────────────────────────────────────────────────────────────────
    # Méthodes auxiliaires de preprocessing
    # ──────────────────────────────────────────────────────────────────

    # Méthode auxiliaire de preprocessing des codes
    def _preprocess_codes(
        self, codes: Union[List[int], List[str], int, str, None]
    ) -> Optional[str]:
        """Preprocess country or product codes into the Comtrade CSV format.

        Args:
            codes: Single code or list of codes. Lists are joined with commas,
                integers are stringified, strings and ``None`` are returned
                unchanged.

        Returns:
            Comma-separated string of codes, or ``None``.

        Examples:
            >>> ComtradeClient._preprocess_codes(None, [1, 2, 3])
            '1,2,3'
            >>> ComtradeClient._preprocess_codes(None, "FRA")
            'FRA'
        """
        # Liste de codes → concaténation par des virgules
        if isinstance(codes, list):
            codes = ",".join([str(e) for e in codes])
        # Entier → conversion en chaîne
        elif isinstance(codes, int):
            codes = str(codes)
        # Chaîne ou None → renvoyé tel quel
        return codes

    # Méthode auxiliaire de validation d'une subdivision
    def _validate_subdivision(self, subdivision: Union[str, None]) -> bool:
        """Validate whether a parameter can be split into smaller requests.

        Args:
            subdivision: Comma-separated string with more than one value, or
                ``None``.

        Returns:
            ``True`` if ``None`` or containing multiple values, ``False``
            otherwise.

        Examples:
            >>> ComtradeClient._validate_subdivision(None, "USA,CAN,MEX")
            True
            >>> ComtradeClient._validate_subdivision(None, "USA")
            False
        """
        # None ou liste de plus d'un élément → divisible
        if subdivision is None:
            return True
        elif isinstance(subdivision, str):
            # Identification des différents items de la subdivision
            list_items = subdivision.split(",")
            return len(list_items) > 1
        else:
            return False

    # Méthode auxiliaire d'extraction des codes valides d'une catégorie
    def _extract_codes(self, category: str) -> list:
        """Extract valid codes for a reference category.

        Args:
            category: One of ``"flow"``, ``"reporter"``, ``"partner"`` or
                ``"cmd:HS"``.

        Returns:
            List of valid codes for the category.

        Raises:
            ValueError: If ``category`` is not supported.
        """
        # Extraction des métadonnées puis des codes (logique pure déléguée à parsing)
        df = self.get_metadata(category=category)
        return parsing.extract_codes(df, category)

    # ──────────────────────────────────────────────────────────────────
    # Récupération des données tariffline
    # ──────────────────────────────────────────────────────────────────

    # Méthode auxiliaire de subdivision récursive d'une requête
    def _divide_request(
        self,
        subdivision: str,
        flows: Optional[Union[List[str], str, None]] = ["M", "X"],
        products: Optional[Union[List[int], List[str], int, str, None]] = None,
        reporters: Optional[Union[List[int], List[str], int, str, None]] = None,
        partners: Optional[Union[List[int], List[str], int, str, None]] = None,
        partners2: Optional[Union[List[int], List[str], int, str, None]] = None,
        periods: Optional[Union[List[str], str, None]] = None,
        frequency: Optional[str] = None,
    ) -> pd.DataFrame:
        """Divide a request that exceeds the per-call record limit.

        Splits the requested values of ``subdivision`` into two halves and
        re-issues two recursive :meth:`get_data` calls, concatenating the
        results.

        Args:
            subdivision: Dimension to split (``'flows'``, ``'products'``,
                ``'reporters'``, ``'partners'``, ``'partners2'`` or
                ``'periods'``).
            flows: Trade flow codes.
            products: Product codes.
            reporters: Reporter codes.
            partners: Partner codes.
            partners2: Secondary partner codes.
            periods: Time periods.
            frequency: Data frequency (``'monthly'`` or ``'annual'``).

        Returns:
            Tuple ``(DataFrame, request_metadata)`` combining both halves.

        Raises:
            ValueError: If ``subdivision`` is invalid or cannot be divided
                further.
        """
        # Vérification de la validité de la subdivision
        if subdivision not in [
            "flows",
            "products",
            "reporters",
            "partners",
            "partners2",
            "periods",
        ]:
            raise ValueError(
                f"Invalid subdivision : {subdivision}. Should be in ['flows', "
                "'products', 'reporters', 'partners', 'partners2', 'periods']"
            )

        # Extraction des items sur lesquels effectuer la subdivision
        items = locals()[subdivision]

        # Validation de la subdivision
        if self._validate_subdivision(subdivision=items):
            # Si None, requête des valeurs valides (sauf pour les périodes)
            if (items is None) & (subdivision == "periods"):
                raise ValueError("Unable to request the valid values for 'periods'")
            elif items is None:
                # Requête des options valides
                list_items = self._extract_codes(
                    category=self._parameters["SUBDIVISION_METADATA"][subdivision]
                )
            else:
                list_items = items.split(",")
            # Construction des deux sous-listes
            list_items1, list_items2 = (
                list_items[: (len(list_items) // 2)],
                list_items[(len(list_items) // 2):],
            )
            # Requête récursive sur la première sous-liste
            df1, request_metadata1 = self.get_data(
                flows=list_items1 if subdivision == "flows" else flows,
                products=list_items1 if subdivision == "products" else products,
                reporters=list_items1 if subdivision == "reporters" else reporters,
                partners=list_items1 if subdivision == "partners" else partners,
                partners2=list_items1 if subdivision == "partners2" else partners2,
                periods=list_items1 if subdivision == "periods" else periods,
                frequency=frequency,
            )
            # Requête récursive sur la seconde sous-liste
            df2, request_metadata2 = self.get_data(
                flows=list_items2 if subdivision == "flows" else flows,
                products=list_items2 if subdivision == "products" else products,
                reporters=list_items2 if subdivision == "reporters" else reporters,
                partners=list_items2 if subdivision == "partners" else partners,
                partners2=list_items2 if subdivision == "partners2" else partners2,
                periods=list_items2 if subdivision == "periods" else periods,
                frequency=frequency,
            )
            # Concaténation des jeux de données
            df = pd.concat([df1, df2], axis=0, ignore_index=True)
            # Concaténation des métadonnées (mêmes clés dans les deux dictionnaires)
            request_metadata = {
                k: f"{request_metadata1[k]},{request_metadata2[k]}"
                for k in request_metadata1.keys()
            }
            return df, request_metadata
        else:
            raise ValueError(
                "Unable to further truncate the request with parameters : "
                f"'flows' : {flows}, 'products' : {products}, "
                f"'reporters' : {reporters}, 'partners' : {partners}, "
                f"'partners2' : {partners2}, 'periods' : {periods}"
            )

    # Méthode principale de récupération des données tariffline
    def get_data(
        self,
        flows: Optional[Union[List[str], str, None]] = ["M", "X"],
        products: Optional[Union[List[int], List[str], int, str, None]] = None,
        reporters: Optional[Union[List[int], List[str], int, str, None]] = None,
        partners: Optional[Union[List[int], List[str], int, str, None]] = None,
        partners2: Optional[Union[List[int], List[str], int, str, None]] = None,
        periods: Optional[Union[List[str], str, None]] = None,
        period_start: Optional[Union[str, None]] = None,
        period_end: Optional[Union[str, None]] = None,
        frequency: Optional[str] = "monthly",
    ) -> pd.DataFrame:
        """Fetch tariffline data from UN Comtrade.

        Issues a single ``comtradeapicall`` request and, when the response hits
        the per-call record limit (``LIMIT``), recursively subdivides it (by
        flow, product, reporter, partner, partner2 or period) until each chunk
        fits.

        Args:
            flows: Trade flow codes (e.g. ``["M", "X"]``).
            products: Product codes.
            reporters: Reporter country codes.
            partners: Partner country codes.
            partners2: Secondary partner codes.
            periods: Explicit periods (``YYYY`` or ``YYYYMM``).
            period_start: Start period (used when ``periods`` is omitted).
            period_end: End period (used when ``periods`` is omitted).
            frequency: Data frequency (``'monthly'`` or ``'annual'``).

        Returns:
            Tuple ``(DataFrame, request_metadata)`` where ``request_metadata``
            records the resolved request parameters.

        Raises:
            ValueError: If ``frequency`` is invalid.

        Examples:
            >>> df, meta = client.get_data(
            ...     reporters="FRA", periods="2023", frequency="annual",
            ... )  # doctest: +SKIP
        """
        # Vérification de la cohérence des paramètres
        if frequency not in VALID_FREQUENCIES:
            raise ValueError(
                f"Invalid value for frequency : {frequency}. "
                f"Should be in {VALID_FREQUENCIES}"
            )

        # Mémorisation du nombre d'appels initial (pour le logging)
        initial_api_calls = self.api_calls

        # Preprocessing des périodes
        if isinstance(periods, list):
            periods = ",".join(periods)
        elif (period_start is not None) & (period_end is not None):
            periods = ",".join(
                pd.date_range(
                    start=period_start,
                    end=period_end,
                    freq="YS" if frequency == "annual" else "MS",
                )
                .strftime("%Y" if frequency == "annual" else "%Y%m")
                .tolist()
            )
        elif period_start is not None:
            periods = ",".join(
                pd.date_range(
                    start=period_start,
                    end=datetime.today(),
                    freq="YS" if frequency == "annual" else "MS",
                )
                .strftime("%Y" if frequency == "annual" else "%Y%m")
                .tolist()
            )

        # Preprocessing des flux et des codes (pays, produits)
        flows = self._preprocess_codes(codes=flows)
        reporters = self._preprocess_codes(codes=reporters)
        partners = self._preprocess_codes(codes=partners)
        partners2 = self._preprocess_codes(codes=partners2)
        products = self._preprocess_codes(codes=products)

        # Application du rate limiter avant l'appel API
        self._acquire()

        # Requête des données tariffline via la lib officielle
        df = comtradeapicall._getTarifflineData(
            self.subscription_key,
            typeCode="C",  # Type de commerce : 'C' (commodities) ou 'S' (services)
            freqCode="A" if frequency == "annual" else "M",
            clCode="HS",  # Nomenclature : 'HS', 'SITC', 'BEC' ou 'EBOPS'
            period=periods,
            reporterCode=reporters,
            cmdCode=products,
            flowCode=flows,
            partnerCode=partners,
            partner2Code=partners2,
            customsCode=None,
            motCode=None,
            maxRecords=None,
            format_output="JSON",
            countOnly=None,
            includeDesc=True,  # Inclusion des descriptions des variables
            proxy_url=self._proxy_url,
        )
        # Incrément du compteur d'appels API
        self.api_calls += 1

        # Si la limite du nombre d'observations est atteinte, subdivision de la requête
        if len(df) >= self._parameters["LIMIT"]:
            # Test des subdivisions valides, dans l'ordre de préférence
            if self._validate_subdivision(subdivision=flows):
                df, request_metadata = self._divide_request(
                    subdivision="flows",
                    flows=flows, products=products, reporters=reporters,
                    partners=partners, partners2=partners2, periods=periods,
                    frequency=frequency,
                )
            elif self._validate_subdivision(subdivision=products):
                df, request_metadata = self._divide_request(
                    subdivision="products",
                    flows=flows, products=products, reporters=reporters,
                    partners=partners, partners2=partners2, periods=periods,
                    frequency=frequency,
                )
            elif self._validate_subdivision(subdivision=reporters):
                df, request_metadata = self._divide_request(
                    subdivision="reporters",
                    flows=flows, products=products, reporters=reporters,
                    partners=partners, partners2=partners2, periods=periods,
                    frequency=frequency,
                )
            elif self._validate_subdivision(subdivision=partners):
                df, request_metadata = self._divide_request(
                    subdivision="partners",
                    flows=flows, products=products, reporters=reporters,
                    partners=partners, partners2=partners2, periods=periods,
                    frequency=frequency,
                )
            elif self._validate_subdivision(subdivision=partners2):
                df, request_metadata = self._divide_request(
                    subdivision="partners2",
                    flows=flows, products=products, reporters=reporters,
                    partners=partners, partners2=partners2, periods=periods,
                    frequency=frequency,
                )
            elif self._validate_subdivision(subdivision=periods):
                df, request_metadata = self._divide_request(
                    subdivision="periods",
                    flows=flows, products=products, reporters=reporters,
                    partners=partners, partners2=partners2, periods=periods,
                    frequency=frequency,
                )
            else:
                # Subdivision impossible : la requête est conservée telle quelle
                logger.warning(
                    "Unable to further truncate the request with parameters : "
                    f"'flows' : {flows}, 'products' : {products}, "
                    f"'reporters' : {reporters}, 'partners' : {partners}, "
                    f"'partners2' : {partners2}, 'periods' : {periods}"
                )
                request_metadata = self._build_request_metadata(
                    flows, products, reporters, partners, partners2, periods, frequency
                )
        else:
            # Pas de subdivision : construction des métadonnées de la requête
            request_metadata = self._build_request_metadata(
                flows, products, reporters, partners, partners2, periods, frequency
            )

        # Logging du nombre d'appels nécessaires pour finaliser la requête
        logger.info(
            f"{self.api_calls - initial_api_calls} api calls needed to fetch "
            f"tarifline data with parameters : 'flows' : {flows}, "
            f"'products' : {products}, 'reporters' : {reporters}, "
            f"'partners' : {partners}, 'partners2' : {partners2}, "
            f"'periods' : {periods}"
        )

        return df, request_metadata

    # Méthode auxiliaire de construction des métadonnées d'une requête
    @staticmethod
    def _build_request_metadata(
        flows, products, reporters, partners, partners2, periods, frequency
    ) -> dict:
        """Build the metadata dictionary describing a resolved request.

        Returns:
            Dictionary of stringified request parameters.
        """
        # Sérialisation des paramètres de la requête
        return {
            "flows": str(flows),
            "products": str(products),
            "reporters": str(reporters),
            "partners": str(partners),
            "partners2": str(partners2),
            "periods": str(periods),
            "frequency": str(frequency),
        }

    # ──────────────────────────────────────────────────────────────────
    # Métadonnées et périodes
    # ──────────────────────────────────────────────────────────────────

    # Méthode de chargement des métadonnées d'une catégorie de référence
    def get_metadata(self, category: Optional[Union[str, None]] = None) -> pd.DataFrame:
        """Fetch metadata for a reference category from UN Comtrade.

        Args:
            category: Reference category. When ``None``, returns the registry
                of available categories.

        Returns:
            DataFrame with the metadata for the requested category.

        Raises:
            ValueError: If ``category`` is invalid or the data cannot be
                retrieved.

        Examples:
            >>> categories = client.get_metadata()  # doctest: +SKIP
            >>> reporters = client.get_metadata(category="reporter")  # doctest: +SKIP
        """
        # Application du rate limiter avant l'appel API
        self._acquire()

        # Chargement du registre des références
        metadata_index = comtradeapicall.listReference(
            category=category,
            proxy_url=self._proxy_url,
        )

        # Jeu de données vide → erreur avec les modalités valides
        if metadata_index.empty:
            # Requête de l'ensemble des possibilités
            metadata_options = comtradeapicall.listReference(
                category=None,
                proxy_url=self._proxy_url,
            )
            raise ValueError(
                f"Invalid 'category' : {category}. To get further information, "
                f"run with category=None. 'category' should be in "
                f"{metadata_options['category'].tolist()}."
            )

        # Catégorie non spécifiée → registre des méta-données
        if category is None:
            return metadata_index

        # Requête du fichier de la catégorie (session HTTP héritée d'APIClient)
        response = self.session.get(metadata_index["fileuri"].iloc[0])

        # Disjonction de cas suivant le statut de la requête
        if response.status_code == 200:
            # Extraction et normalisation des données
            data = response.json()
            return pd.json_normalize(data["results"])
        else:
            raise ValueError(
                f"Failed to retrieve data for category : {category}. "
                f"Status code: {response.status_code}"
            )

    # Méthode auxiliaire de validation du format d'une période
    def _validate_date(self, period: Union[str, int]) -> str:
        """Validate and format a date period string.

        Args:
            period: Period to validate (``YYYY``, ``YYYY-MM`` or
                ``YYYY-MM-DD``).

        Returns:
            Validated date string (``YYYY-MM-DD``).

        Raises:
            ValueError: If the period format is invalid.

        Examples:
            >>> ComtradeClient._validate_date(None, "2023")
            '2023-01-01'
            >>> ComtradeClient._validate_date(None, "2023-06")
            '2023-06-01'
        """
        # Conversion en chaîne et remplacement des caractères non numériques par '-'
        period = str(period)
        period = re.sub(r"\D", "-", period.strip())

        # La période doit avoir au minimum quatre chiffres (année)
        if len(period) < 4:
            raise ValueError("Period must be at least 4 digits long")

        # Complétion de la chaîne jusqu'à une longueur de 10 (YYYY-MM-DD)
        if len(period) == 4:
            period += "-01-01"
        elif len(period) == 7:
            period += "-01"
        elif len(period) != 10:
            raise ValueError(
                "Period must be in the format YYYY or YYYY-MM or YYYY-MM-DD"
            )
        return period

    # Méthode de construction des périodes valides
    def get_valid_periods(
        self,
        periods: Optional[Union[List[str], str, None]] = None,
        period_start: Optional[Union[str, None]] = None,
        period_end: Optional[Union[str, None]] = None,
        frequency: Optional[str] = "monthly",
    ) -> List[str]:
        """Generate the list of valid periods for trade-data requests.

        Args:
            periods: Explicit periods to intersect with the valid range.
            period_start: Start period (``YYYY``, ``YYYY-MM`` or
                ``YYYY-MM-DD``).
            period_end: End period.
            frequency: Data frequency (``'monthly'`` or ``'annual'``).

        Returns:
            List of valid periods in the API format (``YYYY`` or ``YYYYMM``).

        Raises:
            ValueError: If ``frequency`` is invalid.
        """
        # Vérification de la cohérence des paramètres
        if frequency not in VALID_FREQUENCIES:
            raise ValueError(
                f"Invalid value for frequency : {frequency}. "
                f"Should be in {VALID_FREQUENCIES}"
            )

        # Résolution des bornes de la plage (1962 → aujourd'hui par défaut)
        start = (
            self._validate_date(period=1962)
            if period_start is None
            else self._validate_date(period=period_start)
        )
        end = (
            datetime.today()
            if period_end is None
            else self._validate_date(period=period_end)
        )

        # Construction du champ des périodes valides
        valid_periods = (
            pd.date_range(
                start=start,
                end=end,
                freq="YS" if frequency == "annual" else "MS",
            )
            .strftime("%Y" if frequency == "annual" else "%Y%m")
            .tolist()
        )

        # La période en cours n'est jamais complète : retrait du dernier élément
        valid_periods = valid_periods[:-1]

        # Intersection avec les périodes en argument si renseignées
        if periods is not None:
            valid_periods = np.intersect1d(valid_periods, periods).tolist()

        return valid_periods

    # ──────────────────────────────────────────────────────────────────
    # Seam de téléchargement incrémental 
    # ──────────────────────────────────────────────────────────────────

    # Méthode de récupération de la disponibilité finale des données
    def get_final_data_availability(
        self,
        reporters: Optional[Union[List[int], List[str], int, str, None]] = None,
        periods: Optional[Union[List[str], str, None]] = None,
        frequency: str = "annual",
        type_code: str = "C",
        classification: str = "HS",
    ) -> pd.DataFrame:
        """Fetch the final-data availability for reporters and periods.

        Wraps ``comtradeapicall.getFinalDataAvailability``. Used by the download
        script to compare each release's ``lastReleased`` date with the last
        recorded download and decide whether a (reporter, period) couple must be
        refreshed.

        Args:
            reporters: Reporter codes (``None`` for all reporters).
            periods: Periods to check (``YYYY`` or ``YYYYMM``).
            frequency: Data frequency (``'annual'`` or ``'monthly'``).
            type_code: Trade type (``'C'`` or ``'S'``).
            classification: Classification code (``'HS'``, ...).

        Returns:
            DataFrame returned by ``getFinalDataAvailability``.
        """
        # Preprocessing des codes
        reporters = self._preprocess_codes(codes=reporters)
        if isinstance(periods, list):
            periods = ",".join([str(p) for p in periods])

        # Application du rate limiter avant l'appel API
        self._acquire()

        # Requête de la disponibilité finale des données
        return comtradeapicall.getFinalDataAvailability(
            subscription_key=self.subscription_key,
            typeCode=type_code,
            freqCode="A" if frequency == "annual" else "M",
            clCode=classification,
            reporterCode=reporters,
            period=periods,
        )

    # Méthode d'exécution d'un objet requête
    def execute_query(self, query: "ComtradeQueryRequest") -> pd.DataFrame:
        """Execute a :class:`ComtradeQueryRequest` and return its DataFrame.

        Maps the query selection fields onto :meth:`get_data` and discards the
        request metadata, returning only the DataFrame for symmetry with the
        SDMX clients' ``execute_query``.

        Args:
            query: Comtrade query request.

        Returns:
            DataFrame with the retrieved data.
        """
        # Délégation à get_data avec les champs de sélection de la requête
        df, _ = self.get_data(
            flows=query.flows,
            products=query.products,
            reporters=query.reporters,
            partners=query.partners,
            partners2=query.partners2,
            periods=query.periods,
            period_start=query.period_start,
            period_end=query.period_end,
            frequency=query.frequency,
        )
        return df

    # Méthode de résolution de la structure d'une requête
    def resolve_query_structure(
        self, query: "ComtradeQueryRequest"
    ) -> DataflowStructure:
        """Resolve the dataflow structure backing a query (for primary keys).

        Args:
            query: Comtrade query request.

        Returns:
            The :class:`DataflowStructure` of the query's dataflow.
        """
        # Délégation à get_structure avec les identifiants de la requête
        return self.get_structure(dataflow=query.dataflow, agency=query.agency)

    # Méthode d'extraction de la structure d'un dataflow
    def get_structure(
        self, dataflow: str, agency: str = AGENCY_ID
    ) -> DataflowStructure:
        """Return the structure of a Comtrade dataflow.

        UN Comtrade has no structure endpoint, so the structure is read from the
        registry (populated from ``parameters/comtrade.json``) or derived from
        the declared identifier columns and cached.

        Args:
            dataflow: Logical dataflow identifier (e.g. ``"C_A_HS"``).
            agency: Maintaining agency (default: ``"COMTRADE"``).

        Returns:
            The resolved :class:`DataflowStructure`.
        """
        # Recherche dans le registre avant toute construction
        cached = self.structure_registry.get(agency, dataflow)
        if cached is not None:
            return cached

        # Construction depuis les paramètres déclarés et mise en cache
        structure = parsing.build_structure_from_parameters(
            self._parameters, agency, dataflow
        )
        self.structure_registry.register(structure)
        return structure

    # ──────────────────────────────────────────────────────────────────
    # Fermeture des ressources
    # ──────────────────────────────────────────────────────────────────

    # Méthode de fermeture de la connexion HTTP
    def close(self) -> None:
        """Close the HTTP session and release resources."""
        # Fermeture de la session héritée d'APIClient
        super().close()
        logger.info("Comtrade client closed")
