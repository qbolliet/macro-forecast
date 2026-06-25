# Importation des modules
# Modules de base
import json
import os
import re
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union
from uuid import uuid4

# Module de l'API
import comtradeapicall
# from dotenv import load_dotenv
import numpy as np
import pandas as pd

# Modules ad hoc
from ..storage.loader import Loader
from ..storage.saver import Saver
from ..utils.logger import _init_logger
from ..utils.scrapers import Scraper

# Emplacement du fichier
FILE_PATH = Path(os.path.abspath(__file__))

# Chargement des variables d'environnement
# load_dotenv('../../.env')

# Importation des paramètres de l'API
with open(
    os.path.join(FILE_PATH.parents[2], "parameters/un_comtrade.json")
) as json_file:
    parameters = json.load(json_file)

# Importation des paramètres de l'API
with open(
    os.path.join(FILE_PATH.parents[2], "parameters/miscellanous.json")
) as json_file:
    miscellanous = json.load(json_file)


# Classe de récupération des données de commerce international du UNComtrade
# La méthodologie est accessible ici : https://comtradeapi.un.org/files/v1/app/wiki/MethodologyGuideforComtradePlus.pdf
class UNComtradeScraper(Scraper):
    """A class for retrieving international trade data from UN Comtrade.

    This class provides functionality to fetch, process and save trade data from the
    UN Comtrade database. It inherits from Scraper for web scraping capabilities.

    Args:
        log_filename (os.PathLike, optional): Path to the log file. Defaults to
            "logs/comtrade_builder.log".

    Attributes:
        loader: Loader for loal and S3 loading capabilities
        saver: Saver for loal and S3 saving capabilities
        logger: Logger object for tracking operations
        api_calls (int): Counter for number of API calls made

    Note:
        The methodology for UN Comtrade data is available at:
        https://comtradeapi.un.org/files/v1/app/wiki/MethodologyGuideforComtradePlus.pdf

    Examples:
        >>> scraper = UNComtradeScraper()
        >>> # Get metadata for a specific category
        >>> reporters = scraper.get_metadata(category='reporter')
        >>> # Get trade data
        >>> data = scraper.get_tarifline_data(
        ...     flows=['M', 'X'],
        ...     reporters='FRA',
        ...     period_start='2023-01',
        ...     period_end='2023-12'
        ... )
    """

    # Initialisation
    def __init__(
        self,
        loader: Optional[Loader] = None,
        saver: Optional[Saver] = None,
        api_calls: int = 0,
        log_filename: Optional[os.PathLike] = os.path.join(
            FILE_PATH.parents[2], "logs/comtrade_builder.log"
        ),
    ) -> None:
        """
        Initializes the UNComtradeScraper class with a log file.

        Args:
            log_filename (os.PathLike, optional): Path to the log file. Defaults to
                "logs/comtrade_builder.log".

        Returns:
            None
        """
        # Initialisation du parent
        super().__init__()
        # Initialisation des loaders et savers
        # Initialisation du loader (à l'argument où à une instance par défaut)
        if loader is not None:
            self.loader = loader
        else:
            self.loader = Loader()
        # Initialisation du saver (à l'argument ou à une instance par défaut)
        if saver is not None:
            self.saver = saver
        else:
            self.saver = Saver()

        # Initialisation du logger
        self.logger = _init_logger(filename=log_filename)
        # Initilisation du nombre d'appels à l'API
        self.api_calls = api_calls

    # Méthode auxiliaire de preprocessing des pays
    def _preprocess_codes(
        self, codes: Union[List[int], List[str], int, str, None]
    ) -> str:
        """Preprocess country or product codes into API format.

        Args:
            codes: Single code or list of codes to preprocess. Can be:
                - List[int]: List of numeric codes
                - List[str]: List of string codes
                - int: Single numeric code
                - str: Single string code
                - None: Returns None as is

        Returns:
            str: Comma-separated string of codes

        Examples:
            >>> scraper._preprocess_codes([1, 2, 3])
            '1,2,3'
            >>> scraper._preprocess_codes('FRA')
            'FRA'
            >>> scraper._preprocess_codes(['USA', 'CAN'])
            'USA,CAN'
        """
        # Disjonction de cas suivant le type des pays
        # S'il s'agit d'une liste de strings ou d'une liste d'entiers, ces-dernières sont concaténées
        if isinstance(codes, list):
            codes = ",".join([str(e) for e in codes])
        # Si c'est un entier, ce-dernier est converti en entier
        elif isinstance(codes, int):
            codes = str(codes)
        # S'il s'agit d'une string ou de None, il est renvoyé tel quel

        return codes

    # Méthode auxiliaire de définition d'une subdivision valide
    def _validate_subdivision(self, subdivision: Union[str, None]) -> bool:
        """Validate if a subdivision parameter is valid.

        Args:
            subdivision: The subdivision parameter to validate. Must be a comma-separated
                string with more than one value, or None.

        Returns:
            bool: True if subdivision is valid (None or contains multiple values),
                False otherwise.

        Examples:
            >>> scraper._validate_subdivision('USA,CAN,MEX')
            True
            >>> scraper._validate_subdivision('USA')
            False
            >>> scraper._validate_subdivision(None)
            True
        """
        # Test si est None ou une liste du plus d'un élément
        if subdivision is None:
            return True
        elif isinstance(subdivision, str):
            # Identification des différents items de la subdivision
            list_items = subdivision.split(",")
            if len(list_items) > 1:
                return True
            else:
                return False
        else:
            return False

    # Méthode auxiliaire d'extraction des codes
    def _extract_codes(self, category: str) -> list:
        """Extract valid codes for a given category from metadata.

        Args:
            category (str): Category to extract codes for. One of:
                - 'flow': Trade flow codes
                - 'reporter': Reporter country codes
                - 'partner': Partner country codes
                - 'cmd:HS': HS commodity codes

        Returns:
            list: List of valid codes for the category

        Raises:
            ValueError: If category is not valid

        Examples:
            >>> reporter_codes = scraper._extract_codes('reporter')
            >>> flow_codes = scraper._extract_codes('flow')
        """
        # Extraction des méta-données
        df = self.get_metadata(category=category)
        # Extraction des codes
        if category == "flow":
            return df["id"].tolist()
        elif category == "reporter":
            # On extrait les codes des pays existants
            return df.loc[df["entryExpiredDate"].isna(), "reporterCode"].tolist()
        elif category == "partner":
            # On extrait les codes des pays existants
            return df.loc[df["entryExpiredDate"].isna(), "PartnerCode"].tolist()
        elif category == "cmd:HS":
            # return df.loc[df['aggrLevel']==6, 'PartnerCode'].tolist()
            return df["id"].tolist()

    # Méthodes auxiliaire de subdivision de la requête
    def _divide_tarifline_request(
        self,
        subdivision: str,
        flows: Optional[Union[List[str], str, None]] = ["M", "X"],
        products: Optional[Union[List[int], List[str], int, str, None]] = None,
        reporters: Optional[Union[List[int], List[str], int, str, None]] = None,
        partners: Optional[Union[List[int], List[str], int, str, None]] = None,
        partners2: Optional[Union[List[int], List[str], int, str, None]] = None,
        periods: Optional[Union[List[str], str, None]] = None,
        frequency: Optional[str] = "monthly",
    ) -> pd.DataFrame:
        """Divide a large tariff line request into smaller chunks.

        Handles requests that exceed API limits by breaking them down based on the
        specified subdivision parameter.

        Args:
            subdivision (str): Type of subdivision ('flows', 'products', 'reporters', etc.)
            flows (List[str], optional): Trade flow codes
            products (List[str], optional): Product codes
            reporters (List[str], optional): Reporter country codes
            partners (List[str], optional): Partner country codes
            partners2 (List[str], optional): Secondary partner codes
            periods (List[str], optional): Time periods
            frequency (str, optional): Data frequency ('monthly' or 'annual')

        Returns:
            pd.DataFrame: Combined results from subdivided requests

        Raises:
            ValueError: If subdivision is invalid or request cannot be divided further

        Examples:
            >>> data = scraper._divide_tarifline_request(
            ...     subdivision='reporters',
            ...     reporters='USA,CAN,MEX',
            ...     period_start='2023-01',
            ...     period_end='2023-12'

            ... )
        """
        # Vérification que la valeur de la subdivision est valide
        if subdivision not in [
            "flows",
            "products",
            "reporters",
            "partners",
            "partners2",
            "periods",
        ]:
            raise ValueError(
                f"Invalid subdivision : {subdivision}. Should be in ['flows', 'products', 'reporters', 'partners', 'partners2', 'periods']"
            )

        # Extraction des items sur lesquels effectuer la subdivision
        items = locals()[subdivision]

        # Validation de la subdivision
        if self._validate_subdivision(subdivision=items):
            # Si est None, on requête l'ensemble des pays valides et on concatène deux sous-listes
            if (items is None) & (subdivision == "periods"):
                raise ValueError("Unnable to request the valid values for 'periods'")
            elif items is None:
                # Requête des options valides
                list_items = self._extract_codes(
                    category=parameters["SUBDIVISION_METADATA"][subdivision]
                )
            else:
                list_items = items.split(",")
            # Construction des deux sous-listes
            list_items1, list_items2 = (
                list_items[: (len(list_items) // 2)],
                list_items[(len(list_items) // 2):],
            )
            # Requête récursive sur deux sous-listes de partenaires
            df1, request_metadata1 = self.get_tarifline_data(
                flows=list_items1 if subdivision == "flows" else flows,
                products=list_items1 if subdivision == "products" else products,
                reporters=(
                    list_items1 if subdivision == "reporters" else reporters
                ),
                partners=list_items1 if subdivision == "partners" else partners,
                partners2=(
                    list_items1 if subdivision == "partners2" else partners2
                ),
                periods=list_items1 if subdivision == "periods" else periods,
                frequency=frequency,
            )
            df2, request_metadata2 = self.get_tarifline_data(
                flows=list_items2 if subdivision == "flows" else flows,
                products=list_items2 if subdivision == "products" else products,
                reporters=(
                    list_items2 if subdivision == "reporters" else reporters
                ),
                partners=list_items2 if subdivision == "partners" else partners,
                partners2=(
                    list_items2 if subdivision == "partners2" else partners2
                ),
                periods=list_items2 if subdivision == "periods" else periods,
                frequency=frequency,
            )
            # Concaténation des jeux de données
            df = pd.concat([df1, df2], axis=0, ignore_index=True)
            # Concaténation des métadonnées
            request_metadata = {
                k: f"{request_metadata1[k]},{request_metadata2[k]}"
                for k in request_metadata1.keys()  # Les deux dictionnaires ont les mêmes clés
            }

            return df, request_metadata

        else:
            raise ValueError(
                f"Unnable to further truncate the request with parameters : 'flows' : {flows}, 'products' : {products}, 'reporters' : {reporters}, 'partners' : {partners}, 'partners2' : {partners2}, 'periods' : {periods}"
            )

    # Méthode de téléchargement des données
    def get_tarifline_data(
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
        filepath: Optional[Union[os.PathLike, None]] = None,
        bucket: Optional[Union[str, None]] = None,
    ) -> pd.DataFrame:
        """Fetch tariff line data from UN Comtrade API.

        Args:
            flows (List[str], optional): Trade flow codes (e.g., ['M', 'X'])
            products (List[str], optional): Product codes
            reporters (List[str], optional): Reporter country codes
            partners (List[str], optional): Partner country codes
            partners2 (List[str], optional): Secondary partner codes
            periods (List[str], optional): Specific time periods
            period_start (str, optional): Start period (YYYY-MM)
            period_end (str, optional): End period (YYYY-MM)
            frequency (str, optional): Data frequency ('monthly' or 'annual')
            filepath (str, optional): Path to save the data
            bucket (str, optional): S3 bucket name if saving to S3

        Returns:
            pd.DataFrame: Fetched trade data

        Examples:
            >>> data = scraper.get_tarifline_data(
            ...     flows=['M', 'X'],
            ...     reporters='FRA',
            ...     period_start='2023-01',
            ...     period_end='2023-12',
            ...     frequency='monthly'
            ... )
        """
        # Vérification de la cohérence des paramètres
        if frequency not in ["annual", "monthly"]:
            raise ValueError(
                f"Invalid value for frequency : {frequency}. Should be in ['annual', 'monthly']"
            )

        # Initialisation du nombre de requête API
        initial_api_calls = deepcopy(self.api_calls)

        # Preprocessing des périodes
        if isinstance(periods, list):
            periods = ",".join(periods)
        elif (period_start is not None) & (period_end is not None):
            periods = ",".join(
                list(
                    map(
                        pd.date_range(
                            start=period_start,
                            end=period_end,
                            freq="YS" if frequency == "annual" else "MS",
                        )
                        .strftime("%Y" if frequency == "annual" else "%Y%m")
                        .tolist(),
                        str,
                    )
                )
            )
        elif period_start is not None:
            periods = ",".join(
                list(
                    map(
                        pd.date_range(
                            start=period_start,
                            end=datetime.today(),
                            freq="YS" if frequency == "annual" else "MS",
                        )
                        .strftime("%Y" if frequency == "annual" else "%Y%m")
                        .tolist(),
                        str,
                    )
                )
            )
        # Preprocessing des flux
        flows = self._preprocess_codes(codes=flows)
        # Preprocessing des codes M49 des pays
        reporters = self._preprocess_codes(codes=reporters)
        partners = self._preprocess_codes(codes=partners)
        partners2 = self._preprocess_codes(codes=partners2)
        # Preprocessing des codes de produits
        products = self._preprocess_codes(codes=products)

        # Requête des données
        df = comtradeapicall._getTarifflineData(
            os.getenv("COMTRADE_SUBSCRIPTION_KEY"),
            typeCode="C",  # Type of trade. 'C' for commodities and 'S' for service.
            freqCode=(
                "A" if frequency == "annual" else "M"
            ),  # Trade frequency: 'A' for annual and 'M' for monthly
            clCode="HS",  # Trade (IMTS) classifications: 'HS', 'SITC', 'BEC' or 'EBOPS'.
            period=periods,  # Year or month. Year should be 4 digit year. Month should be six digit integer with the values of the form YYYYMM.
            reporterCode=reporters,  # Reporter code (Possible values are M49 code of the countries separated by comma (,))
            cmdCode=products,  # Commodity code. Multi value input should be in the form of csv (Codes separated by comma (,))
            flowCode=flows,  # Trade flow code. Multi value input should be in the form of csv (Codes separated by comma (,)). Possible values at : https://comtradeapi.un.org/files/v1/app/reference/tradeRegimes.json
            partnerCode=partners,  # Partner code (Possible values are M49 code of the countries separated by comma (,))
            partner2Code=partners2,  # Second partner/consignment code (Possible values are M49 code of the countries separated by comma (,))
            customsCode=None,  # Customs code. Multi value input should be in the form of csv (Codes separated by comma (,)). Possible values at : https://comtradeapi.un.org/files/v1/app/reference/CustomsCodes.json
            motCode=None,  # Mode of transport code. Multi value input should be in the form of csv (Codes separated by comma (,))
            maxRecords=None,
            format_output="JSON",
            countOnly=None,
            includeDesc=True,  # Include descriptions of data variables
            proxy_url=(
                miscellanous["PROXY"]
                if miscellanous["PROXY"] is None
                else f"http://{miscellanous['PROXY']}"
            ),
        )
        # Incrément du nombre d'appels à l'API
        self.api_calls += 1

        # Si on atteint la limite du nombre d'observations requêtable, la requête est subdivisée
        if len(df) >= parameters["LIMIT"]:
            # Test des subdivisions valides
            # Sur les flux
            if self._validate_subdivision(subdivision=flows):
                df, request_metadata = self._divide_tarifline_request(
                    subdivision="flows",
                    flows=flows,
                    products=products,
                    reporters=reporters,
                    partners=partners,
                    partners2=partners2,
                    periods=periods,
                    frequency=frequency,
                )
            # Sur les produits
            elif self._validate_subdivision(subdivision=products):
                df, request_metadata = self._divide_tarifline_request(
                    subdivision="products",
                    flows=flows,
                    products=products,
                    reporters=reporters,
                    partners=partners,
                    partners2=partners2,
                    periods=periods,
                    frequency=frequency,
                )
            # Sur les pays
            elif self._validate_subdivision(subdivision=reporters):
                df, request_metadata = self._divide_tarifline_request(
                    subdivision="reporters",
                    flows=flows,
                    products=products,
                    reporters=reporters,
                    partners=partners,
                    partners2=partners2,
                    periods=periods,
                    frequency=frequency,
                )
            elif self._validate_subdivision(subdivision=partners):
                df, request_metadata = self._divide_tarifline_request(
                    subdivision="partners",
                    flows=flows,
                    products=products,
                    reporters=reporters,
                    partners=partners,
                    partners2=partners2,
                    periods=periods,
                    frequency=frequency,
                )
            elif self._validate_subdivision(subdivision=partners2):
                df, request_metadata = self._divide_tarifline_request(
                    subdivision="partners2",
                    flows=flows,
                    products=products,
                    reporters=reporters,
                    partners=partners,
                    partners2=partners2,
                    periods=periods,
                    frequency=frequency,
                )
            # Sur les périodes
            elif self._validate_subdivision(subdivision=periods):
                df, request_metadata = self._divide_tarifline_request(
                    subdivision="periods",
                    flows=flows,
                    products=products,
                    reporters=reporters,
                    partners=partners,
                    partners2=partners2,
                    periods=periods,
                    frequency=frequency,
                )
            else:
                # Logging
                self.logger.warning(
                    f"Unnable to further truncate the request with parameters : 'flows' : {flows}, 'products' : {products}, 'reporters' : {reporters}, 'partners' : {partners}, 'partners2' : {partners2}, 'periods' : {periods}"
                )
        # Il y a un appel récursif de "get_tarifline_data" dans "_divide_tarifline_request", pour ne pas exporter plusieurs fois les mêmes données
        else:
            # Création des méta-données
            request_metadata = {
                "flows": str(flows),
                "products": str(products),
                "reporters": str(reporters),
                "partners": str(partners),
                "partners2": str(partners2),
                "periods": str(periods),
                "frequency": str(frequency)
            }
            # Export des données
            if (not df.empty) & (filepath is not None):
                # Nom du fichier à exporter
                list_filename = []
                for e in [flows, products, reporters, partners, partners2, periods, frequency]:
                    if isinstance(e, list):
                        if len(e) == 1:
                            list_filename.append(e[0])
                        else:
                            list_filename.append(f"{e[0]}-{e[-1]}")
                    elif isinstance(e, str):
                        # Séparation des éléments
                        list_e = e.split(',')
                        if len(list_e) == 1:
                            list_filename.append(list_e[0])
                        else:
                            list_filename.append(f"{list_e[0]}-{list_e[-1]}")
                    elif e is not None:
                        list_filename.append(e)
                # Ajout d'un code uuid pour garantir l'unicité
                filename = "_".join(list_filename) + f"_{uuid4()}.csv"
                # Export des données
                self.saver.save(filepath=os.path.join(filepath, filename), obj=df, bucket=bucket, index=False)
                # Logging
                self.logger.info(
                    f"Succesfully exported tarifline data with parameters : 'flows' : {flows}, 'products' : {products}, 'reporters' : {reporters}, 'partners' : {partners}, 'partners2' : {partners2}, 'periods' : {periods} to {os.path.join(filepath, filename)}."
                )
            else:
                # Logging
                if df.empty:
                    self.logger.warning(
                        f"Did not export tarifline data with parameters : 'flows' : {flows}, 'products' : {products}, 'reporters' : {reporters}, 'partners' : {partners}, 'partners2' : {partners2}, 'periods' : {periods}. Empty DataFrame"
                    )
                else:
                    self.logger.warning(
                        f"Did not export tarifline data with parameters : 'flows' : {flows}, 'products' : {products}, 'reporters' : {reporters}, 'partners' : {partners}, 'partners2' : {partners2}, 'periods' : {periods}. No export path specified"
                    )

        # Logging du nombre d'appels nécessaires pour finaliser la requête
        self.logger.info(
            f"{self.api_calls - initial_api_calls} api calls needed to fetch tarifline data with parameters : 'flows' : {flows}, 'products' : {products}, 'reporters' : {reporters}, 'partners' : {partners}, 'partners2' : {partners2}, 'periods' : {periods}"
        )

        return df, request_metadata

    # Méthode de chargement des méta-données
    def get_metadata(self, category: Optional[Union[str, None]] = None) -> pd.DataFrame:
        """Fetch metadata for a specific category from UN Comtrade.

        Args:
            category (str, optional): Category to fetch metadata for. If None,
                returns list of all available categories.

        Returns:
            pd.DataFrame: Metadata for the specified category

        Examples:
            >>> # Get all categories
            >>> categories = scraper.get_metadata()
            >>> # Get reporter countries
            >>> reporters = scraper.get_metadata(category='reporter')
        """
        # Chargement des références
        metadata_index = comtradeapicall.listReference(
            category=category,
            proxy_url=(
                miscellanous["PROXY"]
                if miscellanous["PROXY"] is None
                else f"http://{miscellanous['PROXY']}"
            ),
        )

        # Si le jeu de données est vide, renvoi une erreur avec les modalités valides
        if metadata_index.empty:
            # Requête de l'ensemble des possibilités
            metadata_options = comtradeapicall.listReference(
                category=None,
                proxy_url=(
                    miscellanous["PROXY"]
                    if miscellanous["PROXY"] is None
                    else f"http://{miscellanous['PROXY']}"
                ),
            )
            # Erreur
            raise ValueError(
                f"Invalid 'category' : {category}. To get further information, run with category=None. 'category' should be in {metadata_options['category'].tolist()}."
            )

        # Si la catégorie n'est pas spécifiée, renvoi le registre des méta-données
        if category is None:
            return metadata_index
        else:
            # Initialisation du session s'il n'en existe pas déjà une
            if not hasattr(self, "session"):
                self._init_session()
            # Requête
            response = self.session.get(metadata_index["fileuri"].iloc[0])

            # Disjonction de cas suivant le statut de la requête
            if response.status_code == 200:
                # Extraction des données
                data = response.json()

                # Conversion des données en DataFrame
                df = pd.json_normalize(data["results"])

                return df
            else:
                raise ValueError(
                    f"Failed to retrieve data for category : {category}. Status code: {response.status_code}"
                )

    # Méthode auxiliaire de validation du format de la période
    def _validate_date(self, period: Union[str, int]) -> str:
        """Validate and format a date period string.

        Args:
            period (Union[str, int]): Period to validate (YYYY, YYYY-MM, or YYYY-MM-DD)

        Returns:
            str: Validated and formatted date string (YYYY-MM-DD)

        Raises:
            ValueError: If period format is invalid

        Examples:
            >>> scraper._validate_date('2023')
            '2023-01-01'
            >>> scraper._validate_date('2023-06')
            '2023-06-01'
            >>> scraper._validate_date('2023-06-15')
            '2023-06-15'
        """
        # Conversion en string
        period = str(period)
        # Stripping de la chaine de caractère et remplacement de tous les caractères non numériques par '-'
        period = re.sub(r"\D", "-", period.strip())

        # La période doit avoir au minimum quatre chiffres (correspondants à une année)
        if len(period) < 4:
            raise ValueError("Period must be at least 4 digits long")

        # Complétion de la chaine de caractères de sorte qu'elle soit de longueur 10 avec "-01"
        if len(period) == 4:
            period += "-01-01"
        elif len(period) == 7:
            period += "-01"
        elif len(period) != 10:
            raise ValueError(
                "Period must be in the format YYYY or YYYY-MM or YYYY-MM-DD"
            )

        return period

    # Méthode de construction des périodes valides pour requêter des données de commerce international
    def get_valid_periods(
        self,
        periods: Optional[Union[List[str], str, None]] = None,
        period_start: Optional[Union[str, None]] = None,
        period_end: Optional[Union[str, None]] = None,
        frequency: Optional[str] = "monthly",
    ) -> List[str]:
        """Generate list of valid periods for trade data requests.

        Args:
            periods (List[str], optional): List of specific periods to validate
            period_start (str, optional): Start period (YYYY, YYYY-MM, or YYYY-MM-DD)
            period_end (str, optional): End period (YYYY, YYYY-MM, or YYYY-MM-DD)
            frequency (str, optional): Data frequency ('monthly' or 'annual')

        Returns:
            List[str]: List of valid periods in the specified format

        Examples:
            >>> # Get monthly periods for 2023
            >>> periods = scraper.get_valid_periods(
            ...     period_start='2023-01',
            ...     period_end='2023-12',
            ...     frequency='monthly'
            ... )
            >>> # Get specific annual periods
            >>> periods = scraper.get_valid_periods(
            ...     periods=['2020', '2021', '2022'],
            ...     frequency='annual'
            ... )
        """
        # Vérification de la cohérence des paramètres
        if frequency not in ["annual", "monthly"]:
            raise ValueError(
                f"Invalid value for frequency : {frequency}. Should be in ['annual', 'monthly']"
            )

        # Construction du champ des possibles
        # Si aucune date de début ou de fin n'est donnée, les données sont requêtées entre 1962 et aujourd'hui
        if (period_start is None) & (period_end is None):
            valid_periods = (
                pd.date_range(
                    start=self._validate_date(period=1962),
                    end=datetime.today(),
                    freq="YS" if frequency == "annual" else "MS",
                )
                .strftime("%Y" if frequency == "annual" else "%Y%m")
                .tolist()
            )
        elif period_start is None:
            valid_periods = (
                pd.date_range(
                    start=self._validate_date(period=1962),
                    end=self._validate_date(period=period_end),
                    freq="YS" if frequency == "annual" else "MS",
                )
                .strftime("%Y" if frequency == "annual" else "%Y%m")
                .tolist()
            )
        elif period_end is None:
            valid_periods = (
                pd.date_range(
                    start=self._validate_date(period=period_start),
                    end=datetime.today(),
                    freq="YS" if frequency == "annual" else "MS",
                )
                .strftime("%Y" if frequency == "annual" else "%Y%m")
                .tolist()
            )
        else:
            valid_periods = (
                pd.date_range(
                    start=self._validate_date(period=period_start),
                    end=self._validate_date(period=period_end),
                    freq="YS" if frequency == "annual" else "MS",
                )
                .strftime("%Y" if frequency == "annual" else "%Y%m")
                .tolist()
            )
        # La période en cours n'est jamais comprise dans les données, aussi on peut supprimer le dernier élément de la liste des périodes valides
        valid_periods = valid_periods[:-1]
        # Intersection des périodes valides avec les périodes en argument si ces-dernières sont renseignées
        if periods is not None:
            valid_periods = np.intersect1d(valid_periods, periods).tolist()

        return valid_periods

    # Méthode de construction des données
    def build_tarifline_data(
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
        usecols: Optional[Union[List[str], None]] = None,
        id_cols: Optional[Union[List[str], None]] = None,
        aggregation_cols: Optional[Union[List[str], None]] = None,
        raw_filepath: Optional[Union[os.PathLike, None]] = None,
        bucket: Optional[Union[str, None]] = None,
    ) -> pd.DataFrame:
        """Build comprehensive trade data with optional aggregation.

        Args:
            flows (List[str], optional): Trade flow codes
            products (List[str], optional): Product codes
            reporters (List[str], optional): Reporter country codes
            partners (List[str], optional): Partner country codes
            partners2 (List[str], optional): Secondary partner codes
            periods (List[str], optional): Time periods
            period_start (str, optional): Start period
            period_end (str, optional): End period
            frequency (str, optional): Data frequency ('monthly' or 'annual')
            usecols (List[str], optional): Columns to keep in output
            id_cols (List[str], optional): Columns to use as identifiers
            aggregation_cols (List[str], optional): Columns to aggregate
            raw_filepath (str, optional): Path to save raw data
            bucket (str, optional): S3 bucket name

        Returns:
            pd.DataFrame: Processed trade data

        Examples:
            >>> data = scraper.build_tarifline_data(
            ...     flows=['M', 'X'],
            ...     reporters='FRA',
            ...     period_start='2023-01',
            ...     period_end='2023-12',
            ...     id_cols=['period', 'reporter', 'partner'],
            ...     aggregation_cols=['value', 'quantity']
            ... )
        """
        # Requête des données
        df, request_metadata = self.get_tarifline_data(
            flows=flows,
            products=products,
            reporters=reporters,
            partners=partners,
            partners2=partners2,
            periods=periods,
            period_start=period_start,
            period_end=period_end,
            frequency=frequency,
            filepath=raw_filepath,
            bucket=bucket,
        )

        # Si le jeu de données n'est pas vide
        if not df.empty:
            # Restriction aux colonnes d'intérêt
            if usecols is not None:
                df = df[np.unique(usecols + parameters["FLOW_COLUMNS"]).tolist()]
            elif (id_cols is not None) & (aggregation_cols is not None):
                df = df[
                    np.unique(
                        id_cols + aggregation_cols + parameters["FLOW_COLUMNS"]
                    ).tolist()
                ]
            elif aggregation_cols is not None:
                df = df[
                    np.unique(aggregation_cols + parameters["FLOW_COLUMNS"]).tolist()
                ]
            elif id_cols is not None:
                df = df[np.unique(id_cols + parameters["FLOW_COLUMNS"]).tolist()]

            # Agrégation par flux
            df = self.agg_by_flow(
                df=df, id_cols=id_cols, aggregation_cols=aggregation_cols
            )

        return df, request_metadata

    # Construction de données symétriques
    def build_symetric_tarifline_data(
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
        usecols: Optional[Union[List[str], None]] = None,
        id_cols: Optional[Union[List[str], None]] = None,
        aggregation_cols: Optional[Union[List[str], None]] = None,
        symetric_flow: Optional[str] = "import",
        raw_filepath: Optional[Union[os.PathLike, None]] = None,
        bucket: Optional[Union[str, None]] = None,
    ) -> pd.DataFrame:
        """Build symmetrized trade data by comparing reporter and partner declarations.

        Args:
            flows (List[str], optional): Trade flow codes
            products (List[str], optional): Product codes
            reporters (List[str], optional): Reporter country codes
            partners (List[str], optional): Partner country codes
            partners2 (List[str], optional): Secondary partner codes
            periods (List[str], optional): Time periods
            period_start (str, optional): Start period
            period_end (str, optional): End period
            frequency (str, optional): Data frequency ('monthly' or 'annual')
            usecols (List[str], optional): Columns to keep in output
            id_cols (List[str], optional): Columns to use as identifiers
            aggregation_cols (List[str], optional): Columns to aggregate
            symetric_flow (str, optional): Flow to symmetrize ('import' or 'export')
            raw_filepath (str, optional): Path to save raw data
            bucket (str, optional): S3 bucket name

        Returns:
            pd.DataFrame: Symmetrized trade data

        Examples:
            >>> data = scraper.build_symetric_tarifline_data(
            ...     flows=['M', 'X'],
            ...     reporters=['FRA', 'DEU'],
            ...     period='2023',
            ...     symetric_flow='import'
            ... )
        """
        # Construction des données
        df, request_metadata = self.build_tarifline_data(
            flows=flows,
            products=products,
            reporters=reporters,
            partners=partners,
            partners2=partners2,
            periods=periods,
            period_start=period_start,
            period_end=period_end,
            frequency=frequency,
            usecols=usecols,
            id_cols=np.unique(id_cols + ["flowCode"]).tolist(),
            aggregation_cols=aggregation_cols,
            raw_filepath=raw_filepath,
            bucket=bucket,
        )

        # Si le jeu de données n'est pas vide
        if not df.empty:
            # Agrégation des flux symétriques
            df = self.agg_symetric_flows(
                df=df, id_cols=id_cols, symetric_flow=symetric_flow
            )
            # Suppression de la colonne 'flowCode' si elle ne fait pas partie des colonnes d'identifiants
            if "flowCode" not in id_cols:
                df.drop("flowCode", axis=1, inplace=True)

        return df, request_metadata

    # Agrégation par flux X pays1 X pays2 x période X nomenclature
    # /!\ On a toujours des duplicats par 'typeCode', 'freqCode', 'period', 'reporterISO', 'flowDesc', 'partnerISO', 'cmdCode', 'classificationCode', 'partner2ISO', 'mosCode', 'motCode', 'qtyUnitCode' que l'on ne sait pas expliquer et que l'on somme par défaut
    def agg_by_flow(
        self,
        df: pd.DataFrame,
        id_cols: Optional[Union[List[str], None]] = None,
        aggregation_cols: Optional[Union[List[str], None]] = None,
    ) -> pd.DataFrame:
        """Aggregate trade data by flow and other dimensions.

        Args:
            df (pd.DataFrame): Input trade data
            id_cols (List[str], optional): Columns to use as identifiers
            aggregation_cols (List[str], optional): Columns to aggregate

        Returns:
            pd.DataFrame: Aggregated trade data

        Examples:
            >>> aggregated = scraper.agg_by_flow(
            ...     df=raw_data,
            ...     id_cols=['period', 'reporter'],
            ...     aggregation_cols=['value']
            ... )
        """
        # Extraction des colonnes de variables catégorielles du jeu de données
        id_cols = (
            np.unique(id_cols + parameters["FLOW_COLUMNS"]).tolist()
            if id_cols is not None
            else np.unique(
                df.select_dtypes(include=["object", "category"]).columns.tolist()
                + parameters["FLOW_COLUMNS"]
            ).tolist()
        )
        # Extraction des valeurs numériques du jeu de données
        aggregation_cols = (
            np.unique(aggregation_cols + parameters["FLOW_COLUMNS"]).tolist()
            if aggregation_cols is not None
            else np.unique(
                df.select_dtypes(include=["number"]).columns.tolist()
                + parameters["FLOW_COLUMNS"]
            ).tolist()
        )

        # Déduplication des variables catégorielles
        df_category = df[id_cols].drop_duplicates(
            subset=parameters["FLOW_COLUMNS"], keep="first"
        )

        # Somme des variables continues
        df_number = (
            df[aggregation_cols]
            .groupby(parameters["FLOW_COLUMNS"], as_index=False)[
                np.setdiff1d(aggregation_cols, parameters["FLOW_COLUMNS"]).tolist()
            ]
            .sum()
        )

        # Appariement des deux jeux de données
        df_res = pd.merge(
            left=df_category,
            right=df_number,
            how="left",
            on=parameters["FLOW_COLUMNS"],
            validate="one_to_one",
        )

        return df_res

    # Agrégation des flux d'imports exports symétriques
    def agg_symetric_flows(
        self,
        df: pd.DataFrame,
        id_cols: Optional[Union[List[str], None]] = None,
        symetric_flow: Optional[str] = "import",
    ) -> pd.DataFrame:
        """Aggregate symmetric import/export flows.

        Args:
            df (pd.DataFrame): Input trade data
            id_cols (List[str], optional): Columns to use as identifiers
            symetric_flow (str): Flow type to symmetrize ('import' or 'export')

        Returns:
            pd.DataFrame: Aggregated symmetric flows

        Examples:
            >>> symmetric = scraper.agg_symetric_flows(
            ...     df=raw_data,
            ...     id_cols=['period', 'reporter'],
            ...     symetric_flow='import'
            ... )
        """
        # Vérification de la valeur du paramètre
        if symetric_flow not in ["import", "export"]:
            raise ValueError(
                "Invalid value for 'symetric_flow'. Should be in ['import', 'export']"
            )

        # Séparation des flux d'imports / export
        df_imp = df.loc[df["flowCode"].isin(["M", "FM", "MIP", "MOP", "RM"])]
        df_exp = df.loc[df["flowCode"].isin(["X", "DX", "RX", "XIP", "XOP"])]

        # Identification des colonnes relatives aux pays d'origine et de destination
        reporter_columns = [col for col in df.columns if col.startswith("reporter")]
        partner_columns = [
            col
            for col in df.columns
            if col.startswith("partner") and not col.startswith("partner2")
        ]

        # Inversion des pays d'imports et d'exports
        inverse_reporter_columns = {
            col: col.replace("reporter", "partner") for col in reporter_columns
        }
        inverse_partner_columns = {
            col: col.replace("partner", "reporter") for col in partner_columns
        }

        # Identification des variables catégorielles sur lesquelles moyenner
        id_cols = (
            np.unique(id_cols + parameters["FLOW_COLUMNS"]).tolist()
            if id_cols is not None
            else np.unique(
                df.select_dtypes(include=["object", "category"]).columns.tolist()
                + parameters["FLOW_COLUMNS"]
            ).tolist()
        )
        # Identification des colonnes sur lesquelles agréger les données
        groupby_cols = np.unique(
            id_cols
            + ["flowCode"]
            + reporter_columns
            + partner_columns
            + list(inverse_reporter_columns.values())
            + list(inverse_partner_columns.values())
        ).tolist()

        # Création des jeux de données
        if symetric_flow == "import":
            # Jeu de données d'imports
            df_reversed_imp = df_exp.rename(
                inverse_partner_columns | inverse_reporter_columns, axis=1
            )
            df_reversed_imp["flowCode"] = df_reversed_imp["flowCode"].replace(
                {"X": "M", "DX": "FM", "RX": "RM", "XIP": "MIP", "XOP": "MOP"}
            )
            # Si la description des flux est également dans les colonnes, on change les libellés correspondants
            if "flowDesc" in df.columns:
                df_reversed_imp["flowDesc"] = df_reversed_imp["flowDesc"].replace(
                    {
                        "Export": "Import",
                        "Domestic Export": "Foreign Import",
                        "Re-export": "Re-import",
                        "Export of goods after inward processing": "Import of goods for inward processing",
                        "Export of goods for outward processing": "Import of goods after outward processing",
                    }
                )
            df_res = pd.concat([df_imp, df_reversed_imp], axis=0, join="inner")
        elif symetric_flow == "export":
            # Jeu de données d'exports
            df_reversed_exp = df_imp.rename(
                inverse_partner_columns | inverse_reporter_columns, axis=1
            )
            df_reversed_exp["flowCode"] = df_reversed_exp["flowCode"].replace(
                {"M": "X", "FM": "DX", "MIP": "XIP", "MOP": "XOP", "RM": "RX"}
            )
            # Si la description des flux est également dans les colonnes, on change les libellés correspondants
            if "flowDesc" in df.columns:
                df_reversed_exp["flowDesc"] = df_reversed_exp["flowDesc"].replace(
                    {
                        "Import": "Export",
                        "Foreign Import": "Domestic Export",
                        "Re-import": "Re-export",
                        "Import of goods for inward processing": "Export of goods after inward processing",
                        "Import of goods after outward processing": "Export of goods for outward processing",
                    }
                )
            df_res = pd.concat([df_exp, df_reversed_exp], axis=0, join="inner")

        # Moyenne des deux flux déclarés
        df_res = df_res.groupby(
            np.intersect1d(groupby_cols, df_res.columns.tolist()).tolist(),
            as_index=False,
        )[np.setdiff1d(df_res.columns.tolist(), groupby_cols)].mean()

        return df_res

    # Méthode de construction des données en définissant une partition par produits et période
    def build_tarifline_by_period_product(
        self,
        name: str,
        export_path: os.PathLike,
        executed_requests_filepath: os.PathLike,
        empty_requests_filepath: os.PathLike,
        bucket: Optional[Union[str, None]] = None,
        raw_export_path: Optional[Union[os.PathLike, None]] = None,
        flows: Optional[Union[List[str], str, None]] = ["M", "X"],
        products: Optional[Union[List[int], List[str], int, str, None]] = None,
        products_step: Optional[int] = 10,
        periods: Optional[Union[List[str], str, None]] = None,
        period_start: Optional[Union[str, None]] = None,
        period_end: Optional[Union[str, None]] = None,
        frequency: Optional[str] = "monthly",
        id_cols: Optional[Union[List[str], None]] = None,
        aggregation_cols: Optional[Union[List[str], None]] = None,
        symetric_flow: Optional[Union[str, None]] = "import",
    ) -> None:
        """Build trade data by period and product with automatic pagination.

        Iterates over periods and product batches, skipping already-executed
        requests. Empty responses are tracked separately: they are skipped as
        long as fresh (unexecuted, non-empty) batches remain, and retried only
        once the period is otherwise complete.

        Args:
            name (str): Name identifier for the build.
            export_path (os.PathLike): Path to export processed data.
            executed_requests_filepath (os.PathLike): Path to the JSON file
                tracking successfully completed requests.
            empty_requests_filepath (os.PathLike): Path to the JSON file
                tracking requests that returned an empty DataFrame.
            bucket (str, optional): S3 bucket name.
            raw_export_path (os.PathLike, optional): Path to save raw data.
            flows (List[str], optional): Trade flow codes.
            products (List[str], optional): Product codes.
            products_step (int, optional): Number of products per request.
            periods (List[str], optional): Time periods.
            period_start (str, optional): Start period.
            period_end (str, optional): End period.
            frequency (str, optional): Data frequency ('monthly' or 'annual').
            id_cols (List[str], optional): Columns to use as identifiers.
            aggregation_cols (List[str], optional): Columns to aggregate.
            symetric_flow (str, optional): Flow to symmetrize.

        Examples:
            >>> scraper.build_tarifline_by_period_product(
            ...     name='EU_trade_2023',
            ...     export_path='data/processed',
            ...     executed_requests_filepath='logs/executed_requests.json',
            ...     empty_requests_filepath='logs/empty_requests.json',
            ...     products_step=10,
            ...     period_start='2023-01',
            ...     period_end='2023-12'
            ... )
        """
        # Initialisation des produits
        if products is None:
            # La nomenclature 'cmd:HS' contient toutes les nomenclatures (présentes et passées)
            products = self.get_metadata(category="cmd:HS")["id"].tolist()
            # Filtre sur les SH6 uniquement (une requête SH agrégé couvre ses sous-nomenclatures)
            products = [product for product in products if len(product) == 6]

        # Initialisation des périodes
        if periods is None:
            # Extraction des périodes valides
            periods = self.get_valid_periods(
                period_start=period_start, period_end=period_end, frequency=frequency
            )

        # Parcours des périodes
        for period in reversed(periods):
            # Vérification du plafond d'appels API
            if self.api_calls > parameters["MAX_API_CALLS"]:
                self.logger.warning(
                    f"Process terminated prematurely because the number of API calls needed to complete the request exceeds the maximum parameter : {parameters['MAX_API_CALLS']}"
                )
                break

            # Chargement initial des requêtes pour déterminer les lots frais
            try:
                executed_requests_init = self.loader.load(
                    filepath=executed_requests_filepath, bucket=bucket
                )
            except Exception:
                executed_requests_init = {}

            try:
                empty_requests_init = self.loader.load(
                    filepath=empty_requests_filepath, bucket=bucket
                )
            except Exception:
                empty_requests_init = {}

            executed_products_init = set(
                executed_requests_init.get(name, {}).get(period, [])
            )
            empty_products_init = set(
                empty_requests_init.get(name, {}).get(period, [])
            )

            # Détermination de l'existence de lots frais pour cette période
            # (ni exécutés avec succès, ni vides lors des précédentes exécutions)
            has_fresh_batches = any(
                len(
                    np.setdiff1d(
                        products[i: min(i + products_step, len(products))],
                        list(executed_products_init) + list(empty_products_init),
                    )
                ) > 0
                for i in range(0, len(products), products_step)
            )

            # Parcours des nomenclatures par lot
            for i in range(0, len(products), products_step):
                # Vérification du plafond d'appels API
                if self.api_calls > parameters["MAX_API_CALLS"]:
                    self.logger.warning(
                        f"Process terminated prematurely because the number of API calls needed to complete the request exceeds the maximum parameter : {parameters['MAX_API_CALLS']}"
                    )
                    break

                list_nomenclature = products[i: min(i + products_step, len(products))]

                # Rechargement des requêtes depuis le stockage (état le plus récent)
                try:
                    executed_requests = self.loader.load(
                        filepath=executed_requests_filepath, bucket=bucket
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Could not load executed requests : {str(e)}. Initialized it to a new one"
                    )
                    executed_requests = {}

                try:
                    empty_requests = self.loader.load(
                        filepath=empty_requests_filepath, bucket=bucket
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Could not load empty requests : {str(e)}. Initialized it to a new one"
                    )
                    empty_requests = {}

                # Filtrage des produits déjà exécutés avec succès
                if name not in executed_requests or period not in executed_requests.get(name, {}):
                    list_nomenclature_not_executed = list_nomenclature
                else:
                    list_nomenclature_not_executed = np.setdiff1d(
                        list_nomenclature, executed_requests[name][period]
                    ).tolist()

                if len(list_nomenclature_not_executed) == 0:
                    self.logger.info(
                        f"Already retrieved tarifline data with parameters : 'period' : {period}, 'products' : {list_nomenclature}"
                    )
                    continue

                # Filtrage des produits ayant retourné un DataFrame vide
                # (ignorés tant que des lots frais existent pour la période)
                empty_products = set(empty_requests.get(name, {}).get(period, []))

                if has_fresh_batches:
                    list_nomenclature_request = [
                        p for p in list_nomenclature_not_executed
                        if p not in empty_products
                    ]
                else:
                    # Mode rattrapage : réexécution des lots précédemment vides
                    list_nomenclature_request = list_nomenclature_not_executed

                if len(list_nomenclature_request) == 0:
                    continue

                # Requête
                if symetric_flow is None:
                    df, request_metadata = self.build_tarifline_data(
                        flows=flows,
                        products=list_nomenclature_request,
                        reporters=None,
                        partners=None,
                        partners2=None,
                        periods=period,
                        period_start=None,
                        period_end=None,
                        frequency=frequency,
                        usecols=None,
                        id_cols=id_cols,
                        aggregation_cols=aggregation_cols,
                        raw_filepath=raw_export_path,
                        bucket=bucket,
                    )
                else:
                    df, request_metadata = self.build_symetric_tarifline_data(
                        flows=flows,
                        products=list_nomenclature_request,
                        reporters=None,
                        partners=None,
                        partners2=None,
                        periods=period,
                        period_start=None,
                        period_end=None,
                        frequency=frequency,
                        usecols=None,
                        id_cols=id_cols,
                        aggregation_cols=aggregation_cols,
                        symetric_flow=symetric_flow,
                        raw_filepath=raw_export_path,
                        bucket=bucket,
                    )

                # Traitement du résultat
                if isinstance(df, pd.DataFrame):
                    if not df.empty:
                        # Extraction des nomenclatures effectivement retournées
                        list_nomenclature_completed_request = sorted(
                            request_metadata["products"].split(",")
                        )

                        # Construction du nom du fichier retourné
                        if len(list_nomenclature_completed_request) == 1:
                            filename = list_nomenclature_completed_request[0]
                        else:
                            filename = f"{list_nomenclature_completed_request[0]}-{list_nomenclature_completed_request[-1]}"

                        # Export du jeu de données
                        self.saver.save(
                            filepath=os.path.join(
                                export_path,
                                f"{period}/{filename}.csv",
                            ),
                            bucket=bucket,
                            obj=df,
                            index=False,
                        )

                        # Mise à jour de executed_requests
                        if name not in executed_requests:
                            executed_requests[name] = {
                                period: list_nomenclature_completed_request
                            }
                        elif period not in executed_requests[name]:
                            executed_requests[name][period] = list_nomenclature_completed_request
                        else:
                            executed_requests[name][period] = (
                                executed_requests[name][period]
                                + list_nomenclature_completed_request
                            )
                        self.saver.save(
                            filepath=executed_requests_filepath,
                            bucket=bucket,
                            obj=executed_requests,
                        )

                        # Retrait éventuel de empty_requests si le lot était précédemment vide
                        if (
                            name in empty_requests
                            and period in empty_requests.get(name, {})
                        ):
                            updated_empty = [
                                p for p in empty_requests[name][period]
                                if p not in list_nomenclature_completed_request
                            ]
                            empty_requests[name][period] = updated_empty
                            self.saver.save(
                                filepath=empty_requests_filepath,
                                bucket=bucket,
                                obj=empty_requests,
                            )

                        # Logging
                        self.logger.info(
                            f"Successfully retrieved and exported tarifline data with parameters : 'period' : {period}, 'products' : {list_nomenclature_request}"
                        )

                    else:
                        # Enregistrement du lot vide dans empty_requests
                        if name not in empty_requests:
                            empty_requests[name] = {period: list_nomenclature_request}
                        elif period not in empty_requests[name]:
                            empty_requests[name][period] = list_nomenclature_request
                        else:
                            empty_requests[name][period] = list(
                                set(empty_requests[name][period])
                                | set(list_nomenclature_request)
                            )
                        self.saver.save(
                            filepath=empty_requests_filepath,
                            bucket=bucket,
                            obj=empty_requests,
                        )

                        # Logging
                        self.logger.warning(
                            f"Failed to retrieve tarifline data with parameters : 'period' : {period}, 'products' : {list_nomenclature_request}. Empty DataFrame returned"
                        )
                else:
                    self.logger.warning(
                        f"Failed to retrieve tarifline data with parameters : 'period' : {period}, 'products' : {list_nomenclature_request}. No DataFrame returned"
                    )
