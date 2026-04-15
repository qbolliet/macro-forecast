"""Eurostat data client.

This module provides a high-level client for querying Eurostat data through
their SDMX API and converting responses to pandas DataFrames. Both SDMX 3.0
(primary) and SDMX 2.1 API versions are supported.
"""
# Importation des modules
# Modules de base
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
import gzip
from io import StringIO
import itertools
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
import xml.etree.ElementTree as ET

import pandas as pd

# Utilitaires internes au package pour la requête de données au format SDMX
from ..core.client import APIClient
from ..core.structures import (
    DataflowStructure,
    DataflowStructureRegistry,
    DimensionInfo,
)
from ..core.rate_limiter import RateLimiter

# Initialisation du logger
logger = logging.getLogger(__name__)

# Identifiant de l'agence Eurostat
AGENCY_ID = "ESTAT"


# ──────────────────────────────────────────────────────────────────────
# Types et énumérations
# ──────────────────────────────────────────────────────────────────────

# Type pour la gestion des doublons
DuplicateHandling = Literal["ignore", "warn", "raise"]

# Types pour les paramètres de requêtes de structure SDMX
# Détails des structures
StructureDetail = Literal[
    "full",
    "allstubs",
    "referencestubs",
    "allcompletestubs",
    "referencecompletestubs",
    "referencepartial",
]
# Références des structures
StructureReferences = Literal[
    "none",
    "parents",
    "parentsandsiblings",
    "ancestors",
    "children",
    "descendants",
    "all",
]
# Compression des structures
StructureCompress = Literal["true", "false"]
# Type de détail des données
DataDetail = Literal[
    "full",
    "dataonly",
    "serieskeysonly",
    "nodata"
]

# Énumération des versions d'API SDMX Eurostat supportées
class EurostatAPIVersion(str, Enum):
    """Supported Eurostat SDMX API versions.

    Attributes:
        V3_0: SDMX 3.0 API (primary, recommended).
        V2_1: SDMX 2.1 API (legacy, kept for compatibility testing).
    """

    V3_0 = "3.0"
    V2_1 = "2.1"


# Énumération des types d'artefacts structurels SDMX interrogeables
class StructureResourceType(str, Enum):
    """SDMX structure resource types queryable via ``get_structure``.

    Attributes:
        DATAFLOW: Dataflow definition.
        DATASTRUCTURE: Data Structure Definition (DSD).
        DATACONSTRAINT: Data constraint (valid dimension combinations).
        CONCEPTSCHEME: Concept scheme.
        CODELIST: Codelist (controlled vocabulary for a dimension).
    """

    DATAFLOW = "dataflow"
    DATASTRUCTURE = "datastructure"
    DATACONSTRAINT = "dataconstraint"
    CONCEPTSCHEME = "conceptscheme"
    CODELIST = "codelist"


# Énumération des formats de réponse pour les requêtes de données Eurostat
class EurostatResponseFormat(str, Enum):
    """Response format for Eurostat SDMX data queries.

    Attributes:
        CSV: SDMX-CSV format (default).
        TSV: Tab-separated values format (legacy Eurostat).
        JSON: JSON-stat 2.0 format.
        XML: SDMX-ML XML format.
    """

    CSV = "csv"
    TSV = "tsv"
    JSON = "json"
    XML = "xml"


# ──────────────────────────────────────────────────────────────────────
# Endpoint builders (stratégie par version d'API)
# ──────────────────────────────────────────────────────────────────────


# Classe abstraite de construction d'endpoints et de paramètres par version d'API
class EndpointBuilder(ABC):
    """Abstract base for version-specific URL and parameter construction.

    Subclasses implement the endpoint layout and query-parameter conventions
    for a given SDMX API version.

    All method signatures span the **union** of parameters supported by
    SDMX 3.0 and SDMX 2.1.  Version-specific parameters that are not
    applicable to a given subclass are silently ignored by that subclass.
    Callers should always use keyword arguments so that optional
    version-specific parameters can be forwarded transparently.
    """

    # ── Headers ───────────────────────────────────────────────────────
    # Méthode abstraite de construction des headers HTTP
    @abstractmethod
    def build_headers(
        self,
        accept_encoding: Optional[str] = None,
        accept_language: Optional[str] = None,
    ) -> Dict[str, str]:
        """Build HTTP request headers.

        Args:
            accept_encoding: Value for the ``Accept-Encoding`` header.
            accept_language: Value for the ``Accept-Language`` header.

        Returns:
            HTTP headers dictionary including at minimum the ``Accept``
            header for the SDMX version handled by this builder.
        """

    # ── Data endpoints ────────────────────────────────────────────────
    # Méthode abstraite de construction du endpoint de téléchargement des données
    @abstractmethod
    def build_data_endpoint(
        self,
        dataflow: str,
        agency: str,
        version: str,
        key: Optional[str] = None,
    ) -> str:
        """Build the URL path for a data query.

        Args:
            dataflow: Dataflow identifier.
            agency: Maintaining agency.
            version: Dataflow version.
            key: Optional positional key for dimension filtering.
                Used by SDMX 2.1 (path-based filtering); ignored by 3.0
                when dimension filters are passed as query parameters.

        Returns:
            URL path segment (without base URL).
        """

    # Méthode abstraite de construction des paramètres de téléchargement des données
    @abstractmethod
    def build_data_params(
        self,
        *,
        # Commun aux deux versions
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        first_n_observations: Optional[int] = None,
        compress: bool = False,
        # Spécifique SDMX 3.0
        dimensions: Optional[Dict[str, List[str]]] = None,
        response_format: Optional["EurostatResponseFormat"] = None,
        response_format_version: Optional[str] = None,
        lang: Optional[str] = None,
        labels: Optional[str] = None,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        return_data: Optional[str] = None,
        # Spécifique SDMX 2.1
        dimension_at_observation: Optional[str] = None,
        detail: Optional["DataDetail"] = None,
    ) -> Dict[str, str]:
        """Build query parameters for a data request.

        All parameters are keyword-only to support transparent forwarding
        across versions without relying on positional ordering.

        Args:
            start_period: Start period filter (ISO / SDMX format).
            end_period: End period filter.
            last_n_observations: Number of most-recent observations to return.
            first_n_observations: Number of first observations to return.
            compress: Whether to request gzip compression.
            dimensions: Normalised dimension filters as
                ``{dim_name: [value, ...]}``.
                **SDMX 3.0 only** — ignored by the 2.1 builder.
            response_format: Desired response format enum value.
                **SDMX 3.0 only** — ignored by the 2.1 builder.
            response_format_version: Format version string (e.g. ``"1.0"``).
                **SDMX 3.0 only** — ignored by the 2.1 builder.
            lang: Language code for label localisation (e.g. ``"en"``).
                **SDMX 3.0 only** — ignored by the 2.1 builder.
            labels: Label display mode (e.g. ``"name"``).
                **SDMX 3.0 only** — ignored by the 2.1 builder.
            attributes: Attribute selection string (e.g. ``"dsd"``, ``"none"``).
                **SDMX 3.0 only** — ignored by the 2.1 builder.
            measures: Measure selection string.
                **SDMX 3.0 only** — ignored by the 2.1 builder.
            return_data: Return-data flag.
                **SDMX 3.0 only** — ignored by the 2.1 builder.
            dimension_at_observation: Dimension serialised at observation
                level (e.g. ``"AllDimensions"``).
                **SDMX 2.1 only** — ignored by the 3.0 builder.
            detail: Data detail level (e.g. ``"full"``, ``"dataonly"``).
                **SDMX 2.1 only** — ignored by the 3.0 builder.

        Returns:
            Query-parameter dictionary.
        """

    # ── Structure endpoints ───────────────────────────────────────────
    # Méthode abstraite de construction du endpoint de téléchargement de la structure des données
    @abstractmethod
    def build_structure_endpoint(
        self,
        resource_type: "StructureResourceType",
        resource_id: str,
        agency: str,
        version: Optional[str],
    ) -> str:
        """Build the URL path for a structure query.

        The wildcard token ``"*"`` is accepted for both *resource_id* and
        *agency* on all versions; subclasses map it to the
        version-appropriate token (e.g. ``"all"`` for SDMX 2.1).

        Args:
            resource_type: Type of structure artefact.
            resource_id: Artefact identifier, or ``"*"`` for all artefacts.
            agency: Maintaining agency, or ``"*"`` for all agencies.
            version: Artefact version.  Use ``"+"`` for the latest version,
                ``"*"`` for all versions, ``"~"`` for the SDMX 3.0
                latest-per-resource wildcard, or ``None`` to omit the version
                segment entirely (required for the Dataset listing special case).

        Returns:
            URL path segment (without base URL).
        """

    # Méthode abstraite de construction des paramètres de téléchargement de la structure des données
    @abstractmethod
    def build_structure_params(
        self,
        references: Optional[StructureReferences] = "none",
        detail: Optional[StructureDetail] = "full",
        # Spécifique SDMX 3.0
        format: Optional[str] = None,
        format_version: Optional[str] = None,
        compress: Optional[StructureCompress] = None,
    ) -> Dict[str, str]:
        """Build query parameters for a structure request.

        Args:
            references: Related artefacts to include (default: ``"none"``).
            detail: Level of detail (default: ``"full"``).
            format: Response format string (e.g. ``"structure"``).
                **SDMX 3.0 only** — ignored by the 2.1 builder.
            format_version: Format version string (e.g. ``"3.0"``).
                **SDMX 3.0 only** — ignored by the 2.1 builder.
            compress: Whether to request gzip compression (``"true"`` /
                ``"false"``).
                **SDMX 3.0 only** — ignored by the 2.1 builder.

        Returns:
            Query-parameter dictionary.
        """


# Constructeur d'endpoints pour l'API SDMX 3.0 (version principale)
class EndpointBuilderV30(EndpointBuilder):
    """Endpoint builder for the Eurostat SDMX 3.0 API.

    Implements URL construction and query-parameter encoding following the
    SDMX 3.0 conventions used by the Eurostat dissemination endpoint.

    URL patterns:
        data:
            ``/sdmx/3.0/data/dataflow/{agency}/{resource}/{version}[/{key}]``
        structure:
            ``/sdmx/3.0/structure/{type}/{agency}/{resource}/{version}``

    Attributes:
        ACCEPT_HEADER: MIME type sent in the ``Accept`` request header.

    Note:
        Dimension filtering is performed via query parameters
        (``c[DIM]=val1,val2``) rather than the URL path key, which is the
        primary difference from the SDMX 2.1 approach.
    """

    # Adresse de base du header
    ACCEPT_HEADER = "application/vnd.sdmx.structure+xml;version=3.0.0"

    # Mapping des formats de réponse vers les valeurs de paramètre API
    _FORMAT_PARAM: Dict[EurostatResponseFormat, str] = {
        EurostatResponseFormat.CSV: "csvdata",
        EurostatResponseFormat.TSV: "tsv",
        EurostatResponseFormat.JSON: "json",
        EurostatResponseFormat.XML: "structurespecificdata",
    }

    # Construction des headers pour SDMX 3.0
    def build_headers(
        self,
        accept_encoding: Optional[str] = None,
        accept_language: Optional[str] = None,
    ) -> Dict[str, str]:
        """Build HTTP request headers for SDMX 3.0.

        Args:
            accept_encoding: Value for the ``Accept-Encoding`` header
                (e.g. ``"gzip"``).
            accept_language: Value for the ``Accept-Language`` header
                (e.g. ``"en"``).

        Returns:
            HTTP headers dictionary with at minimum the ``Accept`` header
            set to the SDMX 3.0 structure MIME type.
        """
        # Initialisation du dictionnaire des headers
        headers = {"Accept": self.ACCEPT_HEADER}
        # Ajout des clés si spécifiées
        if accept_encoding is not None:
            headers["Accept-Encoding"] = accept_encoding
        if accept_language is not None:
            headers["Accept-Language"] = accept_language
        return headers

    # Construction de l'endpoint de données SDMX 3.0
    def build_data_endpoint(
        self,
        dataflow: str,
        agency: str,
        version: str,
        key: Optional[str] = None,
    ) -> str:
        """Build the URL path for an SDMX 3.0 data query.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx3-0/data-query#APIDetailedguidelinesSDMX3.0APIdataquery-Overview

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%203.0%20Data%20queries/get_sdmx_3_0_data_dataflow__agencyID___resourceID___version___key_

        Args:
            dataflow: Dataflow identifier (e.g. ``"namq_10_gdp"``).
            agency: Maintaining agency (e.g. ``"ESTAT"``).
            version: Dataflow version.  Use ``"*"`` for the latest version.
            key: Optional positional key for dimension filtering.
                When provided, appended as a trailing path segment.
                Not commonly used in SDMX 3.0 (prefer query-parameter
                filtering via :meth:`build_data_params`).

        Returns:
            URL path segment (without base URL).
        """
        # Construction du path de base
        path = f"/sdmx/3.0/data/dataflow/{agency}/{dataflow}/{version}"
        # Ajout de la clé si spécifiée
        if key is not None:
            path += f"/{key}"
        return path

    # Construction des paramètres de requête de données SDMX 3.0
    def build_data_params(
        self,
        *,
        # Commun aux deux versions
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        first_n_observations: Optional[int] = None,
        compress: bool = False,
        # Spécifique SDMX 3.0
        dimensions: Optional[Dict[str, List[str]]] = None,
        response_format: Optional[EurostatResponseFormat] = None,
        response_format_version: Optional[str] = None,
        lang: Optional[str] = None,
        labels: Optional[str] = None,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        return_data: Optional[str] = None,
        # Spécifique SDMX 2.1 (ignoré par ce builder)
        dimension_at_observation: Optional[str] = None,
        detail: Optional[DataDetail] = None,
    ) -> Dict[str, str]:
        """Build query parameters for an SDMX 3.0 data request.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx3-0/data-query#APIDetailedguidelinesSDMX3.0APIdataquery-Overview

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%203.0%20Data%20queries/get_sdmx_3_0_data_dataflow__agencyID___resourceID___version___key_

        Args:
            start_period: Start period filter encoded as
                ``ge:<value>`` in ``c[TIME_PERIOD]``.
            end_period: End period filter encoded as
                ``le:<value>`` in ``c[TIME_PERIOD]``.
            last_n_observations: Number of most-recent observations.
            first_n_observations: Number of first observations.
            compress: Whether to request gzip compression via the
                ``compress`` query parameter.
            dimensions: Dimension filters as ``{name: [values]}``.
                Encoded as ``c[DIM]=val1,val2`` query parameters.
            response_format: Desired response format.  Mapped to the
                ``format`` query parameter via :attr:`_FORMAT_PARAM`.
            response_format_version: Format version string (``formatVersion``
                parameter, e.g. ``"1.0"`` for SDMX-CSV 1.0).
            lang: Language code for label localisation (``lang`` parameter,
                e.g. ``"en"``).
            labels: Label display mode (``labels`` parameter,
                e.g. ``"name"``).
            attributes: Attribute selection string (``attributes``
                parameter, e.g. ``"dsd"``, ``"none"``).
            measures: Measure selection string (``measures`` parameter).
            return_data: Return-data flag (``returnData`` parameter).
            dimension_at_observation: Ignored — SDMX 2.1 only.
            detail: Ignored — SDMX 2.1 only.

        Returns:
            Query-parameter dictionary suitable for use as ``params`` in
            an HTTP GET request.
        """
        # Initialisation du dictionnaire de paramètres
        params: Dict[str, str] = {}

        # Filtres de dimensions (c[DIM]=val1,val2)
        if dimensions:
            for dim_name, dim_values in dimensions.items():
                params[f"c[{dim_name.upper()}]"] = ",".join(dim_values)

        # Filtre de période temporelle (c[TIME_PERIOD]=ge:...+le:...)
        time_parts: list[str] = []
        if start_period:
            time_parts.append(f"ge:{start_period}")
        if end_period:
            time_parts.append(f"le:{end_period}")
        if time_parts:
            params["c[TIME_PERIOD]"] = "+".join(time_parts)

        # Paramètres d'observations
        if last_n_observations is not None:
            params["lastNObservations"] = str(last_n_observations)
        if first_n_observations is not None:
            params["firstNObservations"] = str(first_n_observations)

        # Attributs et mesures optionnels
        if attributes:
            params["attributes"] = attributes
        if measures:
            params["measures"] = measures

        # Format et version de format
        if response_format:
            params["format"] = self._FORMAT_PARAM[response_format]
        if response_format_version:
            params["formatVersion"] = response_format_version

        # Langue
        if lang:
            params["lang"] = lang

        # Labels
        if labels is not None:
            params["labels"] = labels

        # Compression
        params["compress"] = "true" if compress else "false"

        # Données
        if return_data:
            params["returnData"] = return_data

        return params

    # Construction de l'endpoint de structure SDMX 3.0
    def build_structure_endpoint(
        self,
        resource_type: StructureResourceType,
        resource_id: str,
        agency: str,
        version: Optional[str],
    ) -> str:
        """Build the URL path for an SDMX 3.0 structure query.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx3-0/structure-queries

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%203.0%20Structure%20queries/get_sdmx_3_0_structure_dataflow__agencyID___resourceID_

        Args:
            resource_type: Type of structure artefact (e.g.
                :attr:`StructureResourceType.DATAFLOW`).
            resource_id: Artefact identifier, or ``"*"`` for all artefacts
                of the given type.
            agency: Maintaining agency, or ``"*"`` for all agencies.
            version: Artefact version.  Use ``"+"`` or ``"~"`` for the
                latest version, ``"*"`` for all versions, or ``None`` to omit
                the version segment (Dataset listing special case).

        Returns:
            URL path segment (without base URL).
        """
        # Base path sans version
        path = f"/sdmx/3.0/structure/{resource_type.value}/{agency}/{resource_id}"
        # Ajout du segment de version uniquement si spécifié
        if version is not None:
            path += f"/{version}"
        return path

    # Construction des paramètres de requête de structure SDMX 3.0
    def build_structure_params(
        self,
        references: Optional[StructureReferences] = "none",
        detail: Optional[StructureDetail] = "full",
        format: Optional[str] = "structure",
        format_version: Optional[str] = "3.0",
        compress: Optional[StructureCompress] = "true",
    ) -> Dict[str, str]:
        """Build query parameters for an SDMX 3.0 structure request.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx3-0/structure-queries

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%203.0%20Structure%20queries/get_sdmx_3_0_structure_dataflow__agencyID___resourceID_

        Args:
            references: Related artefacts to embed in the response
                (default: ``"none"``).
            detail: Level of detail for each returned artefact
                (default: ``"full"``).
            format: Response format identifier (default: ``"structure"``).
            format_version: Version of the response format
                (default: ``"3.0"``).
            compress: Whether to request gzip compression of the response.
                Defaults to ``"true"`` because structure responses can be
                large; the client transparently decompresses the result.

        Returns:
            Query-parameter dictionary.
        """
        # Initialisation du dictionnaire des paramètres
        params: Dict[str, str] = {}

        # Ajout des clés quand elles sont non nulles
        if references is not None:
            params["references"] = references
        if detail is not None:
            params["detail"] = detail
        if format is not None:
            params["format"] = format
        if format_version is not None:
            params["formatVersion"] = format_version
        if compress is not None:
            params["compress"] = compress

        return params


# Constructeur d'endpoints pour l'API SDMX 2.1 (version legacy)
class EndpointBuilderV21(EndpointBuilder):
    """Endpoint builder for the Eurostat SDMX 2.1 API.

    Implements URL construction and query-parameter encoding following the
    SDMX 2.1 conventions.  This builder is kept for compatibility testing
    and for accessing Comext datasets that are not yet available on the
    SDMX 3.0 endpoint.

    URL patterns:
        data:
            ``/sdmx/2.1/data/{flow}[,{agency}[,{version}]]/{key}``
        structure:
            ``/sdmx/2.1/{type}/{agency}/{resource}/{version}``

    Attributes:
        ACCEPT_HEADER: MIME type sent in the ``Accept`` request header.

    Note:
        Dimension filtering uses a **positional key** embedded in the URL
        path (``/key`` segment), not query parameters.  The ``dimensions``
        and ``response_format`` parameters of :meth:`build_data_params`
        are accepted for interface compatibility but silently ignored.

        The wildcard tokens differ from SDMX 3.0:

        - ``"*"`` (all versions / all resources) → ``"all"`` in paths
        - ``"+"`` (latest version) → ``"latest"`` in paths
        - ``"dataconstraint"`` resource type → ``"contentconstraint"``
    """

    # Adresse de base du header
    ACCEPT_HEADER = "application/vnd.sdmx.structure+xml;version=2.1"

    # Mapping des formats de réponse vers les valeurs de paramètre API 2.1
    _FORMAT_PARAM: Dict[EurostatResponseFormat, str] = {
        EurostatResponseFormat.CSV: "SDMX-CSV",
        EurostatResponseFormat.TSV: "TSV",
        EurostatResponseFormat.JSON: "JSON",
        EurostatResponseFormat.XML: "SDMX_2.1_STRUCTURED",
    }

    # Mapping des types de structure 3.0 vers les types 2.1
    _RESOURCE_MAP: Dict[StructureResourceType, str] = {
        StructureResourceType.DATAFLOW: "dataflow",
        StructureResourceType.DATASTRUCTURE: "datastructure",
        StructureResourceType.DATACONSTRAINT: "contentconstraint",
        StructureResourceType.CONCEPTSCHEME: "conceptscheme",
        StructureResourceType.CODELIST: "codelist",
    }

    # Construction des headers pour SDMX 2.1
    def build_headers(
        self,
        accept_encoding: Optional[str] = None,
        accept_language: Optional[str] = None,
    ) -> Dict[str, str]:
        """Build HTTP request headers for SDMX 2.1.

        Args:
            accept_encoding: Value for the ``Accept-Encoding`` header
                (e.g. ``"gzip"``).
            accept_language: Value for the ``Accept-Language`` header
                (e.g. ``"en"``).

        Returns:
            HTTP headers dictionary with at minimum the ``Accept`` header
            set to the SDMX 2.1 structure MIME type.
        """
        # Initialisation du dictionnaire des headers
        headers = {"Accept": self.ACCEPT_HEADER}
        # Ajout des clés si spécifiées
        if accept_encoding is not None:
            headers["Accept-Encoding"] = accept_encoding
        if accept_language is not None:
            headers["Accept-Language"] = accept_language
        return headers

    # Construction de l'endpoint de données SDMX 2.1
    def build_data_endpoint(
        self,
        dataflow: str,
        agency: Optional[str],
        version: Optional[str],
        key: Optional[str] = "all",
    ) -> str:
        """Build the URL path for an SDMX 2.1 data query.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx2-1/data-query

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%202.1%20Data%20queries/get_sdmx_2_1_data__flow___key_

        In SDMX 2.1 the *flow* path parameter combines agency, dataflow ID
        and version in a single compound token:

        Examples::

            EXR               → dataflow ID only
            ECB,EXR           → agency + dataflow ID
            ECB,EXR,1.0       → agency + dataflow ID + version

        Args:
            dataflow: Dataflow identifier (e.g. ``"namq_10_gdp"``).
            agency: Maintaining agency.  When provided, prepended to the
                flow token (e.g. ``"ESTAT"``).
            version: Dataflow version.  When provided, appended to the
                flow token.
            key: Positional key for dimension filtering (default:
                ``"all"`` — no filtering).  Individual dimension values
                are separated by ``"."`` and multiple values within a
                dimension by ``"+"``.

        Returns:
            URL path segment (without base URL).
        """
        # Construction du flow composé
        flow = dataflow
        # Ajout de l'agency si précisé
        if agency is not None:
            flow = f"{agency}," + flow
        # Ajout de la version si spécifiée
        if version is not None:
            flow = flow + f",{version}"
        return f"/sdmx/2.1/data/{flow}/{key}"

    # Construction des paramètres de requête de données SDMX 2.1
    def build_data_params(
        self,
        *,
        # Commun aux deux versions
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        last_n_observations: Optional[int] = None,
        first_n_observations: Optional[int] = None,
        compress: bool = False,
        # Spécifique SDMX 3.0 (ignoré par ce builder)
        dimensions: Optional[Dict[str, List[str]]] = None,
        response_format: Optional[EurostatResponseFormat] = None,
        response_format_version: Optional[str] = None,
        lang: Optional[str] = None,
        labels: Optional[str] = None,
        attributes: Optional[str] = None,
        measures: Optional[str] = None,
        return_data: Optional[str] = None,
        # Spécifique SDMX 2.1
        dimension_at_observation: Optional[str] = None,
        detail: Optional[DataDetail] = None,
    ) -> Dict[str, str]:
        """Build query parameters for an SDMX 2.1 data request.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx2-1/data-query

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%202.1%20Data%20queries/get_sdmx_2_1_data__flow___key_

        Args:
            start_period: Start period filter (``startPeriod`` parameter).
            end_period: End period filter (``endPeriod`` parameter).
            last_n_observations: Number of most-recent observations
                (``lastNObservations`` parameter).
            first_n_observations: Number of first observations
                (``firstNObservations`` parameter).
            compress: Whether to request gzip compression
                (``compressed`` parameter).
            dimensions: Ignored — SDMX 3.0 only (use ``key`` in
                :meth:`build_data_endpoint` for 2.1 filtering).
            response_format: Ignored — SDMX 3.0 only.
            response_format_version: Ignored — SDMX 3.0 only.
            lang: Ignored — SDMX 3.0 only.
            labels: Ignored — SDMX 3.0 only.
            attributes: Ignored — SDMX 3.0 only.
            measures: Ignored — SDMX 3.0 only.
            return_data: Ignored — SDMX 3.0 only.
            dimension_at_observation: Dimension serialised at observation
                level (``dimensionAtObservation`` parameter,
                e.g. ``"AllDimensions"``).
            detail: Data detail level (``detail`` parameter,
                e.g. ``"full"``, ``"dataonly"``).

        Returns:
            Query-parameter dictionary.
        """
        # Initialisation du dictionnaire de paramètres
        params: Dict[str, str] = {}

        # Période temporelle (startPeriod / endPeriod)
        if start_period:
            params["startPeriod"] = start_period
        if end_period:
            params["endPeriod"] = end_period

        # Paramètres d'observations
        if first_n_observations is not None:
            params["firstNObservations"] = str(first_n_observations)
        if last_n_observations is not None:
            params["lastNObservations"] = str(last_n_observations)

        # Paramètres spécifiques 2.1
        if dimension_at_observation:
            params["dimensionAtObservation"] = dimension_at_observation
        if detail:
            params["detail"] = detail

        # Compression (paramètre 2.1 : "compressed")
        params["compressed"] = "true" if compress else "false"

        return params

    # Construction de l'endpoint de structure SDMX 2.1
    def build_structure_endpoint(
        self,
        resource_type: StructureResourceType,
        resource_id: str,
        agency: str,
        version: Optional[str],
    ) -> str:
        """Build the URL path for an SDMX 2.1 structure query.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx2-1/structure-queries#APIDetailedguidelinesSDMX2.1APIstructurequeries-Multiplevaluesandwildcardvaluesupport

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%202.1%20Structure%20queries/get_sdmx_2_1_dataflow__agencyID___resourceID___version_

        Wildcard token mapping (SDMX 3.0 → SDMX 2.1):

        - resource_id ``"*"`` → ``"all"``
        - agency ``"*"`` → ``"all"``
        - version ``"+"`` or ``"~"`` or ``None`` → ``"latest"``
        - version ``"*"`` → ``"all"``

        Args:
            resource_type: Type of structure artefact.  ``DATACONSTRAINT``
                is mapped to ``contentconstraint``.
            resource_id: Artefact identifier, or ``"*"`` for all artefacts
                (mapped to ``"all"``).
            agency: Maintaining agency, or ``"*"`` for all agencies
                (mapped to ``"all"``).
            version: Artefact version.  Use ``"+"`` or ``"~"`` for the
                latest version; ``"*"`` for all versions; ``None`` defaults
                to ``"latest"``.

        Returns:
            URL path segment (without base URL).
        """
        # Conversion du type de ressource vers la terminologie 2.1
        mapped_type = self._RESOURCE_MAP[resource_type]

        # Conversion des tokens de version vers les équivalents 2.1 (None → "latest")
        v21_version = (
            "latest" if version in ("+", "~", None)
            else ("all" if version == "*" else version)
        )

        # Conversion des wildcards d'agence et de ressource vers 2.1
        v21_agency = "all" if agency == "*" else agency
        v21_resource_id = "all" if resource_id == "*" else resource_id

        return (
            f"/sdmx/2.1/{mapped_type}"
            f"/{v21_agency}/{v21_resource_id}/{v21_version}"
        )

    # Construction des paramètres de requête de structure SDMX 2.1
    def build_structure_params(
        self,
        references: Optional[StructureReferences] = "none",
        detail: Optional[StructureDetail] = "full",
        # Spécifique SDMX 3.0 (ignoré par ce builder)
        format: Optional[str] = None,
        format_version: Optional[str] = None,
        compress: Optional[StructureCompress] = None,
    ) -> Dict[str, str]:
        """Build query parameters for an SDMX 2.1 structure request.

        Full documentation:
            https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/sdmx2-1/structure-queries#APIDetailedguidelinesSDMX2.1APIstructurequeries-Multiplevaluesandwildcardvaluesupport

        Swagger UI:
            https://ec.europa.eu/eurostat/api/dissemination/swagger-ui#/SDMX%202.1%20Structure%20queries/get_sdmx_2_1_dataflow__agencyID___resourceID___version_

        Args:
            references: Related artefacts to embed in the response
                (default: ``"none"``).
            detail: Level of detail for each returned artefact
                (default: ``"full"``).
            format: Ignored — SDMX 3.0 only.
            format_version: Ignored — SDMX 3.0 only.
            compress: Ignored — SDMX 2.1 does not support this parameter.

        Returns:
            Query-parameter dictionary.
        """
        # Initialisation du dictionnaire de paramètres
        params: Dict[str, str] = {}
        # Ajout des clés supportées par l'API 2.1
        if detail is not None:
            params["detail"] = detail
        if references is not None:
            params["references"] = references
        return params


# Registre des builders par version d'API
_ENDPOINT_BUILDERS: Dict[EurostatAPIVersion, EndpointBuilder] = {
    EurostatAPIVersion.V3_0: EndpointBuilderV30(),
    EurostatAPIVersion.V2_1: EndpointBuilderV21(),
}


# ──────────────────────────────────────────────────────────────────────
# Dataclass de requête
# ──────────────────────────────────────────────────────────────────────


# Classe de base contenant les paramètres communs aux deux versions d'API
@dataclass
class BaseEurostatQueryRequest:
    """Base class for Eurostat query requests.

    Contains all parameters shared by both SDMX 3.0 and 2.1 API versions.
    Use :class:`EurostatQueryRequestV30` or :class:`EurostatQueryRequestV21`
    directly to benefit from version-specific parameter typing.

    Attributes:
        dataflow: Dataflow identifier (e.g., ``"namq_10_gdp"``,
            ``"DS-045409"``).
        version: Dataflow version (default: ``"*"`` for latest).
        dimensions: Dimension filters as ``{name: value_or_list}``.
        start_period: Start period filter (ISO / SDMX format).
        end_period: End period filter.
        last_n_observations: Number of most-recent observations to return.
        first_n_observations: Number of first observations to return.
        format: Response format (default: CSV).
        compress: Whether to request gzip compression of the response.
        on_duplicate: Duplicate handling strategy.
        split_dimensions: Dimensions to split into separate sub-requests.
        max_split_combinations: Maximum allowed split combinations.
    """
    # Attributs communs aux deux versions
    dataflow: str
    version: str = "*"
    dimensions: Optional[Dict[str, Union[str, List[str]]]] = None
    start_period: Optional[str] = None
    end_period: Optional[str] = None
    last_n_observations: Optional[int] = None
    first_n_observations: Optional[int] = None
    format: EurostatResponseFormat = EurostatResponseFormat.CSV
    compress: bool = False
    on_duplicate: DuplicateHandling = "warn"
    split_dimensions: Optional[List[str]] = None
    max_split_combinations: int = 100

    # Méthode de conversion des paramètres communs en dictionnaire de kwargs
    def _base_dict(self) -> Dict[str, Any]:
        """Return the common parameters as a dictionary.

        Returns:
            Dictionary of parameters shared by both API versions.
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
            "on_duplicate": self.on_duplicate,
            "split_dimensions": self.split_dimensions,
            "max_split_combinations": self.max_split_combinations,
        }

    # Méthode d'extraction de la clé unique associée au dataflow
    def get_dataflow_key(self) -> str:
        """Get unique key for this dataflow.

        Returns:
            Key in format ``'dataflow::version'``.
        """
        return f"{self.dataflow}::{self.version}"


# Classe représentant une requête de données Eurostat via l'API SDMX 3.0
@dataclass
class EurostatQueryRequestV30(BaseEurostatQueryRequest):
    """Query request for the Eurostat SDMX 3.0 API.

    Extends :class:`BaseEurostatQueryRequest` with parameters specific to
    the SDMX 3.0 endpoint. Use this class when the client is configured with
    ``api_version=EurostatAPIVersion.V3_0`` (the default).

    Attributes:
        attributes: Attribute selection string (e.g., ``"dataStructure"``).
        measures: Measure selection string (e.g., ``"OBS_VALUE"``).
        lang: Language code for label localisation (e.g., ``"en"``,
            ``"fr"``).
        labels: Label display mode (e.g., ``"name"``, ``"id"``).
        response_format_version: Format version string (e.g., ``"1.0"``).

    Example:
        >>> query = EurostatQueryRequestV30(
        ...     dataflow="namq_10_gdp",
        ...     dimensions={"GEO": ["FR", "DE"], "FREQ": "Q"},
        ...     lang="fr",
        ... )
        >>> df = client.execute_query(query)
    """
    # Attributs spécifiques SDMX 3.0
    attributes: Optional[str] = None
    measures: Optional[str] = None
    lang: Optional[str] = None
    labels: Optional[str] = None
    response_format_version: Optional[str] = None

    # Méthode de conversion des paramètres en dictionnaire de kwargs
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for ``get_data()`` kwargs.

        Returns:
            Dictionary of all parameters. SDMX 2.1-specific fields are
            included as ``None`` for compatibility with ``get_data()``.
        """
        return {
            **self._base_dict(),
            "attributes": self.attributes,
            "measures": self.measures,
            "lang": self.lang,
            "labels": self.labels,
            "response_format_version": self.response_format_version,
        }


# Classe représentant une requête de données Eurostat via l'API SDMX 2.1
@dataclass
class EurostatQueryRequestV21(BaseEurostatQueryRequest):
    """Query request for the Eurostat SDMX 2.1 API (legacy).

    Extends :class:`BaseEurostatQueryRequest` with parameters specific to
    the SDMX 2.1 endpoint. Use this class when the client is configured with
    ``api_version=EurostatAPIVersion.V2_1``.

    Attributes:
        dimension_at_observation: Dimension serialised at observation
            level (e.g., ``"AllDimensions"`` for flat output,
            ``"TIME_PERIOD"`` for time series).
        detail: Data detail level (e.g., ``"dataonly"``,
            ``"serieskeysonly"``).

    Example:
        >>> query = EurostatQueryRequestV21(
        ...     dataflow="namq_10_gdp",
        ...     dimensions={"GEO": ["FR", "DE"], "FREQ": "Q"},
        ...     detail="dataonly",
        ... )
        >>> df = client_v21.execute_query(query)
    """
    # Attributs spécifiques SDMX 2.1
    dimension_at_observation: Optional[str] = None
    detail: Optional[DataDetail] = None

    # Méthode de conversion des paramètres en dictionnaire de kwargs
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for ``get_data()`` kwargs.

        Returns:
            Dictionary of all parameters. SDMX 3.0-specific fields are
            included as ``None`` for compatibility with ``get_data()``.
        """
        return {
            **self._base_dict(),
            "dimension_at_observation": self.dimension_at_observation,
            "detail": self.detail,
        }


# ──────────────────────────────────────────────────────────────────────
# Client principal
# ──────────────────────────────────────────────────────────────────────


# Initialisation du client haut niveau pour l'API SDMX Eurostat
class EurostatClient:
    """High-level client for the Eurostat SDMX API.

    Supports SDMX 3.0 (default) and SDMX 2.1 API versions. The API version
    can be switched at construction time, and the client automatically routes
    Comext datasets (``DS-*`` prefix) to the dedicated Comext endpoint.

    Args:
        api_version: SDMX API version to use (default: 3.0).
        base_url: Override for the main API base URL. When *None* the
            standard Eurostat dissemination endpoint matching ``api_version``
            is used.
        timeout: Request timeout in seconds.
        structure_registry: Optional registry for dimension-name resolution.
        auto_fetch_structure: If *True*, fetch structure metadata on demand.
        rate_limiter: Optional rate limiter for API requests.
        auto_load_rate_limit: If *True*, load rate limiter from
            ``parameters/eurostat.json``.

    Example:
        >>> client = EurostatClient()
        >>> df = client.get_data(
        ...     dataflow="namq_10_gdp",
        ...     dimensions={"GEO": ["FR"], "FREQ": "Q"},
        ... )
    """

    # URLs de base par défaut pour chaque version d'API
    _DEFAULT_BASE_URLS: Dict[EurostatAPIVersion, str] = {
        EurostatAPIVersion.V3_0: "https://ec.europa.eu/eurostat/api/dissemination",
        EurostatAPIVersion.V2_1: "https://ec.europa.eu/eurostat/api/dissemination",
    }

    # URLs Comext par version d'API (pour les datasets DS-*)
    _COMEXT_BASE_URLS: Dict[EurostatAPIVersion, str] = {
        EurostatAPIVersion.V3_0: "https://ec.europa.eu/eurostat/api/comext/dissemination",
        EurostatAPIVersion.V2_1: "https://ec.europa.eu/eurostat/api/comext/dissemination",
    }

    # Namespaces XML SDMX 3.0
    _SDMX3_NS = {
        "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/message",
        "str": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/structure",
        "com": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/common",
    }

    # Namespaces XML SDMX 2.1 (fallback)
    _SDMX21_NS = {
        "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
        "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
        "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
    }

    # Initialisation
    def __init__(
        self,
        api_version: EurostatAPIVersion = EurostatAPIVersion.V3_0,
        base_url: Optional[str] = None,
        timeout: int = 90,
        structure_registry: Optional[DataflowStructureRegistry] = None,
        auto_fetch_structure: bool = True,
        rate_limiter: Optional[RateLimiter] = None,
        auto_load_rate_limit: bool = True,
    ):
        # Version d'API et builder d'endpoints associé
        self.api_version = api_version
        self.endpoint_builder: EndpointBuilder = _ENDPOINT_BUILDERS[api_version]

        # URL de base (résolution par défaut selon la version)
        self.base_url = base_url or self._DEFAULT_BASE_URLS[api_version]
        self.auto_fetch_structure = auto_fetch_structure

        # Client HTTP principal
        self.api_client = APIClient(base_url=self.base_url, timeout=timeout)
        self._timeout = timeout

        # Client HTTP Comext (initialisation paresseuse)
        self._comext_client: Optional[APIClient] = None

        # Registre des structures de dataflows
        self.structure_registry = structure_registry or DataflowStructureRegistry()

        # Chargement automatique du rate limiter si demandé
        if auto_load_rate_limit and rate_limiter is None:
            rate_limiter = self._load_rate_limiter()
        self.rate_limiter = rate_limiter

    # ──────────────────────────────────────────────────────────────────
    # Méthodes publiques — Données
    # ──────────────────────────────────────────────────────────────────

    # Méthode publique principale de récupération de données
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
        lang: Optional[str] = None,
        labels: Optional[str] = None,
        response_format_version: Optional[str] = None,
        dimension_at_observation: Optional[str] = None,
        detail: Optional[DataDetail] = None,
        on_duplicate: DuplicateHandling = "warn",
        split_dimensions: Optional[List[str]] = None,
        max_split_combinations: int = 100,
    ) -> pd.DataFrame:
        """Retrieve data from Eurostat.

        The URL key and query parameters are built automatically from the
        dataflow structure (loaded from the registry or fetched on demand):

        - **SDMX 3.0**: dimensions with a single value are embedded in the
          positional URL key; dimensions with multiple values are passed as
          ``c[DIM]=val1,val2`` query parameters (server-side filtering,
          no client-side post-filter required).
        - **SDMX 2.1**: all dimensions are embedded in the positional URL
          key using ``val1+val2`` for multi-value positions.

        When no structure is available the method falls back to passing all
        dimensions as query parameters for SDMX 3.0 (existing behaviour) or
        using the ``"all"`` wildcard key for SDMX 2.1.

        Args:
            dataflow: Dataflow identifier (e.g., ``"namq_10_gdp"``).
            version: Dataflow version (``"*"`` for latest).
            dimensions: Dimension filters as ``{name: value_or_list}``.
            start_period: Start period in SDMX format (e.g., ``"2020-Q1"``).
            end_period: End period in SDMX format.
            last_n_observations: Number of most-recent observations.
            first_n_observations: Number of first observations.
            format: Response format (default: CSV).
            compress: Whether to request gzip compression.
            attributes: Attributes to include (SDMX 3.0 only).
            measures: Measures to include (SDMX 3.0 only).
            lang: Language code for label localisation (SDMX 3.0 only).
            labels: Label display mode (SDMX 3.0 only).
            response_format_version: Format version string (SDMX 3.0 only).
            dimension_at_observation: Dimension at observation level
                (SDMX 2.1 only).
            detail: Data detail level (SDMX 2.1 only).
            on_duplicate: Duplicate handling strategy.
            split_dimensions: Dimension names for which each value triggers
                a separate API request.  Use this to control the trade-off
                between response size and number of requests.
            max_split_combinations: Maximum split combinations allowed.

        Returns:
            DataFrame with retrieved data.

        Raises:
            ValueError: If data retrieval fails.

        Examples:
            >>> # SDMX 3.0 — GEO (single value) goes in URL key,
            >>> # unit (multi-value) goes in c[UNIT]=... query param
            >>> df = client.get_data(
            ...     dataflow="namq_10_gdp",
            ...     dimensions={"GEO": "FR", "unit": ["CLV10_MEUR", "CP_MEUR"]},
            ... )
            >>> # SDMX 3.0 — split GEO into separate requests
            >>> df = client.get_data(
            ...     dataflow="namq_10_gdp",
            ...     dimensions={"GEO": ["FR", "DE"]},
            ...     split_dimensions=["GEO"],
            ... )
        """
        # Application du rate limiter avant la requête
        if self.rate_limiter:
            self.rate_limiter.wait()

        # Chargement de la structure (non fatal en cas d'échec)
        structure = None
        try:
            structure = self._ensure_structure(dataflow, version)
        except Exception:
            # Fetch de secours : population du registry même si auto_fetch_structure=False
            try:
                structure = self.get_dataflow_structure(dataflow, version)
                self.register_structure(structure)
            except Exception as e2:
                logger.warning(f"Could not load structure for {dataflow}: {e2}")

        # Normalisation des dimensions (conversion str → List[str], validation des noms)
        normalized_dims = self._normalize_dimensions(dimensions, structure)

        is_v21 = self.api_version == EurostatAPIVersion.V2_1

        # Génération des combinaisons (dims_for_url, dims_for_params) — appel
        # systématique, même sans split_dimensions (retourne une seule combinaison)
        request_combinations = self._generate_request_combinations(
            dimensions=normalized_dims,
            split_dims=split_dimensions,
            max_combinations=max_split_combinations,
            is_v21=is_v21,
            structure=structure,
        )

        # Requêtes multiples (split_dimensions) : délégation directe sans récursion
        if len(request_combinations) > 1:
            return self._execute_split_requests(
                request_combinations=request_combinations,
                structure=structure,
                dataflow=dataflow,
                version=version,
                start_period=start_period,
                end_period=end_period,
                last_n_observations=last_n_observations,
                first_n_observations=first_n_observations,
                response_format=format,
                compress=compress,
                attributes=attributes,
                measures=measures,
                lang=lang,
                labels=labels,
                response_format_version=response_format_version,
                dimension_at_observation=dimension_at_observation,
                detail=detail,
            )

        # Requête unique : extraction du tuple (dims_for_url, dims_for_params)
        dims_for_url, dims_for_params = request_combinations[0]

        # Construction de la clé positionnelle si des dimensions sont destinées au key
        key_str: Optional[str] = None
        if dims_for_url and structure:
            key_str = self._build_key_string(dims_for_url, structure)
        elif is_v21:
            # V21 : clé obligatoire dans le path — fallback à "all" sans structure
            key_str = "all"

        # Construction de l'endpoint et des paramètres via le builder
        endpoint = self.endpoint_builder.build_data_endpoint(
            dataflow=dataflow,
            agency=AGENCY_ID,
            version=version,
            key=key_str,
        )
        params = self.endpoint_builder.build_data_params(
            dimensions=dims_for_params or None,
            start_period=start_period,
            end_period=end_period,
            last_n_observations=last_n_observations,
            first_n_observations=first_n_observations,
            compress=compress,
            response_format=format,
            response_format_version=response_format_version,
            lang=lang,
            labels=labels,
            attributes=attributes,
            measures=measures,
            dimension_at_observation=dimension_at_observation,
            detail=detail,
        )

        # Sélection du client API (standard ou Comext)
        client = self._get_api_client(dataflow)

        # Requête des données et parsing selon le format
        try:
            response = client.get(endpoint, params=params)

            # Décompression si nécessaire (gzip transparent)
            raw_bytes = self._decompress_response_bytes(response.content)

            # Parsing de la réponse selon le format demandé
            if format == EurostatResponseFormat.CSV:
                df = self._parse_csv_response(raw_bytes.decode("utf-8"))
            elif format == EurostatResponseFormat.TSV:
                df = self._parse_tsv_response(raw_bytes.decode("utf-8"))
            elif format == EurostatResponseFormat.JSON:
                df = self._parse_json_response(
                    json.loads(raw_bytes.decode("utf-8"))
                )
            else:
                raise ValueError(f"Unsupported format: {format}")

            # Vérification des doublons dans le DataFrame résultant
            self._check_duplicates(df, normalized_dims, structure, on_duplicate)

            # Post-filtrage en fallback uniquement : sans structure toutes les dims
            # partent en query params server-side mais le serveur peut renvoyer
            # des valeurs plus larges que demandé
            if normalized_dims and not structure:
                df = self._filter_dataframe_by_dimensions(df, normalized_dims)

            logger.info(f"Retrieved {len(df)} rows from {dataflow}")
            return df
        # Gestion des erreurs de requête et de parsing
        except Exception as e:
            logger.error(f"Data retrieval failed: {e}")
            raise ValueError(f"Failed to retrieve data from {dataflow}: {e}")

    # Méthode publique d'exécution d'un objet EurostatQueryRequest
    def execute_query(
        self,
        query: Union[EurostatQueryRequestV30, EurostatQueryRequestV21],
    ) -> pd.DataFrame:
        """Execute a query request.

        Accepts either :class:`EurostatQueryRequestV30` or
        :class:`EurostatQueryRequestV21` depending on the API version the
        client was configured with.

        Args:
            query: Version-specific query request instance.

        Returns:
            DataFrame with retrieved data.

        Raises:
            ValueError: If query execution fails.
        """
        # Délégation à get_data avec les paramètres de la requête
        return self.get_data(**query.to_dict())

    # ──────────────────────────────────────────────────────────────────
    # Méthodes publiques — Structure
    # ──────────────────────────────────────────────────────────────────

    # Méthode publique unifiée de requête d'artefacts structurels SDMX
    def get_structure(
        self,
        resource_type: StructureResourceType,
        resource_id: str,
        agency: str = AGENCY_ID,
        version: Optional[str] = "+",
        references: StructureReferences = "none",
        detail: StructureDetail = "full",
        format: Optional[str] = None,
        format_version: Optional[str] = None,
        compress: Optional[StructureCompress] = None,
        accept_encoding: Optional[str] = None,
        accept_language: Optional[str] = None,
    ) -> str:
        """Query an SDMX structure artefact and return raw XML.

        This is the single entry point for all structure queries. The
        ``resource_type`` parameter selects which endpoint is targeted
        (dataflow, datastructure, dataconstraint, conceptscheme, codelist).

        When the ``compress`` parameter is ``"true"`` (default for SDMX 3.0)
        or when the API returns gzip-compressed content, the response is
        transparently decompressed before being returned.

        Args:
            resource_type: Type of structure artefact to retrieve.
            resource_id: Artefact identifier (e.g., ``"namq_10_gdp"``), or
                ``"*"`` to retrieve all artefacts of the given type.
            agency: Maintaining agency (default: ``AGENCY_ID``), or ``"*"``
                for all agencies.
            version: Artefact version. Use ``"+"`` for latest (``"latest"``
                is used automatically when the 2.1 builder is active), ``"~"``
                for SDMX 3.0 latest-per-resource, ``"*"`` for all versions.
            detail: Level of detail (default: ``"full"``).
            references: Related artefacts to include (default: ``"none"``).
            format: Response format identifier (SDMX 3.0 only,
                default: ``"structure"``).
            format_version: Format version string (SDMX 3.0 only).
            compress: Whether to request gzip compression (SDMX 3.0 only,
                default: ``"true"``; the response is decompressed
                transparently).
            accept_encoding: Value for the ``Accept-Encoding`` request header.
            accept_language: Value for the ``Accept-Language`` request header.

        Returns:
            Raw XML response text.

        Raises:
            ValueError: If the request fails.

        Notes:
            Passing ``resource_id="*"`` triggers the Eurostat "special case"
            bulk endpoint that returns **all** artefacts of the given type in a
            single request.  This covers both documented special cases of the
            SDMX 3.0 API:

            - *Dataset listing*: ``resource_type=DATAFLOW``, ``resource_id="*"``
              → full catalogue of all dataflows.
              The documentation can be found here : https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-getting-started/sdmx3.0#APIGettingstartedwithSDMX3.0API-SpecialcaseofDatasetlisting

            - *Metadata harvesting*: any other ``resource_type`` with
              ``resource_id="*"`` → all codelists, DSDs, concept schemes, etc.
              The documentation can be found here : https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-getting-started/sdmx3.0#APIGettingstartedwithSDMX3.0API-SpecialcaseofMetadataharvesting
              
            Compression (``compress="true"``) is strongly recommended for these
            bulk queries because responses can be very large.

            Version token summary:

            - ``"*"``: any resource / any agency (wildcard).
            - ``"+"``: latest published version (SDMX 2.1 convention; mapped
              to ``"latest"`` by the 2.1 builder).
            - ``"~"``: latest version *per resource* (SDMX 3.0 only).

            For the dataflow catalogue specifically, prefer
            :meth:`list_all_dataflows`, which automatically selects the correct
            version token, sets optimal parameters (``detail="allstubs"``,
            ``references="none"``), and returns a parsed DataFrame.

        Examples:
            >>> # Requête d'un codelist spécifique
            >>> xml = client.get_structure(
            ...     StructureResourceType.CODELIST, "CL_GEO"
            ... )
            >>> # Requête d'un dataflow avec ses artefacts descendants
            >>> xml = client.get_structure(
            ...     StructureResourceType.DATAFLOW,
            ...     "namq_10_gdp",
            ...     references="descendants",
            ... )
            >>> # Metadata harvesting : tous les codelists Eurostat
            >>> xml = client.get_structure(
            ...     StructureResourceType.CODELIST,
            ...     resource_id="*",
            ...     agency="ESTAT",
            ...     compress="true",
            ... )
        """
        # Construction de l'endpoint et des paramètres via le builder
        endpoint = self.endpoint_builder.build_structure_endpoint(
            resource_type=resource_type,
            resource_id=resource_id,
            agency=agency,
            version=version,
        )
        params = self.endpoint_builder.build_structure_params(
            references=references,
            detail=detail,
            format=format,
            format_version=format_version,
            compress=compress,
        )
        headers = self.endpoint_builder.build_headers(
            accept_encoding=accept_encoding,
            accept_language=accept_language,
        )

        # Sélection du client API (Comext si nécessaire)
        client = self._get_api_client(resource_id)

        # Requête de l'artefact structurel
        try:
            response = client.get(endpoint, params=params, headers=headers)
            # Décompression si nécessaire (réponses gzip de l'API SDMX 3.0)
            content = self._decompress_response_bytes(response.content)
            return content.decode("utf-8")
        # Gestion des erreurs de requête
        except Exception as e:
            logger.error(
                f"Failed to fetch {resource_type.value}/{resource_id}: {e}"
            )
            raise ValueError(
                f"Failed to fetch {resource_type.value} '{resource_id}': {e}"
            )

    # Méthode publique de récupération et parsing de la DSD d'un dataflow
    def get_dataflow_structure(
        self,
        dataflow: str,
        version: str = "+",
    ) -> DataflowStructure:
        """Retrieve and parse the DSD for a dataflow.

        Convenience wrapper around ``get_structure`` that returns a parsed
        ``DataflowStructure`` instead of raw XML.

        The version wildcards ``"*"`` and ``"~"`` are valid for data
        endpoints but cause HTTP 500 errors on Eurostat's structure endpoint.
        They are therefore remapped to ``"+"`` (latest version) before the
        structure request is issued.

        Args:
            dataflow: Dataflow identifier.
            version: Dataflow version (``"+"`` for latest). The wildcards
                ``"*"`` and ``"~"`` are automatically remapped to ``"+"``.

        Returns:
            Parsed ``DataflowStructure`` with dimension information.

        Raises:
            ValueError: If the structure cannot be retrieved or parsed.
        """
        # Remapping des wildcards "data" vers "+" (dernière version) pour la structure :
        # l'API Eurostat renvoie HTTP 500 pour "*" et "~" sur l'endpoint /structure/datastructure
        _UNSUPPORTED_STRUCTURE_VERSIONS = {"*", "~"}
        structure_version = "+" if version in _UNSUPPORTED_STRUCTURE_VERSIONS else version

        # Requête du XML brut via get_structure (endpoint datastructure, avec descendants)
        xml_text = self.get_structure(
            resource_type=StructureResourceType.DATASTRUCTURE,
            resource_id=dataflow,
            agency=AGENCY_ID,
            version=structure_version,
            references="descendants",
            compress="false"
        )
        # Parsing du XML et retour de la structure de dataflow
        return self._parse_structure_response(xml_text, dataflow)

    # Méthode publique d'extraction du catalogue de dataflows Eurostat
    def list_all_dataflows(
        self,
        agency: str = "ESTAT",
    ) -> pd.DataFrame:
        """Retrieve the full Eurostat dataflow catalogue.

        Convenience wrapper for the *Dataset listing* special case of the
        Eurostat SDMX API.  Fetches all dataflow definitions available on the
        configured API endpoint and returns them as a tidy DataFrame.

        Internally calls :meth:`get_structure` with ``resource_id="*"``,
        ``detail="allstubs"``, and ``references="none"``, which maps to the
        following bulk endpoints:

        - SDMX 3.0: ``/sdmx/3.0/structure/dataflow/{agency}/*``
        - SDMX 2.1: ``/sdmx/2.1/dataflow/{agency}/all/latest``

        For other structure types (codelists, DSDs, concept schemes), use
        :meth:`get_structure` directly with ``resource_id="*"``.  See the
        *Notes* section of :meth:`get_structure` for details on bulk /
        metadata-harvesting queries.

        Args:
            agency: Maintaining agency filter. Defaults to ``"ESTAT"`` (official
                Eurostat datasets). Use ``"*"`` for all agencies (mapped to
                ``"all"`` by the SDMX 2.1 builder).

        Returns:
            DataFrame with columns: ``id``, ``name``, ``version``,
            ``agency``.

        Raises:
            ValueError: If the catalogue cannot be retrieved or parsed.

        Examples:
            >>> # Catalogue des datasets officiels Eurostat (défaut)
            >>> catalogue = client.list_all_dataflows()
            >>> # Catalogue de toutes les agences
            >>> all_agencies = client.list_all_dataflows(agency="*")
        """
        # Sélection du token de version adapté à la version d'API :
        # SDMX 3.0 → None pour omettre le segment de version (cas spécial Dataset listing)
        # SDMX 2.1 → "+" converti en "latest" par le builder V2.1
        version: Optional[str] = None if self.api_version == EurostatAPIVersion.V3_0 else "+"

        # Requête du catalogue via get_structure avec wildcards
        xml_text = self.get_structure(
            resource_type=StructureResourceType.DATAFLOW,
            resource_id="*",
            agency=agency,
            version=version,
            references="none",
            detail="allstubs",
            format="structure",
            format_version="3.0",
            compress="true",
        )

        # Parsing du XML et retour sous forme de DataFrame
        return self._parse_dataflow_list_response(xml_text)

    # ──────────────────────────────────────────────────────────────────
    # Méthodes publiques — Registre de structures
    # ──────────────────────────────────────────────────────────────────

    # Méthode publique d'enregistrement d'une structure pré-chargée
    def register_structure(self, structure: DataflowStructure) -> None:
        """Register a pre-loaded structure in the internal registry.

        Args:
            structure: DataflowStructure to register.
        """
        # Enregistrement de la structure dans le registre interne
        self.structure_registry.register(structure)
        logger.info(f"Registered structure for {structure.dataflow}")

    # ──────────────────────────────────────────────────────────────────
    # Context manager et fermeture des ressources
    # ──────────────────────────────────────────────────────────────────

    # Méthode de fermeture des connexions HTTP
    def close(self) -> None:
        """Close API client connections."""
        # Fermeture du client API standard
        if self.api_client:
            self.api_client.close()
        # Fermeture du client Comext si initialisé
        if self._comext_client:
            self._comext_client.close()
        logger.info("Eurostat client closed")

    # Entrée du context manager
    def __enter__(self) -> "EurostatClient":
        """Context manager entry.

        Returns:
            Self for use in with statement.
        """
        return self

    # Sortie du context manager avec fermeture des ressources
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit.

        Args:
            exc_type: Exception type if raised.
            exc_val: Exception value if raised.
            exc_tb: Exception traceback if raised.
        """
        self.close()

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Sélection du client API
    # ──────────────────────────────────────────────────────────────────

    # Méthode statique de détection des datasets Comext (préfixe DS-)
    @staticmethod
    def _is_comext_dataset(dataflow: str) -> bool:
        """Detect if a dataflow belongs to the Comext database.

        Args:
            dataflow: Dataflow identifier.

        Returns:
            True if the dataflow starts with ``'DS-'``.
        """
        # Détection du préfixe DS- caractéristique des datasets Comext
        return dataflow.upper().startswith("DS-")

    # Méthode d'accès au client Comext avec initialisation paresseuse
    def _get_comext_client(self) -> APIClient:
        """Get or create the Comext API client (lazy initialisation).

        Returns:
            ``APIClient`` instance for the Comext endpoint.
        """
        # Création du client Comext si non encore initialisé
        if self._comext_client is None:
            comext_url = self._COMEXT_BASE_URLS[self.api_version]
            self._comext_client = APIClient(
                base_url=comext_url, timeout=self._timeout
            )
        return self._comext_client

    # Méthode de sélection du client API approprié selon le dataflow
    def _get_api_client(self, dataflow: str) -> APIClient:
        """Return the appropriate API client for a given dataflow.

        Args:
            dataflow: Dataflow identifier.

        Returns:
            Standard or Comext ``APIClient``.
        """
        # Redirection vers le client Comext pour les datasets DS-*
        if self._is_comext_dataset(dataflow):
            return self._get_comext_client()
        return self.api_client

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Configuration
    # ──────────────────────────────────────────────────────────────────

    # Méthode auxiliaire de chargement du rate limiter depuis le fichier de configuration
    def _load_rate_limiter(self) -> Optional[RateLimiter]:
        """Load rate limiter from ``parameters/eurostat.json``.

        Returns:
            ``RateLimiter`` instance or *None* if configuration not found.
        """
        try:
            # Construction du chemin vers le fichier de paramètres
            params_path = (
                Path(__file__).parents[3] / "parameters" / "eurostat.json"
            )
            # Lecture et parsing du fichier de configuration si existant
            if params_path.exists():
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
            # Logging si aucune configuration de rate limit trouvée
            logger.debug("No RATE_LIMIT configuration found")
            return None
        # Gestion des erreurs de lecture ou de parsing
        except Exception as e:
            logger.warning(f"Failed to load rate limiter config: {e}")
            return None

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Parsing des réponses
    # ──────────────────────────────────────────────────────────────────

    # Méthode statique de décompression transparente des réponses gzip
    @staticmethod
    def _decompress_response_bytes(content: bytes) -> bytes:
        """Transparently decompress response bytes if gzip-encoded.

        Some Eurostat API endpoints return gzip-compressed content when the
        ``compress=true`` query parameter is set, or by default for large
        structure responses.  This method inspects the magic bytes and
        decompresses only when necessary, so it is safe to call on any
        response regardless of whether compression was requested.

        Args:
            content: Raw response bytes, possibly gzip-compressed.

        Returns:
            Decompressed bytes, or the original bytes unchanged if the
            content is not gzip-compressed.
        """
        # Détection de la compression gzip par les octets magiques (0x1F 0x8B)
        if content[:2] == b"\x1f\x8b":
            return gzip.decompress(content)
        return content

    # Méthode statique de parsing de réponse SDMX-CSV
    @staticmethod
    def _parse_csv_response(text: str) -> pd.DataFrame:
        """Parse an SDMX-CSV response.

        Args:
            text: CSV response text.

        Returns:
            Parsed DataFrame.

        Raises:
            ValueError: If CSV parsing fails.
        """
        # Parsing direct du CSV avec pandas
        try:
            return pd.read_csv(StringIO(text))
        # Gestion des erreurs de parsing
        except Exception as e:
            logger.error(f"CSV parsing failed: {e}")
            raise ValueError(f"Failed to parse CSV response: {e}")

    # Méthode statique de parsing du format TSV Eurostat (format large avec flags)
    @staticmethod
    def _parse_tsv_response(text: str) -> pd.DataFrame:
        """Parse Eurostat TSV format (wide format with flags).

        The first column contains dimensions separated by commas, followed
        by tab-separated period columns.

        Args:
            text: TSV response text.

        Returns:
            Parsed DataFrame in long (tidy) format.

        Raises:
            ValueError: If TSV parsing fails.
        """
        try:
            # Lecture du TSV avec séparateur tabulation
            df = pd.read_csv(StringIO(text), sep="\t")
            index_col = df.columns[0]

            # Sélection des colonnes de périodes (contiennent des chiffres)
            period_cols = [
                col
                for col in df.columns[1:]
                if any(char.isdigit() for char in col)
            ]

            # Extraction des dimensions depuis la première colonne composite
            dimensions_split = df[index_col].str.split(",", expand=True)
            dim_names = [f"DIM_{i}" for i in range(len(dimensions_split.columns))]
            dimensions_split.columns = dim_names

            # Reconstruction du DataFrame en format large puis conversion en format long
            df_wide = pd.concat(
                [dimensions_split, df[period_cols].copy()], axis=1
            )
            df_long = df_wide.melt(
                id_vars=dim_names,
                var_name="TIME_PERIOD",
                value_name="value",
            )

            # Nettoyage des valeurs (suppression des flags et conversion numérique)
            df_long["value"] = df_long["value"].astype(str).str.strip()
            df_long["value"] = pd.to_numeric(df_long["value"], errors="coerce")

            return df_long
        # Gestion des erreurs de parsing
        except Exception as e:
            logger.error(f"TSV parsing failed: {e}")
            raise ValueError(f"Failed to parse TSV response: {e}")

    # Méthode statique de parsing de réponse JSON-stat 2.0
    @staticmethod
    def _parse_json_response(data: Dict[str, Any]) -> pd.DataFrame:
        """Parse a JSON-stat 2.0 response.

        Args:
            data: JSON-stat dictionary.

        Returns:
            Parsed DataFrame.

        Raises:
            ValueError: If JSON parsing fails.
        """
        try:
            # Extraction des dimensions et des observations depuis la réponse
            dimensions = data.get("dimension", {})
            observations = data.get("observation", {})

            # Construction des lignes du DataFrame à partir des observations
            rows = []
            for obs_key, value in observations.items():
                # Parsing de la clé d'observation (format : "0:1:2:...")
                indices = list(map(int, obs_key.split(":")))
                row: Dict[str, Any] = {}

                # Mapping des indices positionnels vers les codes de dimensions
                for i, (dim_name, dim_info) in enumerate(dimensions.items()):
                    if i < len(indices):
                        dim_idx = indices[i]
                        if "category" in dim_info and "index" in dim_info["category"]:
                            categories = dim_info["category"]["index"]
                            if dim_idx in categories:
                                row[dim_name] = categories[dim_idx]

                # Ajout de la valeur d'observation à la ligne courante
                row["value"] = value
                rows.append(row)

            return pd.DataFrame(rows)
        # Gestion des erreurs de parsing
        except Exception as e:
            logger.error(f"JSON parsing failed: {e}")
            raise ValueError(f"Failed to parse JSON response: {e}")

    # Méthode de parsing d'une réponse SDMX-ML et d'extraction des dimensions
    def _parse_structure_response(
        self, xml_content: str, dataflow: str
    ) -> DataflowStructure:
        """Parse an SDMX-ML structure response and extract dimensions.

        Tries SDMX 3.0 namespaces first, then falls back to 2.1.

        Args:
            xml_content: XML response content.
            dataflow: Dataflow identifier.

        Returns:
            ``DataflowStructure`` instance.

        Raises:
            ValueError: If XML parsing fails.
        """
        try:
            # Parsing du document XML
            root = ET.fromstring(xml_content)

            # Tentative avec les namespaces SDMX 3.0 puis fallback vers 2.1
            namespaces = self._SDMX3_NS
            structure_elem = root.find(".//str:DataStructure", namespaces)
            if structure_elem is None:
                namespaces = self._SDMX21_NS
                structure_elem = root.find(".//str:DataStructure", namespaces)

            # Vérification de la présence de l'élément DataStructure
            if structure_elem is None:
                raise ValueError(
                    "DataStructure element not found in XML response"
                )

            # Construction d'un index id -> nom depuis les ConceptSchemes
            # (les dimensions ne portent pas de description directement :
            #  elles référencent un Concept via ConceptIdentity)
            concept_names: dict[str, str] = {}
            for concept in root.findall(".//str:Concept", namespaces):
                concept_id = concept.get("id")
                if not concept_id:
                    continue
                # Extraction du nom
                name: str | None = None
                for name_elem in concept.findall("com:Name", namespaces):
                    name = name_elem.text
                concept_names[concept_id] = name

            # Extraction de la liste des dimensions depuis le DSD
            dimensions: list[DimensionInfo] = []
            dimension_list = structure_elem.find(
                ".//str:DimensionList", namespaces
            )
            if dimension_list is not None:
                for i, dim in enumerate(
                    dimension_list.findall("str:Dimension", namespaces)
                ):
                    dim_id = dim.get("id")
                    position = dim.get("position", str(i))

                    # Résolution de la description via le ConceptScheme
                    description = concept_names.get(dim_id)

                    dimensions.append(
                        DimensionInfo(
                            name=dim_id,
                            position=int(position),
                            description=description,
                        )
                    )

            # Construction et retour de la structure de dataflow
            return DataflowStructure(
                agency=AGENCY_ID,
                dataflow=dataflow,
                num_dimensions=len(dimensions),
                dimensions=dimensions,
                description=None,
            )
        # Gestion des erreurs de parsing XML
        except Exception as e:
            logger.error(f"Structure XML parsing failed: {e}")
            raise ValueError(f"Failed to parse structure response: {e}")

    # Méthode de parsing d'une réponse SDMX-ML contenant une liste de dataflows
    def _parse_dataflow_list_response(
        self, xml_content: str
    ) -> pd.DataFrame:
        """Parse an SDMX-ML structure response containing multiple dataflows.

        Tries SDMX 3.0 namespaces first, then falls back to 2.1.
        Extracts the English name for each dataflow when available.

        Args:
            xml_content: XML response content (already decompressed).

        Returns:
            DataFrame with columns: ``id``, ``name``, ``version``,
            ``agency``.

        Raises:
            ValueError: If XML parsing or element extraction fails.
        """
        try:
            # Parsing du document XML
            root = ET.fromstring(xml_content)

            # Tentative avec les namespaces SDMX 3.0 puis fallback 2.1
            namespaces = self._SDMX3_NS
            dataflows = root.findall(".//str:Dataflow", namespaces)
            if not dataflows:
                namespaces = self._SDMX21_NS
                dataflows = root.findall(".//str:Dataflow", namespaces)

            # Extraction des métadonnées de chaque dataflow
            rows = []
            for df_elem in dataflows:
                df_id = df_elem.get("id")
                df_agency = df_elem.get("agencyID")
                df_version = df_elem.get("version")

                # Extraction du nom anglais, ou première langue disponible
                name: Optional[str] = None
                for name_elem in df_elem.findall("com:Name", namespaces):
                    lang = name_elem.get(
                        "{http://www.w3.org/XML/1998/namespace}lang", ""
                    )
                    if name is None or lang == "en":
                        name = name_elem.text

                rows.append(
                    {
                        "id": df_id,
                        "name": name,
                        "version": df_version,
                        "agency": df_agency,
                    }
                )

            # Logging
            logger.info(f"Parsed {len(rows)} dataflows from catalogue response")
            return pd.DataFrame(rows)
        # Gestion des erreurs de parsing XML
        except Exception as e:
            logger.error(f"Dataflow catalogue parsing failed: {e}")
            raise ValueError(f"Failed to parse dataflow catalogue: {e}")

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Normalisation, filtrage, doublons
    # ──────────────────────────────────────────────────────────────────

    # Méthode de construction de la clé positionnelle pour l'URL
    def _build_key_string(
        self,
        dims: Dict[str, List[str]],
        structure: "DataflowStructure",
    ) -> str:
        """Build a positional key string for the data endpoint URL.

        The key encodes dimension filters as a dot-separated sequence of
        values, one slot per dimension.  Each slot is either a single value,
        multiple values joined with ``+``, or a wildcard token.

        Wildcard tokens differ by API version:

        - SDMX 3.0: ``*``
        - SDMX 2.1: ``all``

        Args:
            dims: Dimension name → list of values to include in the URL key.
                Dimensions absent from this mapping receive the wildcard.
            structure: Dataflow structure used to resolve dimension names to
                their positional order.

        Returns:
            Positional key string (e.g. ``"FRA+DEU.*.Q"``).

        Examples:
            >>> key = client._build_key_string(
            ...     {"GEO": ["FR", "DE"], "FREQ": ["Q"]},
            ...     structure,
            ... )
            >>> # Returns e.g. "*.FR+DE.Q.*.*" depending on positions
        """
        # Jeton wildcard selon la version d'API
        wildcard = "*" if self.api_version == EurostatAPIVersion.V3_0 else "all"
        # Index lowercase pour la comparaison insensible à la casse
        dims_lower = {k.lower(): v for k, v in dims.items()}
        # Itération sur les dimensions triées par position (positions XML 1-based)
        sorted_dims = sorted(structure.dimensions, key=lambda d: d.position)
        parts = []
        for dim_info in sorted_dims:
            values = dims_lower.get(dim_info.name.lower())
            if values:
                parts.append("+".join(values))
            else:
                parts.append(wildcard)
        return ".".join(parts)

    # Méthode statique de normalisation des dimensions (str → List[str])
    @staticmethod
    def _normalize_dimensions(
        dimensions: Optional[Dict[str, Union[str, List[str]]]],
        structure: Optional["DataflowStructure"] = None,
    ) -> Optional[Dict[str, List[str]]]:
        """Normalize dimension values to ``Dict[str, List[str]]``.

        Args:
            dimensions: Input dimensions (may contain strings or lists).
            structure: Optional dataflow structure used to validate dimension
                names. Unknown names trigger a warning but are kept.

        Returns:
            Normalised dimensions or *None*.
        """
        # Retour immédiat si pas de dimensions à normaliser
        if not dimensions:
            return None
        # Conversion des valeurs scalaires en listes unitaires
        normalized = {
            k: [v] if isinstance(v, str) else list(v)
            for k, v in dimensions.items()
        }

        # Validation des noms contre la structure si disponible
        if structure:
            for name in normalized:
                if structure.get_position(name) is None:
                    logger.warning(
                        f"Dimension '{name}' not found in structure for this dataflow"
                    )
        return normalized

    # Méthode statique de post-filtrage du DataFrame par valeurs de dimensions
    @staticmethod
    def _filter_dataframe_by_dimensions(
        df: pd.DataFrame,
        filters: Optional[Dict[str, Union[str, List[str]]]],
    ) -> pd.DataFrame:
        """Post-filter a DataFrame by dimension values.

        Args:
            df: Input DataFrame.
            filters: Filters to apply.

        Returns:
            Filtered DataFrame.
        """
        # Retour immédiat si pas de filtres ou DataFrame vide
        if not filters or df.empty:
            return df
        # Application successive des filtres par dimension
        result = df.copy()
        for col, values in filters.items():
            if col in result.columns:
                # Normalisation des valeurs scalaires en liste
                if isinstance(values, str):
                    values = [values]
                # Filtrage par appartenance à la liste de valeurs autorisées
                result = result[result[col].isin(values)]
        return result

    # Méthode statique de détection et de gestion des doublons
    @staticmethod
    def _check_duplicates(
        df: pd.DataFrame,
        dimensions: Optional[Dict[str, Union[str, List[str]]]],
        structure: Optional[DataflowStructure],
        on_duplicate: DuplicateHandling,
    ) -> None:
        """Check for and optionally report duplicate rows.

        Args:
            df: DataFrame to check.
            dimensions: Dimension names.
            structure: Dataflow structure (used for column names).
            on_duplicate: Handling strategy.

        Raises:
            ValueError: If ``on_duplicate='raise'`` and duplicates found.
        """
        # Retour immédiat si le DataFrame est vide ou sans dimensions
        if df.empty or not dimensions:
            return

        # Identification des colonnes de dimensions disponibles dans le DataFrame
        dim_cols = (
            [d.name for d in structure.dimensions]
            if structure
            else list(dimensions.keys())
        )
        available_cols = [c for c in dim_cols if c in df.columns]
        if not available_cols:
            return

        # Construction de la liste de colonnes pour la détection des doublons
        check_cols = available_cols + (
            ["TIME_PERIOD"] if "TIME_PERIOD" in df.columns else []
        )
        duplicates = df[check_cols].duplicated().sum()

        # Application de la stratégie de gestion des doublons
        if duplicates > 0:
            message = f"Found {duplicates} duplicate rows"
            if on_duplicate == "raise":
                raise ValueError(message)
            elif on_duplicate == "warn":
                logger.warning(message)

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Chargement de structure à la demande
    # ──────────────────────────────────────────────────────────────────

    # Méthode d'assurance de la disponibilité de la structure d'un dataflow
    def _ensure_structure(
        self, dataflow: str, version: str = "*"
    ) -> DataflowStructure:
        """Load and cache a dataflow structure.

        Args:
            dataflow: Dataflow identifier.
            version: Dataflow version.

        Returns:
            ``DataflowStructure`` instance.

        Raises:
            ValueError: If the structure cannot be retrieved.
        """
        # Vérification dans le cache avant tout appel API
        cached = self.structure_registry.get(AGENCY_ID, dataflow)
        if cached:
            return cached

        # Chargement via API et mise en cache si auto_fetch_structure est activé
        if self.auto_fetch_structure:
            structure = self.get_dataflow_structure(dataflow, version)
            self.register_structure(structure)
            return structure

        # Levée d'une erreur si la structure est introuvable et le fetch désactivé
        raise ValueError(f"Structure not found for {dataflow}::{version}")

    # ──────────────────────────────────────────────────────────────────
    # Méthodes privées — Requêtes splitées
    # ──────────────────────────────────────────────────────────────────

    # Méthode statique de génération des combinaisons de dimensions pour le split
    @staticmethod
    def _generate_request_combinations(
        dimensions: Optional[Dict[str, List[str]]],
        split_dims: Optional[List[str]],
        max_combinations: int,
        is_v21: bool,
        structure: Optional["DataflowStructure"],
    ) -> List[Tuple[Dict[str, List[str]], Dict[str, List[str]]]]:
        """Generate request combinations as ``(dims_for_url, dims_for_params)`` tuples.

        Each combination encodes which dimensions go into the positional URL
        key and which go into ``c[DIM]=...`` query parameters:

        - ``dims_for_url``: embedded in the URL key via
          :meth:`_build_key_string`.  For SDMX 3.0 these are the
          single-value dimensions; for SDMX 2.1 all dimensions are placed
          in the key.
        - ``dims_for_params``: passed to
          :meth:`EndpointBuilder.build_data_params` as ``c[DIM]=...``
          server-side filters (SDMX 3.0 only; empty dict for SDMX 2.1).

        When ``split_dims`` is given, one combination is produced per
        element of the cartesian product of the split-dimension values;
        each combination always contains a single value per split
        dimension, so that value always lands in ``dims_for_url``.

        Args:
            dimensions: Normalised dimensions, or *None*.
            split_dims: Dimension names to split into separate requests.
            max_combinations: Maximum allowed combinations.
            is_v21: Whether the target API version is SDMX 2.1.
            structure: Dataflow structure (reserved for future use,
                currently unused).

        Returns:
            List of ``(dims_for_url, dims_for_params)`` tuples.  Always
            contains at least one element.

        Raises:
            ValueError: If combinations exceed *max_combinations*.

        Examples:
            >>> # V3.0, no split — single-value dim to URL, multi-value to params
            >>> combos = EurostatClient._generate_request_combinations(
            ...     {"GEO": ["FR"], "UNIT": ["CLV10_MEUR", "CP_MEUR"]},
            ...     split_dims=None, max_combinations=100,
            ...     is_v21=False, structure=None,
            ... )
            >>> combos[0]  # ({"GEO": ["FR"]}, {"UNIT": ["CLV10_MEUR", "CP_MEUR"]})
        """
        # Cas trivial : pas de dimensions → une seule combinaison vide
        if not dimensions:
            return [({}, {})]

        # Séparation des dimensions à splitter de celles à conserver intactes
        split_dims_set = set(split_dims) if split_dims else set()
        split_dict = {k: v for k, v in dimensions.items() if k in split_dims_set}
        keep_dict = {k: v for k, v in dimensions.items() if k not in split_dims_set}

        # Génération du produit cartésien des valeurs des dimensions à splitter
        # (tuple vide si aucune dim à splitter → une seule combinaison)
        split_keys = list(split_dict.keys())
        split_values = [split_dict[k] for k in split_keys]
        combinations = list(itertools.product(*split_values)) if split_keys else [()]

        # Vérification du nombre de combinaisons avant traitement
        if len(combinations) > max_combinations:
            raise ValueError(
                f"Split combinations ({len(combinations)}) exceed "
                f"max allowed ({max_combinations})"
            )

        # Construction des tuples (dims_for_url, dims_for_params) pour chaque combinaison
        result: List[Tuple[Dict[str, List[str]], Dict[str, List[str]]]] = []
        for combo in combinations:
            # Fusion des dims non-splittées avec la valeur unique de chaque dim splittée
            combo_dims: Dict[str, List[str]] = keep_dict.copy()
            for k, val in zip(split_keys, combo):
                combo_dims[k] = [val]

            # Répartition entre URL key et query params selon la version SDMX
            if is_v21:
                # SDMX 2.1 : toutes les dims vont dans le key positionnel
                dims_for_url = combo_dims
                dims_for_params: Dict[str, List[str]] = {}
            else:
                # SDMX 3.0 : valeur unique → key positionnel, multi-valeurs → c[DIM]=...
                dims_for_url = {k: v for k, v in combo_dims.items() if len(v) == 1}
                dims_for_params = {k: v for k, v in combo_dims.items() if len(v) > 1}

            result.append((dims_for_url, dims_for_params))

        return result

    # Méthode d'exécution des requêtes splitées et de concaténation des résultats
    def _execute_split_requests(
        self,
        request_combinations: List[Tuple[Dict[str, List[str]], Dict[str, List[str]]]],
        structure: Optional[DataflowStructure],
        dataflow: str,
        version: str,
        start_period: Optional[str],
        end_period: Optional[str],
        last_n_observations: Optional[int],
        first_n_observations: Optional[int],
        response_format: EurostatResponseFormat,
        compress: bool,
        attributes: Optional[str],
        measures: Optional[str],
        lang: Optional[str],
        labels: Optional[str],
        response_format_version: Optional[str],
        dimension_at_observation: Optional[str],
        detail: Optional[DataDetail],
    ) -> pd.DataFrame:
        """Execute multiple split requests and concatenate results.

        Builds each sub-request directly from its
        ``(dims_for_url, dims_for_params)`` tuple without recursing into
        :meth:`get_data`, so the structure is reused rather than
        re-fetched for every combination.

        Args:
            request_combinations: List of ``(dims_for_url, dims_for_params)``
                tuples produced by :meth:`_generate_request_combinations`.
            structure: Dataflow structure, used to build the positional URL
                key via :meth:`_build_key_string`.
            dataflow: Dataflow identifier.
            version: Dataflow version.
            start_period: Start period.
            end_period: End period.
            last_n_observations: Number of last observations.
            first_n_observations: Number of first observations.
            response_format: Response format.
            compress: Whether to compress.
            attributes: Attributes to include (SDMX 3.0 only).
            measures: Measures to include (SDMX 3.0 only).
            lang: Language code (SDMX 3.0 only).
            labels: Label display mode (SDMX 3.0 only).
            response_format_version: Format version string (SDMX 3.0 only).
            dimension_at_observation: Dimension at observation level
                (SDMX 2.1 only).
            detail: Data detail level (SDMX 2.1 only).

        Returns:
            Concatenated DataFrame from all requests.
        """
        is_v21 = self.api_version == EurostatAPIVersion.V2_1
        client = self._get_api_client(dataflow)
        # Initialisation de la liste des DataFrames résultants
        dfs: list[pd.DataFrame] = []

        # Exécution de chaque sous-requête correspondant à une combinaison de dimensions
        for dims_for_url, dims_for_params in request_combinations:
            try:
                # Construction de la clé positionnelle
                key_str: Optional[str] = None
                if dims_for_url and structure:
                    key_str = self._build_key_string(dims_for_url, structure)
                elif is_v21:
                    key_str = "all"

                # Construction de l'endpoint et des paramètres via le builder
                endpoint = self.endpoint_builder.build_data_endpoint(
                    dataflow=dataflow,
                    agency=AGENCY_ID,
                    version=version,
                    key=key_str,
                )
                params = self.endpoint_builder.build_data_params(
                    dimensions=dims_for_params or None,
                    start_period=start_period,
                    end_period=end_period,
                    last_n_observations=last_n_observations,
                    first_n_observations=first_n_observations,
                    compress=compress,
                    response_format=response_format,
                    response_format_version=response_format_version,
                    lang=lang,
                    labels=labels,
                    attributes=attributes,
                    measures=measures,
                    dimension_at_observation=dimension_at_observation,
                    detail=detail,
                )

                # Exécution de la requête
                response = client.get(endpoint, params=params)
                raw_bytes = self._decompress_response_bytes(response.content)

                # Parsing de la réponse selon le format demandé
                if response_format == EurostatResponseFormat.CSV:
                    df = self._parse_csv_response(raw_bytes.decode("utf-8"))
                elif response_format == EurostatResponseFormat.TSV:
                    df = self._parse_tsv_response(raw_bytes.decode("utf-8"))
                elif response_format == EurostatResponseFormat.JSON:
                    df = self._parse_json_response(
                        json.loads(raw_bytes.decode("utf-8"))
                    )
                else:
                    raise ValueError(f"Unsupported format: {response_format}")

                dfs.append(df)
            # Gestion des erreurs par sous-requête (non fatale)
            except Exception as e:
                logger.error(f"Split request failed for {dims_for_url}: {e}")
                continue

        # Concaténation des résultats ou retour d'un DataFrame vide si tout a échoué
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()