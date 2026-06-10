"""OECD query request dataclass.

Public DTO encapsulating all parameters of an :meth:`OECDClient.get_data`
call, enabling type-safe construction and batching of query requests.
"""
# Importation des modules
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from ...core.sdmx import DimensionAtObservation, DuplicateHandling
from .formats import OECDResponseFormat


# Classe représentant une requête de données
@dataclass
class OECDQueryRequest:
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
        >>> query = OECDQueryRequest(
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
    format: OECDResponseFormat = OECDResponseFormat.CSV_LABELS
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
