# Importation des éléments d'intérêt du module
# OECD
from .oecd import OECDQueryRequest, OECDClient
# Eurostat
from .eurostat import (
    BaseEurostatQueryRequest,
    EurostatQueryRequestV30,
    EurostatQueryRequestV21,
    EurostatClient,
    EurostatResponseFormat,
)

# Réexport des éléments d'intérêt du module
__all__ = [
    'OECDQueryRequest',
    'OECDClient',
    'BaseEurostatQueryRequest',
    'EurostatQueryRequestV30',
    'EurostatQueryRequestV21',
    'EurostatClient',
    'EurostatResponseFormat',
]