COUNTRY_GLOBAL = "Global"
FACET_NONE = "None"

units = {
    "thousand_tonne": {
        "label": "Thousand tonne",
        "column": "value_tonne",
        "format": ",.0f",
        "scale": 1e-3,
    },
    "million_eur": {
        "label": "Million EUR",
        "column": "value_eur",
        "format": ",.0f",
        "scale": 1e-6,
    },
    "million_usd": {
        "label": "Million USD",
        "column": "value_eur",
        "format": ",.0f",
        "scale": 1e-6,
    },
}


countries = {"RU": "Russia", "EG": "Egypt"}

from .layout import *
from .data import *
from .update import *
