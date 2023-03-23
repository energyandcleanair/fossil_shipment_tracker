COUNTRY_GLOBAL = "Global"
FACET_NONE = "None"
COMMODITY_ALL = "All"

# Default values
DEFAULT_ORIGIN_COUNTRY = "RU"
DEFAULT_DESTINATION_COUNTRY = "EG"
DEFAULT_COLOUR_BY = "destination_name"
DEFAULT_ROLLING_DAYS = 14
DEFAULT_COMMODITY = COMMODITY_ALL
DEFAULT_CHART_TYPE = "area"

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
        "column": "value_usd",
        "format": ",.0f",
        "scale": 1e-6,
    },
}


countries = {"RU": "Russia", "EG": "Egypt"}
colour_bys = {"product": "Product", "destination_name": "Destination"}
from_type = {"country": "Country", "port": "Port"}
to_type = {"country": "Country", "port": "Port"}
facet_bys = {
    FACET_NONE: "None",
    "destination_region": "Destination region",
    "destination_country": "Destination country",
    "commodity_equivalent": "Commodity",
}
commodities = {COMMODITY_ALL: "All", "crude_oil": "Crude", "oil_products": "Oil products"}
chart_types = {"area": "Area", "line": "Line", "bar": "Bar"}

refreshing = False

from .layout import *
from .data import *
from .update import *
from .store import store
