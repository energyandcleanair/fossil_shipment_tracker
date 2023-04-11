COUNTRY_GLOBAL = "Global"
FACET_NONE = "None"
COMMODITY_ALL = "All"

# Default values
DEFAULT_ORIGIN_COUNTRY = "RU"
DEFAULT_DESTINATION_COUNTRY = COUNTRY_GLOBAL
DEFAULT_COLOUR_BY = "destination_name"
DEFAULT_ROLLING_DAYS = 14
DEFAULT_COMMODITIES = ["crude_oil", "oil_products"]
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
colour_bys = {
    "product": "Product",
    "product_group": "Product group",
    "product_family": "Product family",
    "commodity": "Commodity",
    "origin_name": "Origin",
    "destination_name": "Destination",
    "destination_region": "Destination region",
}
from_type = {"country": "Country", "port": "Port"}
to_type = {"country": "Country", "port": "Port"}
facet_bys = {
    FACET_NONE: "None",
    "origin_region": "Origin region",
    "origin_country": "Origin country",
    "product": "Product",
    "product_group": "Product group",
    "product_family": "Product family",
    "destination_region": "Destination region",
    "destination_country": "Destination country",
    "commodity_equivalent": "Commodity",
}
commodities = {"crude_oil": "Crude", "oil_products": "Oil products", "lng": "LNG", "coal": "Coal"}
chart_types = {"area": "Area", "line": "Line", "bar": "Bar"}

refreshing = False

# Preset
laundromat_iso2s = ["EG", "SG", "IN", "CN", "AE"]
eu27_iso2s = [
    "AT",
    "BE",
    "BG",
    "HR",
    "CY",
    "CZ",
    "DK",
    "EE",
    "FI",
    "FR",
    "DE",
    "GR",
    "HU",
    "IE",
    "IT",
    "LV",
    "LT",
    "LU",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SK",
    "SI",
    "ES",
    "SE",
]


pcc_iso2s = [
    "AT",
    "BE",
    "BG",
    "HR",
    "CY",
    "CZ",
    "DK",
    "EE",
    "FI",
    "FR",
    "DE",
    "GR",
    "HU",
    "IE",
    "IT",
    "LV",
    "LT",
    "LU",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SK",
    "SI",
    "ES",
    "SE",
    "GB",
    "US",
    "CA",
    "AU",
    "JP",
]
from .layout import *
from .data import *
from .update import *
from .store import store
from .download import *
