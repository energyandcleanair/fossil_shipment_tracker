CRUDE_OIL = "crude_oil"
OIL_PRODUCTS = "oil_products"
OIL_OR_CHEMICAL = "oil_or_chemical"
OIL_OR_ORE = "oil_or_ore"
LNG = "lng"
LPG = "lpg"
BULK = "bulk"
COAL = "coal"
GENERAL_CARGO = "general_cargo"
BULK_NOT_COAL = "bulk_not_coal"
UNKNOWN_COMMODITY = "unknown"
PIPELINE_GAS = "natural_gas"
PIPELINE_OIL = "pipeline_oil"

# MARINE TRAFFIC METHODS
VESSEL_DATA = "vesselmasterdata/"
VESSEL_EVENTS = "vesselevents/"
VESSEL_POSITION = "exportvesseltrack/"
HTTP_OK = "200"


# LOAD STATUS
FULLY_LADEN = "fully_laden"
PARTIALLY_LADEN = "partially_laden"
IN_BALLAST = "in_ballast"


# SHIPMENT STATUS
ONGOING = "ongoing"
COMPLETED = "completed"
UNDETECTED_ARRIVAL = "undetected_arrival"
UNKNOWN = "unkown"

# COUNTER
COUNTER_OBSERVED = "observed"
COUNTER_ESTIMATED = "estimated"

DWT_MIN = 5000

AVG_TANKER_SPEED_KMH = 22

FOR_ORDERS = "for_orders"

QUERY_POSITION_HOURS_AFTER_ARRIVAL = 72
QUERY_POSITION_HOURS_BEFORE_DEPARTURE = 72
BERTH_MAX_HOURS_AFTER_DEPARTURE = 24

GCV_KWH_PER_M3 = 11.259
KG_PER_M3 = 0.717

# ENTSOGFLOW TYPE
ENTSOG_CROSSBORDER = "crossborder"
ENTSOG_PRODUCTION = "production"
ENTSOG_CONSUMPTION = "consumption"
ENTSOG_DISTRIBUTION = "distribution"
ENTSOG_STORAGE_ENTRY = "storage_entry"
ENTSOG_STORAGE_EXIT = "storage_exit"
ENTSOG_TRANSMISSION_ENTRY = "transmission_entry"
ENTSOG_TRANSMISSION_EXIT = "transmission_exit"

# COMMODITY GROUPING
COMMODITY_GROUPING_DEFAULT = "default"

# IMO WEBSITE SCRAPING
IMO_BASE_URL = "https://gisis.imo.org/public/ships/default.aspx"


# PRICING SCENARIO
PRICING_DEFAULT = "default"
PRICING_PRICECAP = "pricecap"