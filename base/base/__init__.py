import sqlalchemy as sa

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
SUBTYPE_REEFER = "reefer"
UNKNOWN_COMMODITY = "unknown"
PIPELINE_GAS = "natural_gas"
PIPELINE_OIL = "pipeline_oil"

# Transport mode
SEABORNE = "seaborne"
PIPELINE = "pipeline"
RAIL_ROAD = "rail_road"

# MARINE TRAFFIC METHODS
VESSEL_DATA = "vesselmasterdata/"
VESSEL_EVENTS = "vesselevents/"
VESSEL_POSITION = "exportvesseltrack/"
VESSEL_PORTCALLS = "portcalls/"
HTTP_OK = "200"
MARINETRAFFIC_LATENCY_HOURS = 12

# LOAD STATUS
FULLY_LADEN = "fully_laden"
PARTIALLY_LADEN = "partially_laden"
IN_BALLAST = "in_ballast"


# SHIPMENT STATUS
ONGOING = "ongoing"
COMPLETED = "completed"
UNDETECTED_ARRIVAL = "undetected_arrival"
UNKNOWN = "unknown"

# COUNTER
COUNTER_OBSERVED = "observed"
COUNTER_ESTIMATED = "estimated"
COUNTER_VERSION0 = "v0"
COUNTER_VERSION1 = "v1"
COUNTER_VERSION2 = "v2"
COUNTER_VERSION_DEFAULT = COUNTER_VERSION2

DWT_MIN = 5000

AVG_TANKER_SPEED_KMH = 22

FOR_ORDERS = "for_orders"
UNKNOWN_COUNTRY = "unknown"

QUERY_POSITION_HOURS_AFTER_ARRIVAL = 72
QUERY_POSITION_HOURS_BEFORE_DEPARTURE = 72
BERTH_MAX_HOURS_AFTER_DEPARTURE = 24
MARINETRAFFIC_LATENCY_HOURS = 24
MARINETRAFFIC_PORTCALL_WINDOW_HOURS = 12

GCV_KWH_PER_M3 = 11.3505
KG_PER_M3 = 0.717
M3_PER_NM3 = 1.055

# ENTSOGFLOW TYPE
ENTSOG_CROSSBORDER = "crossborder"
ENTSOG_PRODUCTION = "production"
ENTSOG_TRANSMISSION = "transmission"
ENTSOG_DISTRIBUTION = "distribution"
ENTSOG_CONSUMPTION = "consumption"
ENTSOG_STORAGE = "storage"
ENTSOG_LNG = "lng"

ENTSOG_PRODUCTION_ENTRY = "production_entry"
ENTSOG_PRODUCTION_EXIT = "production_exit"
ENTSOG_CONSUMPTION_ENTRY = "consumption_entry"
ENTSOG_CONSUMPTION_EXIT = "consumption_exit"
ENTSOG_DISTRIBUTION_ENTRY = "distribution_entry"
ENTSOG_DISTRIBUTION_EXIT = "distribution_exit"
ENTSOG_LNG_ENTRY = "lng_entry"
ENTSOG_LNG_EXIT = "lng_exit"
ENTSOG_STORAGE_ENTRY = "storage_entry"
ENTSOG_STORAGE_EXIT = "storage_exit"
ENTSOG_TRANSMISSION_ENTRY = "transmission_entry"
ENTSOG_TRANSMISSION_EXIT = "transmission_exit"
ENTSOG_TRADING = "trading"

# COMMODITY GROUPING
COMMODITY_GROUPING_HELP = "Grouping to use (default=coal,oil,gas; split_gas=coal,oil,lng,pipeline_gas; split_gas_oil=coal,crude_oil,oil_products,lng,pipeline_gas)"
COMMODITY_GROUPING_DEFAULT = "default"
COMMODITY_GROUPING_CHOICES = ["default", "split_gas", "split_gas_oil"]

# IMO WEBSITE SCRAPING
IMO_BASE_URL = "https://gisis.imo.org/public/ships/default.aspx"


# PRICING SCENARIO
PRICING_DEFAULT = "default"
PRICING_ENHANCED = "enhanced"  # Enhanced price cap

# INSURER
UNKNOWN_INSURER = "unknown"

# COMPANY REFRESHING
REFRESH_COMPANY_DAYS = 3
REFRESH_KNOWN_COMPANY_DAYS = 30

# NULL FOR POSTGRES CONSTRAINTS TO WORK
PRICE_NULLARRAY_CHAR = sa.sql.expression.literal_column("array[NULL::varchar]")
PRICE_NULLARRAY_INT = sa.sql.expression.literal_column("array[NULL::bigint]")

G7_ISO2S = ["CA", "FR", "DE", "IT", "JP", "GB", "US"]

EU27_ISO2S = [
    "AT",
    "BE",
    "BG",
    "CY",
    "CZ",
    "DE",
    "DK",
    "EE",
    "ES",
    "FI",
    "FR",
    "GR",
    "HR",
    "HU",
    "IE",
    "IT",
    "LT",
    "LU",
    "LV",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SE",
    "SI",
    "SK",
]


CHARTS_USE_KPLER_DEFAULT = True
