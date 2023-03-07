DB_TABLE_PORTCALL = "portcall"
DB_TABLE_SHIP = "ship"
DB_TABLE_SHIP_INSURER = "ship_insurer"
DB_TABLE_SHIP_OWNER = "ship_owner"
DB_TABLE_SHIP_MANAGER = "ship_manager"
DB_TABLE_COMPANY = "company"
DB_TABLE_DEPARTURE = "departure"
DB_TABLE_ARRIVAL = "arrival"
DB_TABLE_PORT = "port"
DB_TABLE_TERMINAL = "terminal"
DB_TABLE_BERTH = "berth"
DB_TABLE_COUNTRY = "country"
DB_TABLE_POSITION = "position"
DB_TABLE_DESTINATION = "destination"
DB_TABLE_TRAJECTORY = "trajectory"
DB_TABLE_SHIPMENT = "shipment"
DB_TABLE_SHIPMENTARRIVALBERTH = "shipmentarrivalberth"
DB_TABLE_SHIPMENTDEPARTUREBERTH = "shipmentdepartureberth"
DB_TABLE_PRICE = "price"
DB_TABLE_PRICE_SCENARIO = "price_scenario"
DB_TABLE_COMMODITY = "commodity"
DB_TABLE_ENTSOGFLOW = "entsogflow"
DB_TABLE_ENTSOGFLOW_RAW = "entsogflow_raw"
DB_TABLE_MARINETRAFFICCALL = "mtcall"
DB_TABLE_CURRENCY = "currency"
DB_TABLE_MTEVENT_TYPE = "mtevent_type"
DB_TABLE_EVENT = "event"
DB_TABLE_SHIPMENT_WITH_STS = "shipment_with_sts"
DB_TABLE_STS_LOCATIONS = "sts_locations"
DB_TABLE_STSDEPARTURELOCATION = "shipmentdeparturelocationsts"
DB_TABLE_STSARRIVALLOCATION = "shipmentarrivallocationsts"

DB_TABLE_PIPELINEFLOW = "pipelineflow"
DB_TABLE_COUNTER = "counter"
DB_TABLE_ENDPOINTCACHE = "endpoint_cache"

# Marine Traffic cache
DB_TABLE_MTVOYAGEINFO = "voyageinfo"

DB_TABLE_ALERT_INSTANCE = "alert_instance"
DB_TABLE_ALERT_CONFIG = "alert_config"
DB_TABLE_ALERT_RECIPIENT = "alert_recipient"
DB_TABLE_ALERT_RECIPIENT_ASSOC = "alert_recipient_assoc"
DB_TABLE_ALERT_CRITERIA = "alert_criteria"
DB_TABLE_ALERT_CRITERIA_ASSOC = "alert_criteria_assoc"


# Flaring related tables
DB_TABLE_FLARING_FACILITY = "flaring_facility"
DB_TABLE_FLARING = "flaring"
DB_TABLE_FLARING_ANOMALY = "flaring_anomaly"
DB_TABLE_FLARING_ANOMALY_ALGORITHM = "flaring_anomaly_algorithm"

# Kpler data
DB_TABLE_KPLER_PRODUCT = "kpler_product"
DB_TABLE_KPLER_FLOW = "kpler_flow"
DB_TABLE_KPLER_VESSEL = "kpler_vessel"
DB_TABLE_KPLER_TRADE = "kpler_trade"
DB_TABLE_KPLER_INSTALLATION = "kpler_installation"
DB_TABLE_KPLER_ZONE = "kpler_zone"


from .models import *
