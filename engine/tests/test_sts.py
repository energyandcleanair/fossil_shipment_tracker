from base.db import session, check_if_table_exists
from base.models import STSLocation, ShipmentArrivalLocationSTS, ShipmentDepartureLocationSTS
from engine.sts import fill, generate_geojson, detect_sts_departure_location, detect_sts_arrival_location

def test_table_exists():
    check_if_table_exists(STSLocation, create_table=True)
    check_if_table_exists(ShipmentDepartureLocationSTS, create_table=True)
    check_if_table_exists(ShipmentArrivalLocationSTS, create_table=True)

def test_generate_geojson():
    generate_geojson()

def test_fill_table():
    fill()

def test_detect_locations():
    detect_sts_arrival_location()
    detect_sts_departure_location()