import sqlalchemy.sql.functions

from base.db import session, check_if_table_exists
from base.models import (
    STSLocation,
    ShipmentArrivalLocationSTS,
    ShipmentDepartureLocationSTS,
    ShipmentWithSTS,
)
from engines.sts import (
    fill,
    generate_geojson,
    detect_sts_departure_location,
    detect_sts_arrival_location,
)
from engines import shipment, arrival, sts
from engines.marinetraffic import Marinetraffic
from engines.mtevents import back_fill_ship_position
import datetime as dt


def test_find_multistage_sts():
    sts.check_multi_stage_sts(ship_imo="9762986")


def test_find_portcall():
    sts.fill_portcalls_around_sts(ship_imo="9762986")


def test_arrival_sts_shipment_update():
    sts_shipment_ids = [
        s.id
        for s in session.query(ShipmentWithSTS.id)
        .filter(ShipmentWithSTS.status == "ongoing")
        .order_by(sqlalchemy.sql.functions.random())
        .limit(2)
        .all()
    ]

    arrival.update(date_from="2021-09-01", date_to="2023-01-01", shipment_id=sts_shipment_ids)


def test_marinetraffic_get_position():
    back_fill_ship_position(event_id="3179")


def test_generate_geojson():
    generate_geojson()


def test_fill_table():
    fill()


def test_detect_locations():
    detect_sts_arrival_location()
    detect_sts_departure_location()
