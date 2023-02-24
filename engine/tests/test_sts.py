import base
from base.db import session, check_if_table_exists
from base.models import STSLocation, ShipmentArrivalLocationSTS, ShipmentDepartureLocationSTS, ShipmentWithSTS
from engine.sts import fill, generate_geojson, detect_sts_departure_location, detect_sts_arrival_location
from engine import shipment, arrival, sts, position, departure, company
from engine.marinetraffic import Marinetraffic
from engine.mtevents import back_fill_ship_position
import datetime as dt
import callbased


def test_company():
    company.update()

def test_callbased():
    callbased.update_arrivals(
        date_from='2021-01-01',
        ship_imo='9179945',
        commodities=[base.CRUDE_OIL, base.OIL_PRODUCTS, base.OIL_OR_CHEMICAL]
    )

def test_departure():
    departure.add()
def test_get_missing():
    #for country in ['HK', 'TR', 'CN', 'SG', 'IN', 'EG', 'AE', 'MY']:
    for country in ['HK']:
        position.get_missing_berths(export_file="missing_berhs_"+country+".kml",
                                    date_from='2021-01-01',
                                    sample=None,
                                    exclude_positions_in_berth=True,
                                    exclude_shipments_in_berth=True,
                                    arrival_iso2=country,
                                    cluster_m=300,
                                    commodity=[base.CRUDE_OIL, base.OIL_OR_CHEMICAL, base.OIL_PRODUCTS])
def test_find_multistage_sts():
    sts.check_multi_stage_sts()

def test_find_portcall():
    sts.fill_portcalls_around_sts()


def test_arrival_sts_shipment_update():
    sts_shipment_ids = [s.id for s in session.query(ShipmentWithSTS.id).all()]

    arrival.update(date_from='2021-09-01', date_to='2023-01-01', shipment_id=sts_shipment_ids)


def test_marinetraffic_get_position():
    # Marinetraffic.get_positions('9247481', date_from= '2022-07-01 11:30' , date_to= '2022-07-01 11:35', period='hourly')
    # d = Marinetraffic.get_closest_position(imo='9247481', date='2022-07-01 11:30')
    # assert d is not None
    back_fill_ship_position()


def test_update_shipment():
    shipment.update("2021-01-01")


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
