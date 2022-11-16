import tqdm
from geoalchemy2 import Geometry, func
import geopandas as gpd
import pandas as pd
import shapely
import sqlalchemy as sa
from fiona.drvsupport import supported_drivers

from base.logger import logger, logger_slack
from base.db import session, engine
from base.models import Berth, Port, Shipment, ShipmentWithSTS, ShipmentArrivalBerth, ShipmentDepartureBerth, \
    Departure, ShipmentDepartureLocationSTS, ShipmentArrivalLocationSTS, Event, STSLocation, Arrival

from base.utils import update_geometry_from_wkb, to_list
from base.db_utils import upsert

from base.models import DB_TABLE_BERTH, DB_TABLE_STS_LOCATIONS, DB_TABLE_STSDEPARTURELOCATION, \
    DB_TABLE_STSARRIVALLOCATION

from engine import portcall

def fill_portcalls_around_sts():
    """
    The purpose of this function is to find the first preceeding and proceeding portcall for sts events

    Returns
    -------

    """
    sts_shipments = session.query(
            ShipmentWithSTS.id,
            Departure.ship_imo,
            Event.date_utc,
            Event.id) \
        .join(Departure, Departure.id == ShipmentWithSTS.departure_id) \
        .outerjoin(Arrival, Arrival.id == ShipmentWithSTS.arrival_id) \
        .outerjoin(Event, Event.id == Arrival.event_id) \
        .filter(Departure.event_id == sa.null()).all()

    for shipment in tqdm.tqdm(sts_shipments):
        portcall.get_next_portcall(imo = shipment.ship_imo,
                                   date_from=shipment.date_utc,
                                   arrival_or_departure=None)


def update():
    """
    Update departure and arrival STS locations

    Returns
    -------

    """
    detect_sts_departure_location()
    detect_sts_arrival_location()


def generate_geojson():
    """
    Convert KML to geojson file for filling db

    Returns
    -------

    """

    supported_drivers['KML'] = 'rw'
    # gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'

    sts_gdf = gpd.read_file('assets/sts_locations/STS Areas.kml', driver='KML')
    sts_gdf.insert(0, 'id', range(0, len(sts_gdf)))

    sts_gdf.rename(columns={'Name': 'name'}, inplace=True)

    sts_gdf.to_file("assets/sts_locations/sts_areas.geojson", driver='GeoJSON')


def fill():
    """
    Fill sts_locations data with prepared locations map

    Returns
    -------

    """

    sts_gdf = gpd.read_file("assets/sts_locations/sts_areas.geojson")

    sts_gdf = sts_gdf[["id", "name", "geometry"]]

    # Remove z dimension
    def remove_z(geom):
        return shapely.wkb.loads(shapely.wkb.dumps(geom, output_dimension=2))

    sts_gdf["geometry"] = sts_gdf.geometry.apply(remove_z)

    sts_df = pd.DataFrame(sts_gdf)
    sts_df = update_geometry_from_wkb(sts_df, to="wkt")

    upsert(df=sts_df,
           table=DB_TABLE_STS_LOCATIONS,
           constraint_name="sts_locations_pkey",
           dtype={'geometry': Geometry('GEOMETRY', 4326)})
    return


def detect_sts_departure_location(shipment_id=None):
    """

    Parameters
    ----------
    shipment_id :

    Returns
    -------

    """

    # Look for shipments to update
    shipments_to_update = session.query(ShipmentWithSTS.id).filter(
        ShipmentWithSTS.id.notin_(session.query(ShipmentDepartureLocationSTS.shipment_id)))

    if shipment_id is not None:
        shipment_id = to_list(shipment_id)
        shipments_to_update = shipments_to_update.filter(ShipmentWithSTS.id.in_(shipment_id))

    locations = session.query(ShipmentWithSTS.id.label('shipment_id'),
                              STSLocation.id.label('sts_location_id'),
                              Event.id.label('event_id')
                              ) \
        .filter(ShipmentWithSTS.id.in_(shipments_to_update)) \
        .join(Departure, Departure.id == ShipmentWithSTS.departure_id) \
        .filter(Departure.event_id != sa.null()) \
        .join(Event, Event.id == Departure.event_id) \
        .join(STSLocation, func.ST_Contains(STSLocation.geometry, Event.ship_closest_position)) \
        .order_by(ShipmentWithSTS.id, STSLocation.id, Event.date_utc) \
        .distinct(ShipmentWithSTS.id, STSLocation.id, Event.date_utc)

    locations_df = pd.read_sql(locations.statement, session.bind)

    # Maximum one location per shipment - in the future we can relax this with meta levels (i.e. laconian gulf -> med)
    locations_count = locations_df.groupby(['shipment_id'])["sts_location_id"].count().reset_index()
    if locations_count.sts_location_id.max() > 1:
        logger.warning("Found more than one departure location for a shipment. Keeping latest one")
        locations_df = locations_df \
            .sort_values(["shipment_id", "sts_location_id"], ascending=True) \
            .drop_duplicates(['shipment_id'], keep="last")

    locations_df["method_id"] = "simple_overlapping"
    locations_df = locations_df[["shipment_id", "sts_location_id", "event_id", "method_id"]]
    upsert(df=locations_df, table=DB_TABLE_STSDEPARTURELOCATION, constraint_name='unique_shipmentstsdeparturelocation')
    return


def detect_sts_arrival_location(shipment_id=None):
    """

    Parameters
    ----------
    shipment_id :

    Returns
    -------

    """

    # Look for shipments to update
    shipments_to_update = session.query(ShipmentWithSTS.id).filter(
        ShipmentWithSTS.id.notin_(session.query(ShipmentArrivalLocationSTS.shipment_id)))

    if shipment_id is not None:
        shipment_id = to_list(shipment_id)
        shipments_to_update = shipments_to_update.filter(ShipmentWithSTS.id.in_(shipment_id))

    locations = session.query(ShipmentWithSTS.id.label('shipment_id'),
                              STSLocation.id.label('sts_location_id'),
                              Event.id.label('event_id')
                              ) \
        .filter(ShipmentWithSTS.id.in_(shipments_to_update)) \
        .join(Arrival, Arrival.id == ShipmentWithSTS.arrival_id) \
        .filter(Arrival.event_id != sa.null()) \
        .join(Event, Event.id == Arrival.event_id) \
        .join(STSLocation, func.ST_Contains(STSLocation.geometry, Event.ship_closest_position)) \
        .order_by(ShipmentWithSTS.id, STSLocation.id, Event.date_utc) \
        .distinct(ShipmentWithSTS.id, STSLocation.id, Event.date_utc)

    locations_df = pd.read_sql(locations.statement, session.bind)

    # Maximum one location per shipment - in the future we can relax this with meta levels (i.e. laconian gulf -> med)
    locations_count = locations_df.groupby(['shipment_id'])["sts_location_id"].count().reset_index()
    if locations_count.sts_location_id.max() > 1:
        logger.warning("Found more than one departure location for a shipment. Keeping latest one")
        locations_df = locations_df \
            .sort_values(["shipment_id", "sts_location_id"], ascending=True) \
            .drop_duplicates(['shipment_id'], keep="last")

    locations_df["method_id"] = "simple_overlapping"
    locations_df = locations_df[["shipment_id", "sts_location_id", "event_id", "method_id"]]
    upsert(df=locations_df, table=DB_TABLE_STSARRIVALLOCATION, constraint_name='unique_shipmentstsarrivallocation')
    return