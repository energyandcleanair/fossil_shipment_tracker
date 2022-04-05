import pandas as pd
import geopandas as gpd
import shapely
from geoalchemy2 import func
import sqlalchemy as sa
import datetime as dt

import base
from base.db import session, engine
from base.logger import logger
from base.db_utils import upsert
from base.models import Berth, Port, Flow, FlowArrivalBerth, FlowDepartureBerth, Position, Arrival, Departure
from base.models import DB_TABLE_BERTH, DB_TABLE_FLOWARRIVALBERTH, DB_TABLE_FLOWDEPARTUREBERTH
from base.utils import to_list

from engine import port

def count():
    return session.query(Berth).count()


def fill():
    """
    Fill berth data from prepared files
    :return:
    """
    berths_gdf = gpd.read_file("assets/berths/berths_joined.geojson")

    # ports_gdf = gpd.GeoDataFrame(ports_df, geometry=gpd.points_from_xy(ports_df.lon, ports_df.lat), crs="EPSG:4326")
    berths_gdf = berths_gdf[["id", "name", "port_unlocode", "commodity", "geometry"]]

    # Remove z dimension
    def remove_z(geom):
        return shapely.wkb.loads(shapely.wkb.dumps(geom, output_dimension=2))
    berths_gdf["geometry"] = berths_gdf.geometry.apply(remove_z)

    # Check that all ports are there
    ports = berths_gdf.port_unlocode.unique().tolist()
    existing_ports = [x[0] for x in session.query(Port.unlocode).all()]
    missing_ports = [x for x in ports if x not in existing_ports and x is not None]

    for missing_port in missing_ports:
        port.insert_new_port(iso2=missing_port[0:2],
                             unlocode=missing_port)

    upsert(df=berths_gdf, table=DB_TABLE_BERTH, constraint_name="berth_pkey")
    return

def detect_berths(flow_id=None):
    print("=== Detect berths ===")
    detect_departure_berths(flow_id=flow_id)
    detect_arrival_berths(flow_id=flow_id)
    return


def detect_departure_berths(flow_id=None, min_hours_at_berth=4):
    # Look for flows to update
    flows_to_update = session.query(Flow.id).filter(Flow.id.notin_(session.query(FlowDepartureBerth.flow_id)))

    if flow_id is not None:
        flow_id = to_list(flow_id)
        flows_to_update = flows_to_update.filter(Flow.id.in_(flow_id))

    berths = session.query(Flow.id, Berth.id, Position.id, Position.date_utc,
                           Position.navigation_status, Position.speed,
                           Berth.port_unlocode, Departure.port_id) \
        .filter(Flow.id.in_(flows_to_update)) \
        .filter(sa.or_(
            Position.navigation_status == "Moored",
            Position.speed < 0.5)) \
        .join(Departure, Flow.departure_id == Departure.id) \
        .join(Position, Position.ship_imo == Departure.ship_imo) \
        .join(Arrival, Flow.arrival_id == Arrival.id) \
        .join(Berth, func.ST_Contains(Berth.geometry, Position.geometry)) \
        .filter(Position.date_utc >= Departure.date_utc - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE)) \
        .filter(Position.date_utc <= Arrival.date_utc + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL)) \
        .filter((Arrival.date_utc - Position.date_utc) > (Position.date_utc - Departure.date_utc)) \
        .order_by(Flow.id, Berth.id, Position.date_utc) \
        .distinct(Flow.id, Berth.id, Position.id, Position.date_utc)

    berths_df = pd.read_sql(berths.statement, session.bind)

    berths_df.columns = ["flow_id", "berth_id", "position_id", "position_date_utc",
                         "navigation_status", "speed", "berth_port_unlocode",
                         "departure_port_id"]

    berths_df["has_moored"] = berths_df.navigation_status == "Moored"

    # They should stay minimum n-hours if not moored or stopped
    berths_agg = berths_df \
        .sort_values(["flow_id", "berth_id", 'position_date_utc']) \
        .groupby(["flow_id", "berth_id"]) \
        .agg(
             has_moored=('has_moored', 'max'),
             min_speed=('speed', 'min'),
             min_date_utc=('position_date_utc', 'min'),
             max_date_utc=('position_date_utc', 'max'),
             position_id=('position_id', 'last')) \
        .reset_index()

    if len(berths_agg) == 0:
        return None

    berths_agg_ok = berths_agg.loc[
        ((berths_agg.max_date_utc - berths_agg.min_date_utc) > dt.timedelta(hours=min_hours_at_berth)) \
        | (berths_agg.has_moored) \
        | (berths_agg.min_speed == 0)].copy()

    # Look for problematic ones
    # problematic = berths_agg_ok.loc[berths_df.berth_port_unlocode != berths_df.arrival_port_unlocode].copy()
    # if len(problematic) > 0:
    #     logger.warning("There are problematic matching (e.g. different unlocode between berth and port")

    # Maximum one berthing per flow
    if berths_agg_ok.groupby(['flow_id'])["berth_id"].count().max() > 1:
        raise ValueError("Found more than one berth for a flow")

    berths_agg_ok["method_id"] = "simple_overlapping"
    berths_agg_ok = berths_agg_ok[["flow_id", "berth_id", "position_id", "method_id"]]
    upsert(df=berths_agg_ok, table=DB_TABLE_FLOWDEPARTUREBERTH, constraint_name='unique_flowdepartureberth')
    return


def detect_arrival_berths(flow_id=None, min_hours_at_berth=4):

    # Look for flows to update
    flows_to_update = session.query(Flow.id).filter(Flow.id.notin_(session.query(FlowArrivalBerth.flow_id)))

    if flow_id is not None:
        flow_id = to_list(flow_id)
        flows_to_update = flows_to_update.filter(Flow.id.in_(flow_id))

    berths = session.query(Flow.id, Berth.id, Position.id, Position.date_utc, Berth.port_unlocode, Arrival.port_id) \
        .filter(Flow.id.in_(flows_to_update)) \
        .filter(sa.or_(
            Position.navigation_status == "Moored",
            Position.speed < 0.5)) \
        .join(Departure, Flow.departure_id == Departure.id) \
        .join(Position, Position.ship_imo == Departure.ship_imo) \
        .join(Arrival, Flow.arrival_id == Arrival.id) \
        .join(Berth, func.ST_Contains(Berth.geometry, Position.geometry)) \
        .filter(Position.date_utc >= Departure.date_utc - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE)) \
        .filter(Position.date_utc <= Arrival.date_utc + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL)) \
        .filter((Arrival.date_utc - Position.date_utc) < (Position.date_utc - Departure.date_utc)) \
        .order_by(Flow.id, Berth.id, Position.date_utc) \
        .distinct(Flow.id, Berth.id, Position.id, Position.date_utc)

    berths_df = pd.read_sql(berths.statement,
                            session.bind)

    berths_df.columns = ["flow_id", "berth_id", "position_id", "position_date_utc", "berth_port_unlocode",
                         "arrival_port_id"]

    # They should stay minimum n-hours
    berths_agg = berths_df \
        .sort_values(["flow_id", "berth_id", 'position_date_utc']) \
        .groupby(["flow_id", "berth_id"]) \
        .agg(min_date_utc=('position_date_utc', 'min'),
             max_date_utc=('position_date_utc', 'max'),
             position_id=('position_id', 'first')) \
        .reset_index()

    if len(berths_agg) == 0:
        return None

    berths_agg_ok = berths_agg.loc[
        (berths_agg.max_date_utc - berths_agg.min_date_utc) > dt.timedelta(hours=min_hours_at_berth)]

    # Look for problematic ones
    # problematic = berths_agg_ok.loc[berths_df.berth_port_unlocode != berths_df.arrival_port_unlocode].copy()
    # if len(problematic) > 0:
    #     logger.warning("There are problematic matching (e.g. different unlocode between berth and port")

    berths_agg_ok["method_id"] = "simple_overlapping"
    berths_agg_ok = berths_agg_ok[["flow_id", "berth_id", "position_id", "method_id"]]
    upsert(df=berths_agg_ok, table=DB_TABLE_FLOWARRIVALBERTH, constraint_name='unique_flowarrivalberth')
    return
