import pandas as pd
import geopandas as gpd
import shapely
from geoalchemy2 import func
import sqlalchemy
import datetime as dt

from base.db import session, engine
from base.logger import logger
from base.db_utils import upsert
from base.models import Berth, Port, Flow, FlowArrivalBerth, FlowDepartureBerth, Position, Arrival, Departure
from base.models import DB_TABLE_BERTH, DB_TABLE_FLOWARRIVALBERTH, DB_TABLE_FLOWDEPARTUREBERTH

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

def detect_berths():
    detect_departure_berths()
    detect_arrival_berths()
    return


def detect_departure_berths():
    # Look for flows to update
    flows_to_update = session.query(Flow.id).filter(Flow.id.notin_(session.query(FlowDepartureBerth.flow_id)))

    berths = session.query(Flow.id, Berth.id, Position.id, Berth.port_unlocode, Departure.port_unlocode) \
        .filter(Flow.id.in_(flows_to_update)) \
        .filter(Position.navigation_status == "Moored") \
        .join(Position, Position.flow_id == Flow.id) \
        .join(Departure, Flow.departure_id == Departure.id) \
        .join(Arrival, Flow.arrival_id == Arrival.id) \
        .join(Berth, func.ST_Contains(Berth.geometry, Position.geometry)) \
        .filter((Arrival.date_utc - Position.date_utc) > (Position.date_utc - Departure.date_utc)) \
        .order_by(Flow.id, Berth.id, Position.date_utc.desc()) \
        .distinct(Flow.id, Berth.id)

    berths_df = pd.read_sql(berths.statement,
                            session.bind)

    berths_df.columns = ["flow_id", "berth_id", "position_id", "berth_port_unlocode", "departure_port_unlocode"]

    # Look for problematic ones
    problematic = berths_df.loc[berths_df.berth_port_unlocode != berths_df.departure_port_unlocode]
    if len(problematic) > 0:
        logger.warning("There are problematic matching (e.g. different unlocode between berth and port")

    berths_df["method_id"] = "simple_overlapping"
    berths_df = berths_df[["flow_id", "berth_id", "position_id", "method_id"]]
    upsert(df=berths_df, table=DB_TABLE_FLOWDEPARTUREBERTH, constraint_name='unique_flowdepartureberth')
    return


def detect_arrival_berths(min_hours_at_berth=4):

    # Look for flows to update
    flows_to_update = session.query(Flow.id).filter(Flow.id.notin_(session.query(FlowArrivalBerth.flow_id)))

    berths = session.query(Flow.id, Berth.id, Position.id, Position.date_utc, Berth.port_unlocode, Arrival.port_id) \
        .filter(Flow.id.in_(flows_to_update)) \
        .filter(Position.navigation_status == "Moored") \
        .join(Position, Position.flow_id == Flow.id) \
        .join(Departure, Flow.departure_id == Departure.id) \
        .join(Arrival, Flow.arrival_id == Arrival.id) \
        .join(Berth, func.ST_Contains(Berth.geometry, Position.geometry)) \
        .filter((Arrival.date_utc - Position.date_utc) < (Position.date_utc - Departure.date_utc)) \
        .order_by(Flow.id, Berth.id, Position.date_utc) \
        .distinct(Flow.id, Berth.id, Position.id, Position.date_utc)

    berths_df = pd.read_sql(berths.statement,
                            session.bind)

    berths_df.columns = ["flow_id", "berth_id", "position_id", "position_date_utc", "berth_port_unlocode",
                         "arrival_port_id"]

    # They should stay minimum n-hours
    berths_agg = berths_df \
        .sort_values(["flow_id", "berth_id", "berth_port_unlocode", "arrival_port_id", 'position_date_utc']) \
        .groupby(["flow_id", "berth_id", "berth_port_unlocode", "arrival_port_id"]) \
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
