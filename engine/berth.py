import pandas as pd
import geopandas as gpd
import shapely
from geoalchemy2 import func
import sqlalchemy as sa
import datetime as dt
from geoalchemy2 import Geometry

import base
from base.db import session, engine
from base.logger import logger, logger_slack
from base.db_utils import upsert
from base.models import (
    Berth,
    Port,
    Shipment,
    ShipmentArrivalBerth,
    ShipmentDepartureBerth,
    Position,
    Arrival,
    Departure,
)
from base.models import (
    DB_TABLE_BERTH,
    DB_TABLE_SHIPMENTARRIVALBERTH,
    DB_TABLE_SHIPMENTDEPARTUREBERTH,
)
from base.utils import to_list
from base.utils import update_geometry_from_wkb
from engine import port
from engine.shipment import return_combined_shipments


def update(shipment_id=None):
    logger_slack.info("=== Berth update ===")
    detect_departure_berths(shipment_id=shipment_id)
    detect_arrival_berths(shipment_id=shipment_id)
    logger_slack.info("=== Berth update done===")
    return


def count():
    return session.query(Berth).count()


def fill():
    """
    Fill berth data from prepared files
    :return:
    """
    berths_gdf = gpd.read_file("assets/berths/berths_joined.geojson")

    # ports_gdf = gpd.GeoDataFrame(ports_df, geometry=gpd.points_from_xy(ports_df.lon, ports_df.lat), crs="EPSG:4326")
    berths_gdf = berths_gdf[
        ["id", "name", "port_unlocode", "commodity", "owner", "geometry"]
    ]

    # Remove z dimension
    def remove_z(geom):
        return shapely.wkb.loads(shapely.wkb.dumps(geom, output_dimension=2))

    berths_gdf["geometry"] = berths_gdf.geometry.apply(remove_z)

    # Check that all ports are there
    ports = berths_gdf.port_unlocode.unique().tolist()
    existing_ports = [x[0] for x in session.query(Port.unlocode).all()]
    missing_ports = [x for x in ports if x not in existing_ports and x is not None]

    for missing_port in missing_ports:
        port.insert_new_port(iso2=missing_port[0:2], unlocode=missing_port)

    berths_df = pd.DataFrame(berths_gdf)
    berths_df = update_geometry_from_wkb(berths_df, to="wkt")

    upsert(
        df=berths_df,
        table=DB_TABLE_BERTH,
        constraint_name="berth_pkey",
        dtype={"geometry": Geometry("GEOMETRY", 4326)},
    )
    return


def detect_departure_berths(shipment_id=None, min_hours_at_berth=4, max_distance_deg=1):

    # Look for shipments to update
    shipments_all = return_combined_shipments(session)

    shipments_to_update = session.query(shipments_all.c.shipment_id).filter(
        shipments_all.c.shipment_id.notin_(
            session.query(ShipmentDepartureBerth.shipment_id)
        )
    )

    if shipment_id is not None:
        shipment_id = to_list(shipment_id)
        shipments_to_update = shipments_to_update.filter(
            shipments_all.c.shipment_id.in_(shipment_id)
        )

    berths = (
        session.query(
            shipments_all.c.shipment_id,
            Berth.id,
            Position.id,
            Position.date_utc,
            Position.navigation_status,
            Position.speed,
            Berth.port_unlocode,
            Departure.port_id,
            func.ST_distance(Port.geometry, Berth.geometry).label("distance_to_port"),
        )
        .filter(shipments_all.c.shipment_id.in_(shipments_to_update))
        .filter(sa.or_(Position.navigation_status == "Moored", Position.speed < 0.5))
        .join(Departure, shipments_all.c.shipment_departure_id == Departure.id)
        .join(Port, Departure.port_id == Port.id)
        .join(Position, Position.ship_imo == Departure.ship_imo)
        .outerjoin(Arrival, shipments_all.c.shipment_arrival_id == Arrival.id)
        .join(Berth, func.ST_Contains(Berth.geometry, Position.geometry))
        .filter(
            Position.date_utc
            >= Departure.date_utc
            - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE)
        )
        .filter(
            sa.or_(
                sa.and_(
                    Arrival.date_utc == sa.null(),
                    Position.date_utc
                    <= Departure.date_utc
                    + dt.timedelta(hours=base.BERTH_MAX_HOURS_AFTER_DEPARTURE),
                ),
                sa.and_(
                    Arrival.date_utc != sa.null(),
                    (Arrival.date_utc - Position.date_utc)
                    > (Position.date_utc - Departure.date_utc),
                ),
            )
        )
        .order_by(shipments_all.c.shipment_id, Berth.id, Position.date_utc)
        .distinct(shipments_all.c.shipment_id, Berth.id, Position.id, Position.date_utc)
    )

    berths_df = pd.read_sql(berths.statement, session.bind)

    berths_df.columns = [
        "shipment_id",
        "berth_id",
        "position_id",
        "position_date_utc",
        "navigation_status",
        "speed",
        "berth_port_unlocode",
        "departure_port_id",
        "distance_to_port",
    ]

    berths_df["has_moored"] = berths_df.navigation_status == "Moored"

    # They should stay minimum n-hours if not moored or stopped
    berths_agg = (
        berths_df.sort_values(["shipment_id", "berth_id", "position_date_utc"])
        .groupby(["shipment_id", "berth_id"])
        .agg(
            has_moored=("has_moored", "max"),
            min_speed=("speed", "min"),
            min_date_utc=("position_date_utc", "min"),
            max_date_utc=("position_date_utc", "max"),
            position_id=("position_id", "last"),
            distance_to_port=("distance_to_port", "min"),
        )
        .reset_index()
    )

    if len(berths_agg) == 0:
        return None

    berths_agg_ok = berths_agg.loc[
        (
            (berths_agg.max_date_utc - berths_agg.min_date_utc)
            > dt.timedelta(hours=min_hours_at_berth)
        )
        | (berths_agg.has_moored)
        | (berths_agg.min_speed == 0)
    ].copy()

    # For ports without geometry, the distance is nan. We keep them
    berths_agg_ok = berths_agg_ok[
        pd.isna(berths_agg_ok.distance_to_port)
        | (berths_agg_ok.distance_to_port <= max_distance_deg)
    ]

    if len(berths_agg) == 0:
        return None

    # Only keep moored if any
    berths_agg_ok = (
        berths_agg_ok.groupby(["shipment_id"])["has_moored"]
        .max()
        .reset_index()
        .merge(berths_agg_ok)
    )

    # Look for problematic ones
    # problematic = berths_agg_ok.loc[berths_df.berth_port_unlocode != berths_df.arrival_port_unlocode].copy()
    # if len(problematic) > 0:
    #     logger.warning("There are problematic matching (e.g. different unlocode between berth and port")
    # Maximum one berthing per shipment
    berths_count = (
        berths_agg_ok.groupby(["shipment_id"])["berth_id"].count().reset_index()
    )
    if berths_count.berth_id.max() > 1:
        logger.warning(
            "Found more than one departure berth for a shipment. Keeping latest one"
        )
        berths_agg_ok = berths_agg_ok.sort_values(
            ["shipment_id", "max_date_utc"]
        ).drop_duplicates(["shipment_id"], keep="last")

    berths_agg_ok["method_id"] = "simple_overlapping"
    berths_agg_ok = berths_agg_ok[
        ["shipment_id", "berth_id", "position_id", "method_id"]
    ]
    upsert(
        df=berths_agg_ok,
        table=DB_TABLE_SHIPMENTDEPARTUREBERTH,
        constraint_name="unique_shipmentdepartureberth",
    )
    return


def detect_arrival_berths(shipment_id=None, min_hours_at_berth=4, max_distance_deg=1):

    # Look for shipments to update
    shipments_all = return_combined_shipments(session)

    shipments_to_update = session.query(shipments_all.c.shipment_id).filter(
        shipments_all.c.shipment_id.notin_(
            session.query(ShipmentArrivalBerth.shipment_id)
        )
    )

    if shipment_id is not None:
        shipments_to_update = shipments_to_update.filter(
            shipments_all.c.shipment_id.in_(to_list(shipment_id))
        )

    berths = (
        session.query(
            shipments_all.c.shipment_id,
            Berth.id,
            Position.id,
            Position.date_utc,
            Position.navigation_status,
            Berth.port_unlocode,
            Arrival.port_id,
            func.ST_distance(Port.geometry, Berth.geometry).label("distance_to_port"),
        )
        .filter(shipments_all.c.shipment_id.in_(shipments_to_update))
        .filter(sa.or_(Position.navigation_status == "Moored", Position.speed < 0.5))
        .join(Departure, shipments_all.c.shipment_departure_id == Departure.id)
        .join(Position, Position.ship_imo == Departure.ship_imo)
        .join(Arrival, shipments_all.c.shipment_arrival_id == Arrival.id)
        .join(Port, Arrival.port_id == Port.id)
        .join(Berth, func.ST_Contains(Berth.geometry, Position.geometry))
        .filter(
            Position.date_utc
            >= Departure.date_utc
            - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE)
        )
        .filter(
            Position.date_utc
            <= Arrival.date_utc
            + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL)
        )
        .filter(
            (Arrival.date_utc - Position.date_utc)
            < (Position.date_utc - Departure.date_utc)
        )
        .order_by(shipments_all.c.shipment_id, Berth.id, Position.date_utc)
        .distinct(shipments_all.c.shipment_id, Berth.id, Position.id, Position.date_utc)
    )

    berths_df = pd.read_sql(berths.statement, session.bind)

    berths_df.columns = [
        "shipment_id",
        "berth_id",
        "position_id",
        "position_date_utc",
        "navigation_status",
        "berth_port_unlocode",
        "arrival_port_id",
        "distance_to_port",
    ]

    berths_df["has_moored"] = berths_df.navigation_status == "Moored"

    # They should stay minimum n-hours
    berths_agg = (
        berths_df.sort_values(["shipment_id", "berth_id", "position_date_utc"])
        .groupby(["shipment_id", "berth_id"])
        .agg(
            has_moored=("has_moored", "max"),
            min_date_utc=("position_date_utc", "min"),
            max_date_utc=("position_date_utc", "max"),
            position_id=("position_id", "first"),
            distance_to_port=("distance_to_port", "min"),
        )
        .reset_index()
    )

    if len(berths_agg) == 0:
        return None

    berths_agg_ok = berths_agg.loc[
        (berths_agg.max_date_utc - berths_agg.min_date_utc)
        > dt.timedelta(hours=min_hours_at_berth)
    ]

    # For ports without geometry, the distance is nan. We keep them
    berths_agg_ok = berths_agg_ok[
        pd.isna(berths_agg_ok.distance_to_port)
        | (berths_agg_ok.distance_to_port <= max_distance_deg)
    ]

    if len(berths_agg_ok) == 0:
        return None

    # Only keep moored if any
    berths_agg_ok = (
        berths_agg_ok.groupby(["shipment_id"])["has_moored"]
        .max()
        .reset_index()
        .merge(berths_agg_ok)
    )

    # Maximum one berthing per shipment
    berths_count = (
        berths_agg_ok.groupby(["shipment_id"])["berth_id"].count().reset_index()
    )
    if berths_count.berth_id.max() > 1:
        logger.warning(
            "Found more than one arrival berth for a shipment. Keeping earliest one"
        )
        berths_agg_ok = berths_agg_ok.sort_values(
            ["shipment_id", "min_date_utc"]
        ).drop_duplicates(["shipment_id"], keep="first")

    # berths_agg_ok = pd.DataFrame(berths_count["shipment_id"].loc[berths_count.berth_id == 1]) \
    #     .merge(berths_agg_ok)

    # Look for problematic ones
    # problematic = berths_agg_ok.loc[berths_df.berth_port_unlocode != berths_df.arrival_port_unlocode].copy()
    # if len(problematic) > 0:
    #     logger.warning("There are problematic matching (e.g. different unlocode between berth and port")

    berths_agg_ok["method_id"] = "simple_overlapping"
    berths_agg_ok = berths_agg_ok[
        ["shipment_id", "berth_id", "position_id", "method_id"]
    ]
    upsert(
        df=berths_agg_ok,
        table=DB_TABLE_SHIPMENTARRIVALBERTH,
        constraint_name="unique_shipmentarrivalberth",
    )
    return
