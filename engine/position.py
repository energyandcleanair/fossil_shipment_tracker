from geoalchemy2.functions import ST_MakeLine, ST_Multi, ST_Union, ST_Distance, ST_ClusterDBSCAN, ST_Centroid, ST_Transform
from sqlalchemy import func
import sqlalchemy as sa
import geopandas as gpd
import os


from engine.datalastic import Datalastic
from base.db import session
import datetime as dt
import base
from base.logger import logger_slack
from base.utils import to_list, to_datetime, update_geometry_from_wkb
from base.models import Ship, Departure, Shipment, Position, Arrival, Port, Destination, Berth, ShipmentArrivalBerth
from tqdm import tqdm
from difflib import SequenceMatcher
import pandas as pd


from base.db_utils import execute_statement, upsert
from base.models import DB_TABLE_POSITION


def get(imo, date_from, date_to):
    positions = Datalastic.get_positions(imo=imo, date_from=date_from, date_to=date_to)
    return positions


def update_shipment_last_position():

    # add last_position to shipment table for faster retrieval

    shipment_next_departure_date = session.query(
        Shipment.id,
        func.lead(Departure.date_utc).over(
            Departure.ship_imo,
            Departure.date_utc).label('date_utc')) \
    .join(Departure, Departure.id == Shipment.departure_id).subquery()


    shipments_w_last_position = session.query(Shipment.id,
                                          Position.id.label('position_id'),
                                          Position.destination_name,
                                          Position.destination_port_id
                                          ) \
        .join(Departure, Departure.id == Shipment.departure_id) \
        .outerjoin(shipment_next_departure_date, shipment_next_departure_date.c.id == Shipment.id) \
        .outerjoin(Arrival, Arrival.id == Shipment.arrival_id) \
        .join(Position, Position.ship_imo == Departure.ship_imo) \
        .filter(
            sa.and_(
                Position.date_utc >= Departure.date_utc,
                sa.or_(Arrival.date_utc == sa.null(),
                       Position.date_utc < Arrival.date_utc),
                sa.or_(shipment_next_departure_date.c.date_utc == sa.null(),
                       Position.date_utc < shipment_next_departure_date.c.date_utc)
            )) \
        .distinct(Shipment.id) \
        .order_by(Shipment.id, Position.date_utc.desc()) \
        .subquery()

    update = Shipment.__table__.update().values(last_position_id=shipments_w_last_position.c.position_id) \
        .where(Shipment.__table__.c.id == shipments_w_last_position.c.id)
    execute_statement(update)


def update(commodities=None,
           imo=None,
           shipment_id=None,
           date_from=None,
           shipment_status=None,
           force_for_those_without_destination=False):

    logger_slack.info("=== Position update ===")
    buffer = dt.timedelta(hours=24)
    # We update position which are still ongoing (no arrival yet)
    # or who are still missing some positions (should have til Arrival + n hours, and from Departure - n_hours)

    shipments_positions = session.query(Shipment.id.label('shipment_id'),
                                    # Departure.ship_imo.label('ship_imo'),
                                    # Departure.date_utc.label('departure_date'),
                                    # Arrival.date_utc.label('arrival_date'),
                                    # Ship.commodity.label('commodity'),
                                    Position.date_utc.label('position_date')
                                    ) \
        .join(Departure, Shipment.departure_id == Departure.id) \
        .outerjoin(Arrival, Shipment.arrival_id == Arrival.id) \
        .join(Ship, Ship.imo == Departure.ship_imo) \
        .outerjoin(Position, Position.ship_imo == Departure.ship_imo) \
        .filter(Position.date_utc >= Departure.date_utc - dt.timedelta(
            hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE) - buffer) \
        .filter(sa.or_(
            Arrival.date_utc == sa.null(),
            Position.date_utc <= Arrival.date_utc + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL) + buffer)) \
        .filter(sa.or_(
            not force_for_those_without_destination,
            Position.destination_name != sa.null())) \
        .subquery()


    shipments_to_update = session.query(Shipment.id,
                                    Departure.ship_imo,
                                    Departure.date_utc.label('departure_date'),
                                    Arrival.date_utc.label('arrival_date'),
                                    Ship.commodity,
                                    func.min(shipments_positions.c.position_date).label('first_date'),
                                    func.max(shipments_positions.c.position_date).label('last_date')
                                    ) \
        .outerjoin(shipments_positions, Shipment.id == shipments_positions.c.shipment_id) \
        .join(Departure, Shipment.departure_id == Departure.id) \
        .outerjoin(Arrival, Shipment.arrival_id == Arrival.id) \
        .join(Ship, Ship.imo == Departure.ship_imo) \
        .group_by(Shipment.id, Departure.ship_imo, Departure.date_utc, Arrival.date_utc, Ship.commodity) \
        .having(sa.or_(
                        sa.and_(Arrival.date_utc == sa.null(),
                                func.max(shipments_positions.c.position_date) < dt.datetime.utcnow() - dt.timedelta(hours=12)), # To prevent too much refreshing
                        sa.or_(
                                   func.min(shipments_positions.c.position_date) == sa.null(),
                                   func.max(shipments_positions.c.position_date) < Arrival.date_utc + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL),
                                   func.min(shipments_positions.c.position_date) > Departure.date_utc - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE)
                                   )
                               )
                ) \
        .filter(Shipment.status != base.UNDETECTED_ARRIVAL)

    if shipment_id is not None:
        shipments_to_update = shipments_to_update.filter(Shipment.id.in_(to_list(shipment_id)))

    if imo is not None:
        shipments_to_update = shipments_to_update.filter(Ship.imo.in_(to_list(imo)))

    if commodities is not None:
        shipments_to_update = shipments_to_update.filter(Ship.commodity.in_(to_list(commodities)))

    if shipment_status is not None:
        shipments_to_update = shipments_to_update.filter(Shipment.status.in_(to_list(shipment_status)))

    if date_from is not None:
        shipments_to_update = shipments_to_update.filter(Departure.date_utc >= (to_datetime(date_from)))

    shipments_to_update = shipments_to_update.order_by(Departure.date_utc.desc()).all()
    # Add positions
    for f in tqdm(shipments_to_update):
        shipment_id = f[0]
        ship_imo = f[1]
        departure_date = f[2]
        arrival_date = f[3] if f[3] is not None else dt.datetime.utcnow()
        first_date = f[5]
        last_date = f[6]
        # Add a bit of buffer hours, so that next time, we don't update the shipments
        date_from = departure_date - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE)
        date_to = arrival_date + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL)

        dates = []
        if first_date is None:
            # No position found, we query the whole voyage
            dates.append({"date_from": date_from - buffer, "date_to": date_to + buffer})
        else:
            # We only query head or tail or both
            if first_date > date_from:
                dates.append({"date_from": date_from - buffer, "date_to": first_date})
            if last_date < date_to:
                dates.append({"date_from": last_date, "date_to": date_to + buffer})

        for date in dates:
            positions = get(imo=ship_imo, **date)
            if positions:
                positions_df = pd.DataFrame([x.__dict__ for x in positions])
                positions_df.drop('_sa_instance_state', axis=1, inplace=True)
                upsert(positions_df, table=DB_TABLE_POSITION, constraint_name='unique_position', show_progress=False)

    update_shipment_last_position()


def get_shipment_positions():
    ordered_positions = session.query(Shipment.id.label('shipment_id'),
                                      Ship.commodity,
                                      Ship.type,
                                      Ship.subtype,
                                      Ship.name,
                                      Ship.imo,
                                      Ship.dwt,
                                      Position,
                                      Arrival.date_utc.label('arrival_date'),
                                      ) \
        .join(Departure, Departure.id == Shipment.departure_id) \
        .join(Arrival, Arrival.id == Shipment.arrival_id) \
        .join(Position, Position.ship_imo == Departure.ship_imo) \
        .join(Ship, Ship.imo == Position.ship_imo) \
        .filter(
            sa.and_(
                Position.date_utc >= Departure.date_utc,
                sa.or_(
                    Position.date_utc <= Arrival.date_utc,
                    Arrival.date_utc == sa.null()
                )
            )) \
        .order_by(Shipment.id, Position.date_utc)

    return ordered_positions


def get_missing_berths(max_speed=0.5,
                       date_from=None,
                       date_to=None,
                       commodity=None,
                       exclude_in_berth=True,
                       do_cluster=True,
                       only_one_per_shipment=False,
                       cluster_m=50,
                       hours_from_arrival=72,
                       format='kml',
                       export_file='missing_berths.kml'):

    positions = get_shipment_positions()

    if max_speed:
        positions = positions.filter(sa.or_(
            Position.speed <= max_speed,
            Position.speed == sa.null()))

    if date_from:
        positions = positions.filter(Position.date_utc >= to_datetime(date_from))

    if date_to:
        positions = positions.filter(Position.date_utc <= to_datetime(date_to))

    if commodity:
        positions = positions.filter(Ship.commodity.in_(to_list(commodity)))

    if exclude_in_berth:
        positions = positions\
            .outerjoin(Berth, func.ST_Contains(Berth.geometry, Position.geometry)) \
            .filter(Berth.id == sa.null())
        positions = positions \
            .outerjoin(ShipmentArrivalBerth, Shipment.id == ShipmentArrivalBerth.shipment_id) \
            .filter(ShipmentArrivalBerth.id == sa.null())

    if hours_from_arrival:
        positions = positions \
            .filter(Position.date_utc >= Arrival.date_utc - dt.timedelta(hours=hours_from_arrival)) \
            .filter(Position.date_utc <= Arrival.date_utc + dt.timedelta(hours=hours_from_arrival))

    if do_cluster and cluster_m:
        positions = cluster(positions=positions.subquery(),
                            cluster_m=cluster_m,
                            only_one_per_shipment=only_one_per_shipment)

    positions_df = pd.read_sql(positions.statement, session.bind)
    positions_df = update_geometry_from_wkb(positions_df)
    result_gdf = gpd.GeoDataFrame(positions_df, geometry='geometry')

    if format == "kml":
        import fiona
        import io
        fiona.supported_drivers['KML'] = 'rw'
        if os.path.exists(export_file):
            os.remove(export_file)
        result_gdf.to_file(export_file, driver='KML')


def cluster(positions, cluster_m=50, only_one_per_shipment=False):

        # buffer_deg=0.005 roughly divide the number of points by 2
        clustered_points = session.query(
            positions.c.shipment_id,
            positions.c.speed,
            positions.c.navigation_status,
            positions.c.imo,
            positions.c.type,
            positions.c.subtype,
            positions.c.date_utc,
            positions.c.arrival_date,
            positions.c.geometry,
            ST_ClusterDBSCAN(ST_Transform(positions.c.geometry, 3857), cluster_m, 1) \
                .over(partition_by=positions.c.shipment_id) \
                .label('cluster')) \
            .subquery()

        # Cluster can only happen with consecutive points
        # we force another cluster if this is not the case
        clustered_points2 = session.query(clustered_points.c.shipment_id,
                                          clustered_points.c.imo,
                                          clustered_points.c.subtype,
                                          clustered_points.c.geometry,
                                          clustered_points.c.date_utc,
                                          clustered_points.c.arrival_date,
                                          sa.case([
                                              (func.lag(clustered_points.c.cluster).over(
                                                  partition_by=clustered_points.c.shipment_id,
                                                  order_by=clustered_points.c.date_utc) <= clustered_points.c.cluster,
                                               clustered_points.c.cluster)],
                                              else_=-1 * clustered_points.c.cluster).label('cluster')) \
            .subquery()

        clustered_points3 = session.query(clustered_points2.c.shipment_id,
                                          clustered_points2.c.imo,
                                          clustered_points2.c.subtype,
                                          clustered_points2.c.arrival_date,
                                          func.min(clustered_points2.c.date_utc).label("date_utc"),
                                          func.count(clustered_points2.c.date_utc).label("count"),
                                          ST_Centroid(ST_Union(clustered_points2.c.geometry)).label("geometry")) \
            .group_by(clustered_points2.c.shipment_id,
                      clustered_points2.c.imo,
                      clustered_points2.c.subtype,
                      clustered_points2.c.cluster,
                      clustered_points2.c.arrival_date) \
            .subquery()



        if only_one_per_shipment:
            clustered_count = session.query(func.max(clustered_points3.c.count).label('count'),
                                            clustered_points3.c.shipment_id) \
                .group_by(clustered_points3.c.shipment_id) \
                .subquery()

            clustered_points3 = session.query(clustered_points3) \
                .join(clustered_count, sa.and_(
                                clustered_points3.c.shipment_id == clustered_count.c.shipment_id,
                                clustered_points3.c.count == clustered_count.c.count)) \
                .subquery()


        clustered_points4 = session.query(clustered_points3) \
            .order_by(clustered_points3.c.shipment_id, clustered_points3.c.date_utc)


        return clustered_points4



