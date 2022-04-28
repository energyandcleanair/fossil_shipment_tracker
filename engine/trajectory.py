import datetime as dt
from sqlalchemy import func
from sqlalchemy import or_
import sqlalchemy as sa
from geoalchemy2.functions import ST_MakeLine, ST_Multi, ST_Union, ST_Distance, ST_ClusterDBSCAN, ST_Centroid

from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import text

import base
from base.models import Position, Trajectory, Shipment, Departure, Arrival, Ship, ShipmentArrivalBerth, ShipmentDepartureBerth
from engine import position
from base.db import session
from base.utils import to_list
from base.utils import wkb_to_shape, update_geometry_from_wkb
from base.db_utils import upsert
from base.models import DB_TABLE_TRAJECTORY
import pandas as pd
import geopandas as gpd
from geoalchemy2 import Geometry


def update(shipment_id=None, rebuild_all=False, do_cluster=True, cluster_deg=0.005):
    print("=== Trajectory update ===")
    DepartureBerthPosition = aliased(Position)
    ArrivalBerthPosition = aliased(Position)

    shipments_to_update = session.query(Shipment.id.label('shipment_id'),
                                    sa.func.greatest(
                                        Departure.date_utc - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE),
                                        DepartureBerthPosition.date_utc
                                    ).label('departure_date'),
                                    Departure.ship_imo.label('ship_imo'),
                                    sa.func.least(
                                        Arrival.date_utc + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL),
                                        ArrivalBerthPosition.date_utc
                                    ).label('arrival_date'),
                                    Trajectory.shipment_id
                                    ) \
        .outerjoin(Trajectory, Trajectory.shipment_id == Shipment.id) \
        .join(Departure, Shipment.departure_id == Departure.id) \
        .join(Arrival, Shipment.arrival_id == Arrival.id) \
        .outerjoin(ShipmentDepartureBerth, ShipmentDepartureBerth.shipment_id == Shipment.id) \
        .outerjoin(ShipmentArrivalBerth, ShipmentArrivalBerth.shipment_id == Shipment.id) \
        .outerjoin(DepartureBerthPosition, DepartureBerthPosition.id == ShipmentDepartureBerth.position_id) \
        .outerjoin(ArrivalBerthPosition, ArrivalBerthPosition.id == ShipmentArrivalBerth.position_id) \
        .filter(sa.or_(rebuild_all, Trajectory.shipment_id.is_(None)), Shipment.status==base.COMPLETED)

    if shipment_id is not None:
        shipments_to_update = shipments_to_update.filter(Shipment.id.in_(to_list(shipment_id)))

    shipments_to_update = shipments_to_update.subquery()
    ordered_positions = session.query(shipments_to_update.c.shipment_id,
                                      Position,
                                      # func.lag(Position.date_utc).over(
                                      #     Position.ship_imo,
                                      #     Position.date_utc
                                      # ).label('previous_date_utc'),
                                      # func.lag(Position.geometry).over(
                                      #     Position.ship_imo,
                                      #     Position.date_utc
                                      # ).label('previous_geometry')
                                      ) \
        .join(Position, Position.ship_imo == shipments_to_update.c.ship_imo) \
        .filter(
              sa.and_(
                  Position.date_utc >= shipments_to_update.c.departure_date,
                  Position.date_utc <= shipments_to_update.c.arrival_date
              )) \
        .order_by(shipments_to_update.c.shipment_id, Position.date_utc) \
        .subquery()

    if do_cluster:
        trajectories = cluster(ordered_positions, buffer_deg=cluster_deg)
    else:
        trajectories = session.query(ordered_positions.c.shipment_id.label("shipment_id"),
                                     ST_Multi(ST_MakeLine(ordered_positions.c.geometry)).label("geometry")) \
            .group_by(ordered_positions.c.shipment_id)

    # We split in different segments if two points are two distant (timewise for now)
    # max_hours = 48
    # max_deg = 5
    # segmented_positions = session.query(ordered_positions,
    #                                     ST_Distance(ordered_positions.c.geometry, ordered_positions.c.previous_geometry).label('distance'),
    #                                     sa.and_(
    #                                     ordered_positions.c.date_utc - ordered_positions.c.previous_date_utc > dt.timedelta(hours=max_hours),
    #                                     ST_Distance(ordered_positions.c.geometry, ordered_positions.c.previous_geometry) > max_deg
    #                                      ).label('new_segment')
    #
    #                                     ) \
    # .subquery()
    #
    # from sqlalchemy.sql.expression import text
    # segmented_positions2 = session.query(segmented_positions,
    #                                     text("""sum(new_segment::integer) over
    #                                     (order by shipment_id, date_utc
    #                                     rows between unbounded preceding and current row) as segment""")) \
    #     .subquery()

    # .group_by(segmented_positions2.c.shipment_id, text("coalesce(segment, -1)")) \


    #
    # trajectories_combined = session.query(trajectories.c.shipment_id,
    #                                       ST_Multi(ST_Union(trajectories.c.geometry)).label('geometry')) \
    #     .group_by(trajectories.c.shipment_id)


    trajectories_df = pd.read_sql(trajectories.statement, session.bind)
    trajectories_df = update_geometry_from_wkb(trajectories_df, to="shape")
    trajectories_df = gpd.GeoDataFrame(trajectories_df, geometry="geometry")
    trajectories_df = trajectories_df.loc[~trajectories_df.is_empty]
    trajectories_df = pd.DataFrame(trajectories_df)
    trajectories_df = update_geometry_from_wkb(trajectories_df, to="wkt")
    upsert(df=trajectories_df,
           table=DB_TABLE_TRAJECTORY,
           constraint_name="trajectory_shipment_id_key",
           dtype=({'geometry': Geometry('MULTILINESTRING', 4326)}))


def cluster(ordered_positions, buffer_deg=0.005):
    # buffer_deg=0.005 roughly divide the number of points by 2
    clustered_points = session.query(
        ordered_positions.c.shipment_id,
        ordered_positions.c.date_utc,
        ordered_positions.c.geometry,
        ST_ClusterDBSCAN(ordered_positions.c.geometry, buffer_deg, 1) \
            .over(partition_by=ordered_positions.c.shipment_id) \
            .label('cluster')) \
    .subquery()


    # Cluster can only happen with consecutive points
    # we force another cluster if this is not the case
    clustered_points2 = session.query(clustered_points.c.shipment_id,
                                      clustered_points.c.geometry,
                                      clustered_points.c.date_utc,
                                      sa.case([
                                          (func.lag(clustered_points.c.cluster).over(
                                          partition_by=clustered_points.c.shipment_id,
                                          order_by=clustered_points.c.date_utc) <= clustered_points.c.cluster, clustered_points.c.cluster)],
                                           else_= -1 * clustered_points.c.cluster).label('cluster')) \
        .subquery()


    clustered_points3 = session.query(clustered_points2.c.shipment_id,
                                      func.min(clustered_points2.c.date_utc).label("date_utc"),
                                      ST_Centroid(ST_Union(clustered_points2.c.geometry)).label("geometry")) \
    .group_by(clustered_points2.c.shipment_id, clustered_points2.c.cluster) \
    .subquery()

    clustered_points4 = session.query(clustered_points3) \
        .order_by(clustered_points3.c.shipment_id, clustered_points3.c.date_utc) \
        .subquery()

    #text('ST_Multi(st_makeline(geometry ORDER BY date_utc)) as geometry')

    trajectories = session.query(clustered_points4.c.shipment_id.label("shipment_id"),
                                 ST_Multi(ST_MakeLine(clustered_points4.c.geometry)) \
                                 .label("geometry")) \
        .group_by(clustered_points4.c.shipment_id)

    return trajectories



# clustered_points as (select shipment_id, geometry, date_utc,
# 					 ST_ClusterDBSCAN(geometry, eps := 0.01, minpoints := 1)
# 					 over (partition by (shipment_id, ship_imo)) as cluster
# from points),
#
# clustered_points2 as (
# 	select shipment_id, min(date_utc) as date_utc, st_centroid(st_union(geometry)) as geometry
# 	from clustered_points
# 	group by shipment_id, cluster
# 	order by date_utc
# )
#
#
#
# select shipment_id, st_makeline(geometry ORDER BY date_utc) from clustered_points
# group by shipment_id


