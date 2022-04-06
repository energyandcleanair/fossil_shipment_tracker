import datetime as dt
from sqlalchemy import func
from sqlalchemy import or_
import sqlalchemy as sa
from geoalchemy2.functions import ST_MakeLine

from sqlalchemy.orm import aliased

import base
from base.models import Position, Trajectory, Flow, Departure, Arrival, Ship, FlowArrivalBerth, FlowDepartureBerth
from engine import position
from base.db import session
from base.utils import to_list
from base.utils import wkb_to_shape, update_geometry_from_wkb
from base.db_utils import upsert
from base.models import DB_TABLE_TRAJECTORY
import pandas as pd
import geopandas as gpd
from geoalchemy2 import Geometry


def update(flow_id=None, rebuild_all=False):
    print("=== Trajectory update ===")
    DepartureBerthPosition = aliased(Position)
    ArrivalBerthPosition = aliased(Position)

    flows_to_update = session.query(Flow.id.label('flow_id'),
                                    sa.func.greatest(
                                        Departure.date_utc - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE),
                                        DepartureBerthPosition.date_utc
                                    ).label('departure_date'),
                                    Departure.ship_imo.label('ship_imo'),
                                    sa.func.least(
                                        Arrival.date_utc + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL),
                                        ArrivalBerthPosition.date_utc
                                    ).label('arrival_date'),
                                    Trajectory.flow_id
                                    ) \
        .outerjoin(Trajectory, Trajectory.flow_id == Flow.id) \
        .join(Departure, Flow.departure_id == Departure.id) \
        .join(Arrival, Flow.arrival_id == Arrival.id) \
        .outerjoin(FlowDepartureBerth, FlowDepartureBerth.flow_id == Flow.id) \
        .outerjoin(FlowArrivalBerth, FlowArrivalBerth.flow_id == Flow.id) \
        .outerjoin(DepartureBerthPosition, DepartureBerthPosition.id == FlowDepartureBerth.position_id) \
        .outerjoin(ArrivalBerthPosition, ArrivalBerthPosition.id == FlowArrivalBerth.position_id) \
        .filter(sa.or_(rebuild_all, Trajectory.flow_id.is_(None)), Flow.status==base.COMPLETED)

    if flow_id is not None:
        flows_to_update = flows_to_update.filter(Flow.id.in_(to_list(flow_id)))

    flows_to_update = flows_to_update.subquery()
    ordered_positions = session.query(flows_to_update.c.flow_id, Position) \
        .join(Position, Position.ship_imo == flows_to_update.c.ship_imo) \
        .filter(
              sa.and_(
                  Position.date_utc >= flows_to_update.c.departure_date,
                  Position.date_utc <= flows_to_update.c.arrival_date
              )) \
        .order_by(flows_to_update.c.flow_id, Position.date_utc) \
        .subquery()

    trajectories = session.query(ordered_positions.c.flow_id.label("flow_id"),
                                 ST_MakeLine(ordered_positions.c.geometry).label("geometry")) \
                    .group_by(ordered_positions.c.flow_id)


    trajectories_df = pd.read_sql(trajectories.statement, session.bind)
    trajectories_df = update_geometry_from_wkb(trajectories_df, to="wkt")
    upsert(df=trajectories_df,
           table=DB_TABLE_TRAJECTORY,
           constraint_name="trajectory_flow_id_key",
           dtype=({'geometry': Geometry('LINESTRING', 4326)}))
