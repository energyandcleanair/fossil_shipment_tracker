import datetime as dt
from sqlalchemy import func
from sqlalchemy import or_
import sqlalchemy as sa
from geoalchemy2.functions import ST_MakeLine

from sqlalchemy.orm import aliased

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


def update(shipment_id=None, rebuild_all=False):
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
    ordered_positions = session.query(shipments_to_update.c.shipment_id, Position) \
        .join(Position, Position.ship_imo == shipments_to_update.c.ship_imo) \
        .filter(
              sa.and_(
                  Position.date_utc >= shipments_to_update.c.departure_date,
                  Position.date_utc <= shipments_to_update.c.arrival_date
              )) \
        .order_by(shipments_to_update.c.shipment_id, Position.date_utc) \
        .subquery()

    trajectories = session.query(ordered_positions.c.shipment_id.label("shipment_id"),
                                 ST_MakeLine(ordered_positions.c.geometry).label("geometry")) \
                    .group_by(ordered_positions.c.shipment_id)


    trajectories_df = pd.read_sql(trajectories.statement, session.bind)
    trajectories_df = update_geometry_from_wkb(trajectories_df, to="wkt")
    upsert(df=trajectories_df,
           table=DB_TABLE_TRAJECTORY,
           constraint_name="trajectory_shipment_id_key",
           dtype=({'geometry': Geometry('LINESTRING', 4326)}))
