from engine.datalastic import Datalastic
from base.db import session
import datetime as dt
import base
from base.utils import to_list
from base.models import Ship, Departure, Shipment, Position, Arrival, Port, Destination
import sqlalchemy as sa
from sqlalchemy import func, or_
from tqdm import tqdm
from difflib import SequenceMatcher
import numpy as np
import pandas as pd


from base.db_utils import execute_statement, upsert
from base.models import DB_TABLE_POSITION



def get(imo, date_from, date_to):
    positions = Datalastic.get_positions(imo=imo, date_from=date_from, date_to=date_to)
    return positions


def update_shipment_last_position():

    # add last_position to shipment table for faster retrieval
    shipments_w_last_position = session.query(Shipment.id,
                                          Position.id.label('position_id'),
                                          Position.destination_name,
                                          Position.destination_port_id
                                          ) \
        .join(Departure, Departure.id == Shipment.departure_id) \
        .outerjoin(Arrival, Arrival.id == Shipment.arrival_id) \
        .join(Position, Position.ship_imo == Departure.ship_imo) \
        .filter(
            sa.and_(
                Position.date_utc >= Departure.date_utc,
                sa.or_(Arrival.date_utc == sa.null(),
                       Position.date_utc < Arrival.date_utc),
                Shipment.status != base.UNDETECTED_ARRIVAL,
            )) \
        .distinct(Shipment.id) \
        .order_by(Shipment.id, Position.date_utc.desc()) \
        .subquery()

    update = Shipment.__table__.update().values(last_position_id=shipments_w_last_position.c.position_id) \
        .where(Shipment.__table__.c.id == shipments_w_last_position.c.id)
    execute_statement(update)


def update(commodities=None, imo=None, shipment_id=None,
           force_for_those_without_destination=False):

    print("=== Position update ===")
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


    shipments_to_update = session.query(Shipment,
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
                                func.max(shipments_positions.c.position_date) < dt.datetime.utcnow()-dt.timedelta(hours=12)), # To prevent too much refreshing
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

    shipments_to_update = shipments_to_update.order_by(Departure.date_utc.desc()).all()
    # Add positions
    for f in tqdm(shipments_to_update):
        shipment = f[0]
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



