import datetime as dt

import base
from base.db import session
from base.models import Ship, Departure, Flow, Position, Arrival
from engine.arrival import get_dangling_arrivals
from engine import position
from tqdm import tqdm
import sqlalchemy
from sqlalchemy import func
from base.db import engine


def update():
    print("=== Flow update ===")
    with engine.connect() as con:
        with open('engine/detect_flows.sql', 'r') as file:
            sql_content = file.read()
        con.execute(sql_content)

    # # Collect arrivals that aren't translated into a flow yet
    # dangling_arrivals = get_dangling_arrivals()
    #
    # for d in tqdm(dangling_arrivals):
    #     # imo = session.query(Departure.ship_imo).filter(Departure.id == d.departure_id)
    #     # ship = Ship.query.filter(Ship.imo.in_(imo)).first()
    #     # departure = Departure.query.filter(Departure.id == d.departure_id).first()
    #
    #     data = {
    #         "departure_id": d.departure_id,
    #         "arrival_id": d.id
    #     }
    #
    #     flow = Flow(**data)
    #     session.add(flow)
    # session.commit()


def update_positions():

    print("=== Position update ===")
    # position_subq = session.query(
    #     Position.flow_id,
    #     func.max(Position.date_utc).label('last_date'),
    #     func.min(Position.date_utc).label('first_date')
    # ).group_by(Position.flow_id).subquery('last_position')

    # We update position which are still ongoing (no arrival yet)
    # or who are still missing some positions (should have til Arrival + n hours, and from Departure - n_hours)
    flows_to_update = session.query(Flow,
                                    Departure.ship_imo,
                                    Departure.date_utc.label('departure_date'),
                                    Arrival.date_utc.label('arrival_date'),
                                    func.min(Position.date_utc).label('first_date'),
                                    func.max(Position.date_utc).label('last_date')
                                    ) \
        .join(Departure, Flow.departure_id == Departure.id) \
        .join(Arrival, Flow.arrival_id == Arrival.id) \
        .join(Position, Position.ship_imo == Departure.ship_imo, isouter=True) \
        .filter(sqlalchemy.or_(
                    Position.date_utc == sqlalchemy.null(),
                    Position.date_utc >= Departure.date_utc - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE))) \
        .filter(sqlalchemy.or_(
                    Position.date_utc == sqlalchemy.null(),
                    Position.date_utc <= Arrival.date_utc + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL))) \
        .group_by(Flow.id, Departure.ship_imo, Departure.date_utc, Arrival.date_utc) \
        .having(sqlalchemy.or_(Arrival.date_utc == sqlalchemy.null(),
                               sqlalchemy.or_(
                                   func.min(Position.date_utc) == sqlalchemy.null(),
                                   func.max(Position.date_utc) < Arrival.date_utc + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL),
                                   func.min(Position.date_utc) > Departure.date_utc - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE)
                                   )
                               )
                )


    # if commodities is not None:
    #     flows_to_update = flows_to_update.filter(Ship.commodity.in_(commodities))

    flows_to_update = flows_to_update.all()
    # Add positions
    for f in tqdm(flows_to_update):
        flow = f[0]
        ship_imo = f[1]
        departure_date = f[2]
        arrival_date = f[3]
        first_date = f[4]
        last_date = f[5]
        # Add a bit of buffer hours, so that next time, we don't update the flows
        buffer_hours = 12
        date_from = departure_date - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE + buffer_hours)
        date_to = arrival_date + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL + buffer_hours)

        dates = []
        if first_date is None:
            # No position found, we query the whole voyage
            dates.append({"date_from": date_from, "date_to": date_to})
        else:
            # We only query head or tail or both
            if first_date > date_from:
                dates.append({"date_from": date_from, "date_to": first_date})
            if last_date < date_to:
                dates.append({"date_from": last_date, "date_to": date_to})

        for date in dates:
            positions = position.get(imo=ship_imo, **date)
            if positions:
                for p in positions:
                    p.flow_id = flow.id
                    session.add(p)
        session.commit()
