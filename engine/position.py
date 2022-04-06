from engine.datalastic import Datalastic
from base.db import session
import datetime as dt
import base
from base.utils import to_list
from base.models import Ship, Departure, Flow, Position, Arrival, Port
import sqlalchemy
from sqlalchemy import func, or_
from tqdm import tqdm


def get(imo, date_from, date_to):
    positions = Datalastic.get_positions(imo=imo, date_from=date_from, date_to=date_to)
    return positions


def update_destination_ports():
        update = Position.__table__.update().values(destination_port_id=Port.__table__.c.id) \
                .where(Position.__table__.c.destination_name != sqlalchemy.null(),
                       or_(
                        func.lower(Position.__table__.c.destination_name) == func.lower(Port.__table__.c.name),
                        Position.__table__.c.destination_name == Port.__table__.c.unlocode,
                        func.replace(Position.__table__.c.destination_name, " ", "") == Port.__table__.c.unlocode,
                        func.replace(func.regexp_replace(Position.__table__.c.destination_name, '(.*)(>){1,}',''), " ", "") == Port.__table__.c.unlocode
                )
        )

        from base.db import engine
        with engine.connect() as con:
            con.execute(update)


def update(commodities=None, imo=None, flow_id=None):

    print("=== Position update ===")
    buffer = dt.timedelta(hours=24)
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
                                    Ship.commodity,
                                    func.min(Position.date_utc).label('first_date'),
                                    func.max(Position.date_utc).label('last_date')
                                    ) \
        .join(Departure, Flow.departure_id == Departure.id) \
        .outerjoin(Arrival, Flow.arrival_id == Arrival.id) \
        .join(Ship, Ship.imo == Departure.ship_imo) \
        .outerjoin(Position, Position.ship_imo == Departure.ship_imo) \
        .filter(sqlalchemy.or_(
                    Position.date_utc == sqlalchemy.null(),
                    Position.date_utc >= Departure.date_utc - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE) - buffer)) \
        .filter(sqlalchemy.or_(
                    Position.date_utc == sqlalchemy.null(),
                    Arrival.date_utc == sqlalchemy.null(),
                    Position.date_utc <= Arrival.date_utc + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL) + buffer)) \
        .group_by(Flow.id, Departure.ship_imo, Departure.date_utc, Arrival.date_utc, Ship.commodity) \
        .having(sqlalchemy.or_(Arrival.date_utc == sqlalchemy.null(),
                               sqlalchemy.or_(
                                   func.min(Position.date_utc) == sqlalchemy.null(),
                                   func.max(Position.date_utc) < Arrival.date_utc + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL),
                                   func.min(Position.date_utc) > Departure.date_utc - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE)
                                   )
                               )
                )

    if flow_id is not None:
        flows_to_update = flows_to_update.filter(Flow.id.in_(to_list(flow_id)))

    if imo is not None:
        flows_to_update = flows_to_update.filter(Ship.imo.in_(to_list(imo)))

    if commodities is not None:
        flows_to_update = flows_to_update.filter(Ship.commodity.in_(to_list(commodities)))

    flows_to_update = flows_to_update.order_by(Departure.date_utc.desc()).all()
    # Add positions
    for f in tqdm(flows_to_update):
        flow = f[0]
        ship_imo = f[1]
        departure_date = f[2]
        arrival_date = f[3] if f[3] is not None else dt.datetime.utcnow()
        first_date = f[5]
        last_date = f[6]
        # Add a bit of buffer hours, so that next time, we don't update the flows
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
                print("Uploading %d positions" % (len(positions),))
                for p in positions:
                    p.flow_id = flow.id
                    session.add(p)
            session.commit()

    update_destination_ports()



