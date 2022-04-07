from engine.datalastic import Datalastic
from base.db import session
import datetime as dt
import base
from base.utils import to_list
from base.models import Ship, Departure, Flow, Position, Arrival, Port, Destination
import sqlalchemy as sa
from sqlalchemy import func, or_
from tqdm import tqdm
from difflib import SequenceMatcher
import numpy as np


def get(imo, date_from, date_to):
    positions = Datalastic.get_positions(imo=imo, date_from=date_from, date_to=date_to)
    return positions


def update_destinations():


        # Insert missing ones
        new_destinations = session.query(Flow.last_destination_name) \
            .filter(Flow.last_destination_name.notin_(session.query(Destination.name)))

        for new_destination in new_destinations:
            session.add(Destination(name=new_destination.name))
        session.commit()

        # Update based on name or unlocode matching
        update = Destination.__table__.update().values(port_id=Port.__table__.c.id,
                                                       iso2=Port.__table__.c.iso2) \
                .where(Destination.__table__.c.name != sa.null(),
                       or_(
                        func.lower(Destination.__table__.c.name) == func.lower(Port.__table__.c.name),
                        Destination.__table__.c.name == Port.__table__.c.unlocode,
                        func.replace(Destination.__table__.c.name, " ", "") == Port.__table__.c.unlocode,
                        func.replace(func.regexp_replace(Destination.__table__.c.name, '(.*)(>){1,}', ''), " ", "") == Port.__table__.c.unlocode,
                        func.replace(func.regexp_replace(Destination.__table__.c.name, ' VIA(.*)', ''), " ", "") == Port.__table__.c.unlocode
                )
        )

        from base.db import engine
        with engine.connect() as con:
            con.execute(update)

        # "For orders"
        update = Destination.__table__.update().values(type="for_order") \
            .where(or_(
                Destination.__table__.c.name.ilike('%FOR ORDER%'),
                Destination.__table__.c.name.ilike('%FOR...ORDER%'),
                Destination.__table__.c.name.ilike('%FOR_ORDER%'),
        ))

        from base.db import engine
        with engine.connect() as con:
            con.execute(update)

        # Using datalastic
        still_missings = Destination.query.filter(sa.and_(
                                                  Destination.iso2 == sa.null())).all()
        for still_missing in tqdm(still_missings):
            found = Datalastic.get_port_infos(name=still_missing.name, fuzzy=False)
            if found:
                ratios = np.array([SequenceMatcher(None, x.name, still_missing.name).ratio() for x in found])
                if max(ratios) > 0.8:
                    print("Best match: %s == %s (%f)" % (still_missing.name, found[ratios.argmax()].name, ratios.max()))
                    found_and_filtered = found[ratios.argmax()]
                    if found_and_filtered:
                        still_missing.iso2 = found_and_filtered.iso2
                        session.commit()
                    else:
                        print("wasn't close enough")

        # Looking for country names in destination.name
        country_regexps = {
            'RU': ['[ |,|_|\.]{1}RU[S]?[SIA]?$','^RU [\s|\w]*$'],
            'TR': ['[ |,|_]{1}TURKEY$','^TR [\s|\w]*$', '[ |,|_]{1}ISTANBUL', '[ |,|_]{1}TR$', '^TOROS$'],
            'DK': ['[ |,|_]{1}DENMARK$','[ |,|_|>]{1}DK'],
            'BR': ['[ |,|_]{1}BRAZIL$'],
            'SE': ['[ |,|_]{1}SWEDEN$'],
            'IN': ['[ |,|_]{1}INDIA$'],
            'IT': ['[ |,|_]{1}ITALY$'],
            'GR': ['[ |,|_]{1}GRE[E]?CE$'],
            'EG': ['[ |,|_]{1}EGYPT$'],
            'FR': ['[ |,|_]{1}FRANCE'],
            'EE': ['[ |,|_]{1}ESTONIA','TALLIN[\s|\w]*'],
            'SG': ['[\s|\w]*SINGAPORE[\s|\w]*'],
            'GB': ['[ |,|_]{1}UK$'],
            'RO': ['[ |,|_]{1}ROMANIA', 'CONSTANTA[\s|\w]*'],
            'ZA': ['[ |,|_]{1}ZA'],
            'NL': ['^NL [\s|\w]*$'],
            'KR': ['[ |,|_]{1}S[\.]?KOREA$','^KR [\s|\w]*$'],
            'JP': ['^JP [\s|\w]*$','[ |,|_]{1}JP$'],
            'CN': ['[ |,|_]{1}CHINA$','^CN [\s|\w]*$','^HUANG DAO$','^CAOFEIDIAN$','^LANYUNGANG$'],
            'MY': ['[ |,|_|/]{1}MALAYSIA$'],
            'TW': ['^TW[\s|\w]*','[ |,|_]{1}TW$'],
            'OM': ['[ |,|_|-]{1}OMAN'],
            'ES': ['[ |,|_|-]{1}SPAIN$'],
            'LY': ['[ |,|_|-]{1}LYBIA$', '^LYBIA$'],
            'IR': ['ANZALI']
        }

        for key, regexps in country_regexps.items():
            condition = or_(*[Destination.name.op('~')(regexp) for regexp in regexps])
            update = Destination.__table__.update().values(iso2=key) \
                .where(condition)
            with engine.connect() as con:
                con.execute(update)


def update_flow_last_position():

    # add last_position to flow table for faster retrieval
    flows_w_last_position = session.query(Flow.id,
                                          Position.id.label('position_id'),
                                          Position.destination_name,
                                          Position.destination_port_id
                                          ) \
        .join(Departure, Departure.id == Flow.departure_id) \
        .outerjoin(Arrival, Arrival.id == Flow.arrival_id) \
        .join(Position, Position.ship_imo == Departure.ship_imo) \
        .filter(
            sa.and_(
                Position.date_utc >= Departure.date_utc,
                sa.or_(Arrival.date_utc == sa.null(),
                       Position.date_utc <= Arrival.date_utc),
                Flow.status != base.UNDETECTED_ARRIVAL
            )) \
        .distinct(Flow.id) \
        .order_by(Flow.id, Position.date_utc.desc()) \
        .subquery()

    update = Flow.__table__.update().values(last_position_id=flows_w_last_position.c.position_id) \
        .where(Flow.__table__.c.id == flows_w_last_position.c.id)
    from base.db import engine
    with engine.connect() as con:
        con.execute(update)


def update_flow_last_destination():
    # add last_destination_name to flow table for faster retrieval
    flows_w_last_position = session.query(Flow.id,
                                          Position.id.label('position_id'),
                                          Position.destination_name,
                                          Position.destination_port_id
                                          ) \
        .join(Departure, Departure.id == Flow.departure_id) \
        .outerjoin(Arrival, Arrival.id == Flow.arrival_id) \
        .join(Position, Position.ship_imo == Departure.ship_imo) \
        .filter(
        sa.and_(
            Position.date_utc >= Departure.date_utc,
            Position.destination_name != sa.null(),
            sa.or_(Arrival.date_utc == sa.null(),
                   Position.date_utc <= Arrival.date_utc)
        )) \
        .distinct(Flow.id) \
        .order_by(Flow.id, Position.date_utc.desc()) \
        .subquery()

    update = Flow.__table__.update().values(last_destination_name=flows_w_last_position.c.destination_name) \
        .where(Flow.__table__.c.id == flows_w_last_position.c.id)
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
        .filter(sa.or_(
                    Position.date_utc == sa.null(),
                    Position.date_utc >= Departure.date_utc - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE) - buffer)) \
        .filter(sa.or_(
                    Position.date_utc == sa.null(),
                    Arrival.date_utc == sa.null(),
                    Position.date_utc <= Arrival.date_utc + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL) + buffer)) \
        .group_by(Flow.id, Departure.ship_imo, Departure.date_utc, Arrival.date_utc, Ship.commodity) \
        .having(sa.or_(Arrival.date_utc == sa.null(),
                               sa.or_(
                                   func.min(Position.date_utc) == sa.null(),
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

    # Includes the rest of the pipeline
    update_flow_last_position()
    update_flow_last_destination()
    update_destinations()



