from engine.datalastic import Datalastic
from engine.marinetraffic import Marinetraffic
from base.db import session, engine
import datetime as dt
import base
from base.utils import to_list
from base.models import Ship, Departure, Flow, Position, Arrival, Port, Destination, MTVoyageInfo
import sqlalchemy as sa
from sqlalchemy import func, or_
from tqdm import tqdm
from difflib import SequenceMatcher
import numpy as np
from base.db_utils import execute_statement

def update():
    print("=== Destination update ===")
    update_from_positions()
    update_from_voyageinfo()
    update_matching()


def update_matching():
        # Insert missing ones
        new_destinations = session.query(Flow.last_destination_name) \
            .filter(Flow.last_destination_name.notin_(session.query(Destination.name))) \
            .distinct() \
            .all()

        for new_destination in new_destinations:
            session.add(Destination(name=new_destination.last_destination_name))
        session.commit()

        # Update based on name or unlocode matching
        update = Destination.__table__.update().values(port_id=Port.__table__.c.id,
                                                       iso2=Port.__table__.c.iso2) \
                .where(Destination.__table__.c.name != sa.null(),
                       or_(
                        func.lower(Destination.__table__.c.name) == func.lower(Port.__table__.c.name),
                        Destination.__table__.c.name == Port.__table__.c.unlocode,
                        func.replace(Destination.__table__.c.name, " ", "") == Port.__table__.c.unlocode,
                        func.replace(func.regexp_replace(Destination.__table__.c.name, '(.*)(>|/|_){1,}', ''), " ", "") == Port.__table__.c.unlocode,
                        func.replace(func.regexp_replace(Destination.__table__.c.name, ' VIA(.*)', ''), " ", "") == Port.__table__.c.unlocode
                )
        )

        execute_statement(update)


        # Using datalastic to query port information
        # for ongoing flows. Note that we only query those destinations for which we have no country
        # and not those we have no port information. If and when we'll need port level info,
        # it'll be useful to replace Destination.iso2 == NULL with Destination.port_id == NULL
        still_missings = Destination.query \
            .join(Flow, Flow.last_destination_name == Destination.name) \
            .filter(sa.and_(Flow.status == base.ONGOING,
                            Destination.iso2 == sa.null())).all()

        for still_missing in tqdm(still_missings):
            looking_name = still_missing.name.replace(" OPL","")
            found = Datalastic.get_port_infos(name=looking_name, fuzzy=False)
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
            'RU': ['[ |,|_|\.]{1}RU[S]?[SIA]?$','^RU [\s|\w]*$', '^ROSTOV NO DON$', '^ROSTOU$', '[ |,|_|\.]{1}RU[\w]{3}$'],
            'TR': ['[ |,|_]{1}TURKEY$','^TR [\s|\w]*$', '[ |,|_]{1}ISTANBUL', '[ |,|_]{1}TR$', '^TOROS$', 'CANAKALE$'],
            'DK': ['[ |,|_]{1}DENMARK$','[ |,|_|>]{1}DK$'],
            'BR': ['[ |,|_]{1}BRAZIL$'],
            'SE': ['[ |,|_]{1}SWEDEN$'],
            'IN': ['[ |,|_]{1}INDIA$'],
            'IT': ['[ |,|_]{1}ITALY$'],
            'GR': ['[ |,|_]{1}GRE[E]?CE$'],
            'EG': ['[ |,|_]{1}EGYPT$'],
            'FR': ['[ |,|_]{1}FRANCE'],
            'EE': ['[ |,|_]{1}ESTONIA','TALLIN[\s|\w]*', '^TALLNN$', '^EETIL OPL$'],
            'SG': ['[\s|\w]*SINGAPORE[\s|\w]*'],
            'GB': ['[ |,|_]{1}UK$'],
            'RO': ['[ |,|_]{1}ROMANIA', 'CONSTANTA[\s|\w]*'],
            'ZA': ['[ |,|_]{1}ZA'],
            'NL': ['^NL [\s|\w]*$', '[ |,|_|\.]{1}NL[\s]?[\w]{3}$'],
            'KR': ['[ |,|_]{1}S[\.]?KOREA$','^KR [\s|\w]*$'],
            'JP': ['^JP [\s|\w]*$','[ |,|_]{1}JP$'],
            'CN': ['[ |,|_]{1}CHINA$','^CN [\s|\w]*$','^HUANG DAO$','^CAOFEIDIAN$','^LANYUNGANG$','^CHINA$'],
            'MY': ['[ |,|_|/]{1}MALAYSIA$'],
            'TW': ['^TW[\s|\w]*','[ |,|_]{1}TW$'],
            'OM': ['[ |,|_|-]{1}OMAN'],
            'ES': ['[ |,|_|-]{1}SPAIN$'],
            'LY': ['[ |,|_|-]{1}LYBIA$', '^LYBIA$'],
            'MT': ['[ |,|_|-]{1}MALTA$'],
            'IR': ['ANZALI'],
            'YE': ['^YE [\s|\w]*$'],
            'AE': ['[ |,|_]{1}UAE$'],
            'US': ["^USA$"]
        }

        for key, regexps in country_regexps.items():
            condition = or_(*[Destination.name.op('~')(regexp) for regexp in regexps])
            update = Destination.__table__.update().values(iso2=key) \
                .where(condition)
            execute_statement(update)


        # "For orders" should be unknown
        update = Destination.__table__.update().values(type="for_order", iso2=None) \
            .where(or_(
            Destination.__table__.c.name.ilike('%FOR ORDER%'),
            Destination.__table__.c.name.ilike('%FOR...ORDER%'),
            Destination.__table__.c.name.ilike('%FOR_ORDER%'),
        ))
        execute_statement(update)


def update_from_positions():

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
    execute_statement(update)


    # For ongoing flows still missing a destination
    # use MarineTraffic Voyage
    # But how to prevent requerying it if we rebuild all flows?



def update_from_voyageinfo(commodities = [base.LNG,
                                          base.CRUDE_OIL,
                                          base.OIL_PRODUCTS,
                                          base.OIL_OR_CHEMICAL]):
    """
    For flows for which we have no information on destination,
    we use MT even though this is a bit pricey.
    We reserve it to ongoing flows, with no destination info in positions,
    and leaving from ports of interest only.
    :return:
    """

    missing_flows = session.query(Flow.id, Departure.date_utc, Departure.ship_imo) \
        .join(Departure, Flow.departure_id == Departure.id) \
        .join(Ship, Departure.ship_imo == Ship.imo) \
        .filter(sa.and_(
            # Flow.last_position_id != sa.null(),
            Flow.last_destination_name == sa.null(),
            Flow.status == base.ONGOING,
        # Cannot be older than a month. No date parameter
            Departure.date_utc >= dt.datetime.utcnow() - dt.timedelta(days=31)
        ))

    if commodities is not None:
        missing_flows = missing_flows.filter(Ship.commodity.in_(commodities))

    for missing_flow in tqdm(missing_flows.all()):
        Marinetraffic.get_voyage_info(imo=missing_flow.ship_imo,
                                      date_from=missing_flow.date_utc)


    # Then update flow table
    new_destinations = session.query(Flow.id.label('flow_id'),
                                     Departure.ship_imo,
                                     Departure.date_utc,
                                     MTVoyageInfo.destination_name) \
        .join(Departure, Departure.id == Flow.departure_id) \
        .join(MTVoyageInfo, Departure.ship_imo == MTVoyageInfo.ship_imo) \
        .filter(
            sa.and_(
                MTVoyageInfo.queried_date_utc >= Departure.date_utc,
                Flow.status == base.ONGOING,
                Flow.last_destination_name == sa.null()
            )) \
        .subquery()

    update = Flow.__table__.update().values(last_destination_name=new_destinations.c.destination_name) \
        .where(Flow.__table__.c.id == new_destinations.c.flow_id)
    execute_statement(update)

    return
