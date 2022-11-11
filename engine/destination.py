from engine.datalastic import Datalastic
from engine.marinetraffic import Marinetraffic
from base.db import session, engine
import datetime as dt
import base
from base.logger import logger_slack
from base.models import Ship, Departure, Shipment, Position, Arrival, Port, Destination, MTVoyageInfo, ShipmentWithSTS
import sqlalchemy as sa
from sqlalchemy import ARRAY, String
from sqlalchemy import func, or_
from tqdm import tqdm
from difflib import SequenceMatcher
import numpy as np
from base.db_utils import execute_statement
from engine.shipment import return_combined_shipments


def update():
    logger_slack.info("=== Destination update ===")
    update_from_positions()
    # update_from_voyageinfo()
    update_matching()
    # Update once more to have destination_iso2s updated
    update_from_positions()


def update_matching():

        shipments_all = return_combined_shipments(session)

        # Insert missing ones
        dest1 = session.query(shipments_all.c.shipment_last_destination_name.label('destination_name'))
        dest2 = session.query(func.unnest(shipments_all.c.shipment_destination_names).label('destination_name'))
        destinations = dest1.union(dest2).subquery()

        new_destinations = session.query(destinations) \
            .filter(destinations.c.destination_name.notin_(session.query(Destination.name))) \
            .distinct() \
            .all()

        for new_destination in new_destinations:
            session.add(Destination(name=new_destination.destination_name))
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
        # for ongoing shipments. Note that we only query those destinations for which we have no country
        # and not those we have no port information. If and when we'll need port level info,
        # it'll be useful to replace Destination.iso2 == NULL with Destination.port_id == NULL
        still_missings = Destination.query \
            .join(Shipment, Shipment.last_destination_name == Destination.name) \
            .filter(sa.and_(Shipment.status == base.ONGOING,
                            Destination.iso2 == sa.null())).all()

        still_missings = Destination.query \
            .filter(Destination.iso2 == sa.null()).all()

        for still_missing in tqdm(still_missings):
            looking_name = still_missing.name.replace(" OPL", "")
            found = Datalastic.search_ports(name=looking_name, fuzzy=False)
            if found:
                potential_suffixes = ['', ' PORT']
                ratios = np.array([max([SequenceMatcher(None, x.name.replace(suf, ''), still_missing.name).ratio()
                                        for suf in potential_suffixes])
                                   for x in found])

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
            'RU': ['[ |,|_|\.]{1}RU[S]?[SIA]?$', '^RU [\s|\w]*$', '^ROSTOV NO DON$', '^ROSTOU$', '^RU[\w]{3}$'
                   '^BUKHTA ', '[ |,|_|\.]{1}RU[\w]{3}$', 'YEISK$', 'RUKOR$', 'AZOU$', '^PKC$',
                   'RUSSIA|RUSNVS$|RU_PGN$|TAUPSE|KAV?KAZ$', '^RUS TOCHINO$', '^VLDV$', 'RUAZOV$', 'RUVFP$'],
            'TR': ['[ |,|_]{1}TURKEY$','^TR [\s|\w]*$', '[ |,|_]{1}ISTANBUL', '[ |,|_]{1}TR$',
                   '^TOROS$', 'CANAKALE$', 'IZMIT$', 'TR[/]?ZON$', 'SAMSUN/TR$', 'ST[A]?NBUL$', 'TR[ ]?IST$'],
            'DK': ['[ |,|_]{1}DENMARK$','[ |,|_|>]{1}DK$', ' SKAW$|SKGEN$|^SKAW$'],
            'BR': ['[ |,|_]{1}BRAZIL$', '^BR ?PRM|BRPEE'],
            'SE': ['[ |,|_]{1}SWEDEN$'],
            'IN': ['[ |,|_]{1}INDIA$','^INDIA$','SIKKA$','HAZIRA SPM$', '[ |,|>]{1}IN[ |,|_]{1}[\w]{3}$'],
            'IT': ['RAVENNA$'],
            'GR': ['[ |,|_]{1}GRE[E]?CE$','^VATIKA$','^KALAMATA$', 'LACONIA BAY$', 'LIMNOS GR'],
            'EG': ['[ |,|_]{1}EGYPT$', 'PORT SAID$'],
            'FR': ['[ |,|_]{1}FRANCE','FRFOS$','FRLEH$', 'DUNKERQUE FR$', 'DUNKERQUE$'],
            'EE': ['[ |,|_]{1}ESTONIA','TALLIN[\s|\w]*', '^TALLNN$', '^EETIL OPL$', 'EE MUU$'],
            'SG': ['[\s|\w]*SINGAPORE[\s|\w]*', 'PEBG[B|C]?$'],
            'GB': ['[ |,|_]{1}UK$', '[ |,|_]{1}GB$', 'THAMESHAVEN$'],
            'RO': ['[ |,|_]{1}ROMANIA', 'CONSTANTA[\s|\w]*', '^R0 GAL$'],
            'ZA': ['[ |,|_]{1}ZA'],
            'NL': ['^NL [\s|\w]*$', '[ |,|_|\.]{1}NL[\s]?[\w]{3}$', 'BORS+ELE','NETHERLAND$'],
            'KR': ['[ |,|_]{1}S[\.]?KOREA$','^KR [\s|\w]*$',
                   '( |,)KOREA|S\\.KOREA| KR$|KOR |KR_USN',
                   '[ |,|_|/]{1}KR$', 'YEOSU BERTH$', '^KRICH$'],
            'JP': ['^JP [\s|\w]*$','[ |,|_]{1}JP$'],
            'CN': ['[ |,|_]{1}CHINA$','^C[H|N][_]?[\w]{3}$', '^CN [\s|\w]*$','^HUANG DAO$',
                   '^CAOFEIDIAN$','^LANYUNGANG$','^CHINA$', ' CN$|LAN QIAO$', 'CH LNS$', 'CH TAG$', 'C J K',
                   'PENGLAI-193$', 'CH FAN$', 'CH[ ]?[I]?NA$', 'QING DAO/CN$'],
            'MY': ['[ |,|_|/]{1}MALAYSIA$', 'PELEPAS$'],
            'TW': ['^TW[\s|\w]*','[ |,|_]{1}TW[N]?$'],
            'OM': ['[ |,|_|-]{1}OMAN'],
            'ES': ['[ |,|_|-]{1}SPAIN$', '^SP [\s|\w]*$'],
            'LY': ['[ |,|_|-|/]{1}LYBIA$', '^LYBIA$'],
            'MT': ['[ |,|_|-]{1}MALTA$', '^MALTA OPL$'],
            'IR': ['ANZALI', 'BIK '],
            'YE': ['^YE [\s|\w]*$'],
            'AE': ['[ |,|_]{1}UAE$'],
            'US': ["^USA$|^US ",',USA$', 'GARYVILLE$'],
            'DE': [',GERMA'],
            'BE': [' BE$','BELGIUM$'],
            'NO': ['^NOSGE$'],
            'FI': ['^KOTKA$', 'KOTKA FIN$'],
            'BG': [' BG$'],
            'SK': ['^SK [\s|\w]*$'],
            'PT': ['FIGUEIRO DE FOS$', '^PT SINES$'],
            'VN' : ['VIET[ ]?NAM$'],
            'PL' : ['GSANSK$']
        }

        for key, regexps in country_regexps.items():
            condition = or_(*[Destination.name.op('~')(regexp) for regexp in regexps])
            update = Destination.__table__.update().values(iso2=key) \
                .where(condition)
            execute_statement(update)

        # All those that end with a country name
        from base.models import Country
        country_regex = session.query(Country.iso2,
                                      ('[\.| |,|_|-|/]{1}' + Country.name + '$').label('regexp')).subquery()
        update = Destination.__table__.update().values(iso2=country_regex.c.iso2) \
            .where(sa.and_(
            Destination.iso2 == sa.null(),
            Destination.name.op('~*')(country_regex.c.regexp)))
        print(update)
        execute_statement(update)



        # "For orders" should be set as such
        fororders_regexps = [' ORDER|MALTA FOR|GR ORD|^DUNKERQUE FO$']
        condition = or_(*[Destination.name.op('~')(regexp) for regexp in fororders_regexps])
        update = Destination.__table__.update().values(iso2=base.FOR_ORDERS) \
            .where(condition)
        execute_statement(update)

        # When transit ports only, should be set as null
        unknown_regexps = ['BOSP.?ORUS|DRIFTING AREA|GOGLAND|NW EUROPE|GREAT BELT|NW BLACK SEA|NW ?BS|I.?STANBUL|^TR ?IST$']
        condition = or_(*[Destination.name.op('~')(regexp) for regexp in unknown_regexps])
        update = Destination.__table__.update().values(iso2=sa.null()) \
            .where(condition)
        execute_statement(update)

      


def update_from_positions():

    shipments_all = return_combined_shipments(session)

    # add last_destination_name to shipment table for faster retrieval
    # we add in all position we have stored for the shipment and then take the latest date position with destination
    shipments_w_last_position = session.query(shipments_all.c.shipment_id,
                                          Position.id.label('position_id'),
                                          Position.destination_name,
                                          Position.destination_port_id
                                          ) \
        .join(Departure, Departure.id == shipments_all.c.shipment_departure_id) \
        .outerjoin(Arrival, Arrival.id == shipments_all.c.shipment_arrival_id) \
        .join(Position, Position.ship_imo == Departure.ship_imo) \
        .filter(
        sa.and_(
            shipments_all.c.shipment_last_destination_name == sa.null(),
            Position.date_utc >= Departure.date_utc,
            Position.destination_name != sa.null(),
            sa.or_(Arrival.date_utc == sa.null(),
                   Position.date_utc <= Arrival.date_utc)
        )) \
        .distinct(shipments_all.c.shipment_id) \
        .order_by(shipments_all.c.shipment_id, Position.date_utc.desc()) \
        .subquery()

    update = Shipment.__table__.update().values(last_destination_name=shipments_w_last_position.c.destination_name) \
        .where(Shipment.__table__.c.id == shipments_w_last_position.c.shipment_id)

    update_sts = ShipmentWithSTS.__table__.update().values(last_destination_name=shipments_w_last_position.c.destination_name) \
        .where(ShipmentWithSTS.__table__.c.id == shipments_w_last_position.c.shipment_id)

    execute_statement(update)
    execute_statement(update_sts)


    # List all destinations, after removing consecutive identical ones
    s1 = session.query(shipments_all.c.shipment_id,
                       Position.date_utc,
                       Position.destination_name,
                       func.lag(Position.destination_name).over(
                           partition_by=shipments_all.c.shipment_id,
                           order_by=Position.date_utc)
                       .label('previous_destination_name')
                       ) \
        .join(Departure, Departure.id == shipments_all.c.shipment_departure_id) \
        .outerjoin(Arrival, Arrival.id == shipments_all.c.shipment_arrival_id) \
        .join(Position, Position.ship_imo == Departure.ship_imo) \
        .filter(
            sa.and_(
                # Shipment.last_destination_name == sa.null(),
                Position.date_utc >= Departure.date_utc,
                Position.destination_name != sa.null(),
                sa.or_(Arrival.date_utc == sa.null(),
                       Position.date_utc <= Arrival.date_utc))) \
        .order_by(shipments_all.c.shipment_id, Position.date_utc) \
        .subquery()

    s2 = session.query(s1,
                       Destination.iso2).filter(
        sa.or_(s1.c.previous_destination_name == sa.null(),
                s1.c.destination_name != s1.c.previous_destination_name)) \
        .outerjoin(Destination, Destination.name == s1.c.destination_name) \
        .subquery()

    shipments_destinations = session.query(s2.c.shipment_id,
                                           func.array_agg(s2.c.destination_name,
                                                          type_=ARRAY(String)).label('destination_names'),
                                           func.array_agg(s2.c.date_utc,
                                                          type_=ARRAY(String)).label('destination_dates'),
                                           func.array_agg(s2.c.iso2,
                                                          type_=ARRAY(String)).label('destination_iso2s'),
                                           ) \
        .group_by(s2.c.shipment_id).subquery()

    update = Shipment.__table__.update().values(destination_names=shipments_destinations.c.destination_names,
                                                destination_dates=shipments_destinations.c.destination_dates,
                                                destination_iso2s=shipments_destinations.c.destination_iso2s) \
        .where(Shipment.__table__.c.id == shipments_destinations.c.shipment_id)

    update_sts = ShipmentWithSTS.__table__.update().values(destination_names=shipments_destinations.c.destination_names,
                                                destination_dates=shipments_destinations.c.destination_dates,
                                                destination_iso2s=shipments_destinations.c.destination_iso2s) \
        .where(ShipmentWithSTS.__table__.c.id == shipments_destinations.c.shipment_id)

    execute_statement(update)
    execute_statement(update_sts)



    # For ongoing shipments still missing a destination
    # use MarineTraffic Voyage
    # But how to prevent requerying it if we rebuild all shipments?



def update_from_voyageinfo(commodities = [base.LNG,
                                          base.CRUDE_OIL,
                                          base.OIL_PRODUCTS,
                                          base.OIL_OR_CHEMICAL]):
    """
    For shipments for which we have no information on destination,
    we use MT even though this is a bit pricey.
    We reserve it to ongoing shipments, with no destination info in positions,
    and leaving from ports of interest only.
    :return:
    """

    missing_shipments = session.query(Shipment.id, Departure.date_utc, Departure.ship_imo) \
        .join(Departure, Shipment.departure_id == Departure.id) \
        .join(Ship, Departure.ship_imo == Ship.imo) \
        .filter(sa.and_(
            # Shipment.last_position_id != sa.null(),
            Shipment.last_destination_name == sa.null(),
            # Shipment.status == base.ONGOING,
        # Cannot be older than a month. No date parameter
        #     Departure.date_utc >= dt.datetime.utcnow() - dt.timedelta(days=31)
        ))

    if commodities is not None:
        missing_shipments = missing_shipments.filter(Ship.commodity.in_(commodities))

    for missing_shipment in tqdm(missing_shipments.all()):
        Marinetraffic.get_voyage_info(imo=missing_shipment.ship_imo,
                                      date_from=missing_shipment.date_utc)


    # Then update shipment table
    new_destinations = session.query(Shipment.id.label('shipment_id'),
                                     Departure.ship_imo,
                                     Departure.date_utc,
                                     MTVoyageInfo.destination_name) \
        .join(Departure, Departure.id == Shipment.departure_id) \
        .join(MTVoyageInfo, Departure.ship_imo == MTVoyageInfo.ship_imo) \
        .filter(
            sa.and_(
                MTVoyageInfo.queried_date_utc >= Departure.date_utc,
                Shipment.status == base.ONGOING,
                Shipment.last_destination_name == sa.null()
            )) \
        .subquery()

    update = Shipment.__table__.update().values(last_destination_name=new_destinations.c.destination_name) \
        .where(Shipment.__table__.c.id == new_destinations.c.shipment_id)
    execute_statement(update)

    return
