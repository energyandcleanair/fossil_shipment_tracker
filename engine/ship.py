from tqdm import tqdm
from sqlalchemy.exc import IntegrityError
import base

from base.db import session
from base.logger import logger
from base.models import Ship, PortCall, Departure, Shipment
from base.utils import to_datetime, to_list
from engine.datalastic import Datalastic
from engine.marinetraffic import Marinetraffic

import sqlalchemy as sa


def update():
    # Not much really. We just confirm crude_oil vs oil_products when necessary
    # And use MT for insurance
    collect_mt_for_large_oil_products()
    # collect_mt_for_insurers()
    return


def collect_mt_for_large_oil_products():
    """
    Datalastic indicates Oil Products Tanker for certain tankers
    that are marked as Crude Oil tankers by MT.
    We trust MT, and recollect MT for 'dubious' ships
    :return:
    """
    ships = Ship.query.filter(Ship.commodity == base.OIL_PRODUCTS,
                              Ship.dwt >= 40e3,
                              ).all()

    for ship in tqdm(ships):
        if ship.type != ship.others.get('marinetraffic', {}).get('VESSEL_TYPE'):
            ship_mt = Marinetraffic.get_ship(mmsi=ship.mmsi)
            if ship_mt is not None and ship.imo == ship_mt.imo:
                ship_mt.others.update(ship.others)
                (commodity, quantity, unit) = ship_to_commodity(ship_mt)
                ship_mt.commodity = commodity
                ship_mt.quantity = quantity
                ship_mt.unit = unit
                ship_mt.subtype = None
                session.merge(ship_mt)
                session.commit()
            else:
                logger.info("IMOs don't match or ship not found")
        else:
            logger.info("Was already using MT")

#
# def collect_mt_for_insurers(date_from='2022-02-24',
#                          commodity=[base.CRUDE_OIL, base.LNG]):
#
#     ships = session.query(Ship) \
#         .join(Departure, Ship.imo == Departure.ship_imo) \
#         .join(Shipment, Shipment.departure_id == Departure.id) \
#         .filter(Departure.date_utc >= date_from) \
#         .filter(Ship.insurer == sa.null()) \
#         .filter(Ship.commodity.in_(to_list(commodity))) \
#         .distinct() \
#         .all()
#
#     for ship in tqdm(ships):
#         if not 'INSURER' in ship.others.get('marinetraffic', {}).keys():
#             ship_mt = Marinetraffic.get_ship(mmsi=ship.mmsi, use_cache=True)
#             # ship_mt = Marinetraffic.get_ship(imo=ship.imo, use_cache=False)
#             if ship_mt is not None and ship.imo == ship_mt.imo:
#                 if 'datalastic' in ship.others:
#                     ship_mt.others['datalastic'] = ship.others['datalastic']
#                 session.merge(ship_mt)
#                 session.commit()
#             else:
#                 logger.info("IMOs don't match or ship not found")

def fill_missing_commodity():

    ships = Ship.query.filter(Ship.commodity == sa.null()).all()
    for ship in ships:
        (commodity, quantity, unit) = ship_to_commodity(ship)
        ship.commodity = commodity
        ship.quantity = quantity
        ship.unit = unit
        session.commit()



def fill(imos=[], mmsis=[]):
    """
    Fill database (append or upsert) ship information
    :param imos: list of imos codes
    :param source: what data source to use
    :return:
    """
    imos = [str(x) for x in imos]
    mmsis = [str(x) for x in mmsis]

    # Fill missing ships
    def get_missing_ships_imos(imos):
        existing_imos = [value for value, in session.query(Ship.imo).all()]
        return [x for x in imos if str(x) not in existing_imos]

    def get_missing_ships_mmsis(mmsis):
        existing_mmsis = [value for value, in session.query(Ship.mmsi).all()]
        return [x for x in mmsis if str(x) not in existing_mmsis]

    if not get_missing_ships_imos(imos) and not get_missing_ships_mmsis(mmsis):
        # Ship already in db
        return True

    logger.info("Adding %d missing ships"%(len(imos) + len(mmsis)))

    # First with Datalastic
    ships = [Datalastic.get_ship(imo=x, query_if_not_in_cache=False) for x in get_missing_ships_imos(imos)]
    upload_ships(ships)

    ships = [Datalastic.get_ship(mmsi=x, query_if_not_in_cache=False) for x in get_missing_ships_mmsis(mmsis)]
    upload_ships(ships)

    # Then with Marinetraffic for those still missing
    from engine.marinetraffic import Marinetraffic
    ships = [Marinetraffic.get_ship(imo=x) for x in get_missing_ships_imos(imos)]
    upload_ships(ships)

    ships = [Marinetraffic.get_ship(mmsi=x) for x in get_missing_ships_mmsis(mmsis)]
    upload_ships(ships)

    missing = get_missing_ships_imos(imos)
    missing.extend(get_missing_ships_mmsis(mmsis))
    if missing:
        logger.warning("Some ships are still missing: %s" % (",".join(missing)))
        return False

    return True


def upload_ships(ships):
    for ship in ships:
        if ship and ship.imo is not None:
            ship = set_commodity(ship)
            session.add(ship)
        try:
            session.commit()
        except sa.exc.IntegrityError as e:
            session.rollback()
            # Ship with this IMO probably already exists.
            n_imo_ships = Ship.query.filter(Ship.imo.op('~')(ship.imo)).count()
            if n_imo_ships > 0:
                ship.imo = "%s_v%d"%(ship.imo, n_imo_ships+1)
                session.add(ship)
                session.commit()
            else:
                raise ValueError("Problem inserting ship: %s"%(str(e),))


def ship_to_commodity(ship):
    """
    Guess commodity, and quantity of ship
    :param ship:
    :return: [commodity, quantity, unit]
    """
    import re
    try:
        type = ship.type or ""
        subtype = ship.subtype or ""
        if re.match('Crude oil', type, re.IGNORECASE) \
                or re.match('Crude oil', subtype, re.IGNORECASE):
            commodity = base.CRUDE_OIL
        elif re.match('OIL/CHEMICAL', type, re.IGNORECASE) \
                or re.match('Oil or chemical', subtype, re.IGNORECASE):
            commodity = base.OIL_OR_CHEMICAL
        elif re.match('OIL PRODUCTS', type, re.IGNORECASE) \
                or re.match('Oil products', subtype, re.IGNORECASE):
            commodity = base.OIL_PRODUCTS
        elif re.match('LNG', type, re.IGNORECASE) \
             or re.match('LNG', subtype, re.IGNORECASE):
            commodity = base.LNG
        elif re.match('LPG', type, re.IGNORECASE) \
             or re.match('LPG', subtype, re.IGNORECASE):
            commodity = base.LPG
        elif re.match('Ore or Oil', subtype, re.IGNORECASE):
            commodity = base.OIL_OR_ORE
        elif re.match('Bulk', type, re.IGNORECASE) \
             or re.match('Bulk', subtype, re.IGNORECASE):
            commodity = base.BULK
        elif re.match('cargo', type, re.IGNORECASE):
            commodity = base.GENERAL_CARGO
        else:
            commodity = base.UNKNOWN_COMMODITY

    except TypeError:
        commodity = base.UNKNOWN_COMMODITY

    if ship.liquid_gas is not None and commodity==base.LNG:
        unit = "m3"
        quantity = ship.liquid_gas
    #TODO Should we consider the m3 information for oil?
    else:
        unit = "tonne"
        quantity = ship.dwt


    # # Heuristic1: if oil_products but dwt > 90,000t, then assume crude_oil
    if ship.dwt and float(ship.dwt) > 90e3 and commodity in [base.OIL_PRODUCTS]:
        commodity = base.CRUDE_OIL


    return [commodity, quantity, unit]


def set_commodity(ship):
    [commodity, quantity, unit] = ship_to_commodity(ship)
    ship.commodity = commodity
    ship.quantity = quantity
    ship.unit = unit
    return ship

def fix_mmsi_imo_discrepancy(date_from=None):
    query = session.query(PortCall.ship_imo, PortCall.ship_mmsi)
    if date_from is not None:
        query = query.filter(PortCall.date_utc >= to_datetime(date_from))

    portcall_ships = query.distinct().all()

    correct = []
    wrong = []
    unknown = []
    # For each ship, ask datalastic
    for portcall_ship in tqdm(portcall_ships):
        imo = portcall_ship.ship_imo
        mmsi = portcall_ship.ship_mmsi
        # others = portcall_ship.others
        found_ship = Datalastic.get_ship(mmsi=mmsi, use_cache=True)
        if not found_ship or not found_ship.imo or found_ship.imo == '0':
            from engine.marinetraffic import Marinetraffic
            found_ship = Marinetraffic.get_ship(mmsi=mmsi, use_cache=True)

        if not found_ship:
            unknown.append(mmsi)
        elif found_ship.imo == imo:
            correct.append(mmsi)
        else:
            correct_imo = found_ship.imo
            existing_ship = Ship.query.filter(Ship.imo == correct_imo).all()
            if len(existing_ship) > 1:
                logger.warning("Found more than one")
                continue

            if len(existing_ship) == 0:
                if not found_ship.imo:
                    n_imo_ships = Ship.query.filter(Ship.imo.op('~')('unknown' + '[_v]?')).count()
                    new_imo = "%s_v%d" % ('unknown', n_imo_ships + 1)
                    found_ship.imo = new_imo
                session.add(found_ship)
                session.commit()

                ship_portcalls = PortCall.query.filter(PortCall.ship_mmsi == mmsi).all()
                for ship_portcall in ship_portcalls:
                    ship_portcall.ship_imo = correct_imo

                ship_departures = Departure.query.filter(Departure.ship_imo == imo).all()
                for ship_departure in ship_departures:
                    ship_departure.ship_imo = correct_imo

                try:
                    session.commit()
                except sa.exc.IntegrityError as e:
                    logger.error(e)
                    session.rollback()

            if len(existing_ship) == 1:

                if existing_ship[0].mmsi == mmsi:
                    # Just need to plug with existing ship
                    ship_portcalls = PortCall.query.filter(PortCall.ship_mmsi == mmsi).all()
                    for ship_portcall in ship_portcalls:
                        ship_portcall.ship_imo = correct_imo

                    ship_departures = Departure.query.filter(Departure.ship_imo == imo).all()
                    for ship_departure in ship_departures:
                        ship_departure.ship_imo = correct_imo

                    try:
                        session.commit()
                    except sa.exc.IntegrityError as e:
                        logger.error(e)
                        session.rollback()

                else:
                    n_imo_ships = Ship.query.filter(Ship.imo.op('~')(correct_imo + '[_v]?')).count()
                    new_imo = "%s_v%d" % (correct_imo, n_imo_ships + 1)
                    found_ship.imo = new_imo
                    session.add(found_ship)
                    session.commit()

                    ship_portcalls = PortCall.query.filter(PortCall.ship_mmsi == mmsi).all()
                    for ship_portcall in ship_portcalls:
                        ship_portcall.ship_imo = new_imo

                    ship_departures = Departure.query.filter(Departure.ship_imo == imo).all()
                    for ship_departure in ship_departures:
                        ship_departure.ship_imo = new_imo

                    try:
                        session.commit()
                    except sa.exc.IntegrityError as e:
                        logger.error(e)
                        session.rollback()

            wrong.append(mmsi)

        # # Also update Others in portcall
        # if others and found_ship:
        #     others_imo = others.get("marinetraffic",{}).get("IMO")
        #     if others_imo and others_imo != found_ship.imo:
        #         print(2)
    print(f"=== Correct: {len(correct)} | Wrong: {len(wrong)} | Unknown: {len(unknown)} ===")


def fix_not_found():
    ships = Ship.query.filter(Ship.imo.op('~*')('NOTFOUND.*|.*_v.*')).all()

    from engine.marinetraffic import Marinetraffic
    from engine.datalastic import Datalastic

    # portcalls = PortCall.query.filter(PortCall.ship_imo.op('~*')('NOTFOUND.*')).all()

    for ship in tqdm(ships):
        portcall = PortCall.query.filter(PortCall.ship_imo == ship.imo).first()
        if portcall:
            ship_mt_id = portcall.others.get('marinetraffic', {}).get('SHIP_ID')
            new_ship = Marinetraffic.get_ship(mt_id=ship_mt_id, use_cache=True)

            # Add existing ship if not in db
            if new_ship:
                existing_ship = Ship.query.filter(sa.and_(
                    Ship.imo == new_ship.imo,
                    sa.or_(
                        Ship.mmsi == new_ship.mmsi,
                        Ship.name == new_ship.name,
                        Ship.dwt == new_ship.dwt
                    )
                )).first()

                if not existing_ship and new_ship.imo is not None:
                    try:
                        session.add(new_ship)
                        session.commit()
                    except IntegrityError:
                        session.rollback()  # Do manual edit here for now

                if new_ship and new_ship.name == ship.name:
                    try:
                        session.query(PortCall) \
                            .filter(PortCall.ship_imo == ship.imo) \
                            .update({'ship_imo': new_ship.imo})
                        session.commit()
                    except IntegrityError as e:
                        session.rollback()  # Do manual delete here for now

                    session.query(Departure) \
                        .filter(Departure.ship_imo == ship.imo) \
                        .update({'ship_imo': new_ship.imo})
                    session.commit()

                    Ship.query.filter(Ship.imo == ship.imo).delete()
                    session.commit()
                else:
                    # What to do?
                    logger.info("%s \n vs. \n %s " % (str(new_ship.others),
                                                      str(ship.others)))

