import base

from base.db import session
from base.logger import logger
from base.models import Ship
from engine.datalastic import Datalastic
from engine.marinetraffic import Marinetraffic


def fill(imos=[], mmsis=[]):
    """
    Fill database (append or upsert) ship information
    :param imos: list of imos codes
    :param source: what data source to use
    :return:
    """
    logger.info("Adding missing ships. MMSI: %s | IMO: %s" % (",".join(imos), ",".join(mmsis)))

    # Fill missing ships
    def get_missing_ships_imos(imos):
        existing_imos = [value for value, in session.query(Ship.imo).all()]
        return [x for x in imos if str(x) not in existing_imos]

    def get_missing_ships_mmsis(mmsis):
        existing_mmsis = [value for value, in session.query(Ship.mmsi).all()]
        return [x for x in mmsis if str(x) not in existing_mmsis]

    # First with Datalastic
    ships = [Datalastic.get_ship(imo=x, query_if_not_in_cache=False) for x in get_missing_ships_imos(imos)]
    upload_ships(ships)

    ships = [Datalastic.get_ship(mmsi=x, query_if_not_in_cache=False) for x in get_missing_ships_mmsis(mmsis)]
    upload_ships(ships)

    # Then with Marinetraffic for those still missing
    ships = [Marinetraffic.get_ship(imo=x) for x in get_missing_ships_imos(imos)]
    upload_ships(ships)

    ships = [Marinetraffic.get_ship(mmsi=x) for x in get_missing_ships_mmsis(mmsis)]
    upload_ships(ships)

    missing = get_missing_ships_imos(imos)
    missing.extend(get_missing_ships_mmsis(mmsis))
    if missing:
        logger.warning("Some ships are still missing: %s" % (",".join(missing)))

    return


def upload_ships(ships):
    for ship in ships:
        if ship and ship.imo is not None:
            ship = set_commodity(ship)
            session.add(ship)
    session.commit()


def ship_to_commodity(ship):
    """
    Guess commodity, and quantity of ship
    :param ship:
    :return: [commodity, quantity, unit]
    """
    import re
    try:
        if re.match('Crude oil', ship.type, re.IGNORECASE) \
                or re.match('Crude oil', ship.subtype, re.IGNORECASE):
            commodity = base.CRUDE_OIL
        elif re.match('OIL/CHEMICAL', ship.type, re.IGNORECASE) \
                or re.match('Oil or chemical', ship.subtype, re.IGNORECASE):
            commodity = base.OIL_OR_CHEMICAL
        elif re.match('OIL PRODUCTS', ship.type, re.IGNORECASE) \
                or re.match('Oil products', ship.subtype, re.IGNORECASE):
            commodity = base.OIL_PRODUCTS
        elif re.match('LNG', ship.type, re.IGNORECASE) \
             or re.match('LNG', ship.subtype, re.IGNORECASE):
            commodity = base.LNG
        elif re.match('Ore or Oil', ship.subtype, re.IGNORECASE):
            commodity = base.OIL_OR_ORE
        else:
            commodity = base.UNKNOWN

    except TypeError:
        commodity = base.UNKNOWN

    if ship.liquid_gas is not None and commodity==base.LNG:
        unit = "m3"
        quantity = ship.liquid_gas
    #TODO Should we consider the m3 information for oil?
    else:
        unit = "tonne"
        quantity = ship.dwt

    return [commodity, quantity, unit]


def set_commodity(ship):
    [commodity, quantity, unit] = ship_to_commodity(ship)
    ship.commodity = commodity
    ship.quantity = quantity
    ship.unit = unit
    return ship