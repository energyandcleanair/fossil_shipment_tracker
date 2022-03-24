from base.db import session
from base.logger import logger
from models import Ship
from engine.datalastic import Datalastic
from engine.marinetraffic import Marinetraffic



def fill(imos):
    """
    Fill database (append or upsert) ship information
    :param imos: list of imos codes
    :param source: what data source to use
    :return:
    """

    # Fill missing ships
    def get_missing_ships(imos):
        existing_imos = [value for value, in session.query(Ship.imo).all()]
        return [x for x in imos if str(x) not in existing_imos]

    # First with Datalastic
    ships = [Datalastic.get_ship(imo=x, query_if_not_in_cache=False) for x in get_missing_ships(imos)]
    for ship in ships:
        if ship:
            session.add(ship)
    session.commit()

    # Then with Marinetraffic for those still missing
    ships = [Marinetraffic.get_ship(imo=x) for x in get_missing_ships(imos)]
    for ship in ships:
        if ship:
            session.add(ship)
    session.commit()

    missing = get_missing_ships(imos)
    if missing:
        logger.warning("Some ships are still missing: %s" % (",".join(missing)))

    return
