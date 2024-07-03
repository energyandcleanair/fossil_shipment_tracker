from tqdm import tqdm
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
import base
import json

from base.db import session
from base.logger import logger, logger_slack
from base.models import (
    Ship,
    PortCall,
    Departure,
    Shipment,
    ShipmentDepartureBerth,
    Trajectory,
    MTVoyageInfo,
    Arrival,
)
from base.utils import to_datetime, to_list
import numpy as np

import sqlalchemy as sa

from base.db_utils import upsert


def fill(imos=[]):
    """
    Fill database (append or upsert) ship information
    :param imos: list of imos codes
    :param source: what data source to use
    :return:
    """

    not_null_imos = [imo for imo in imos if imo]  # Filter out nones
    deduped_imos = list(set([str(x) for x in not_null_imos]))

    existing_ships = session.query(Ship).filter(Ship.imo.in_(deduped_imos)).all()
    existing_ship_imos = [ship.imo for ship in existing_ships]
    new_imos = [imo for imo in deduped_imos if imo not in existing_ship_imos]

    ships = [Ship(imo=imo) for imo in new_imos]

    session.add_all(ships)
    session.commit()

    return True
