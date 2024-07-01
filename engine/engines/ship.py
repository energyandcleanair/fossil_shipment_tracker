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


def fill(imos=[]):
    """
    Fill database (append or upsert) ship information
    :param imos: list of imos codes
    :param source: what data source to use
    :return:
    """
    imos = list(set([str(x) for x in imos]))

    ships = [Ship(imo=imo) for imo in imos if imo]

    session.add_all(ships)
    session.commit()

    return True
