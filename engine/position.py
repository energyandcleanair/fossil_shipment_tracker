import datalastic
import departure
from base.db import session
from models import Departure


def update_all():

    # Get latest position or latest departure
    dangling_imos = departure.get_dangling_imos()

    # For each


def update(imo):


    Departure