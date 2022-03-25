import datalastic
import departure
from base.db import session
from models import Departure


def update_all():

    # Get latest position or latest departure
    dangling_imo_dates = departure.get_dangling_imo_dates()

    # For each


def update(imo):
    return


