import pandas as pd
from engine.alert import *
from base.models import Shipment, Departure
from base.db import init_db


# def imos_are_matching():
#     imos = session.query(Arrival.)


def test_alerts(app):
    init_db(drop_first=False)
    update()