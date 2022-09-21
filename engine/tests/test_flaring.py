import pandas as pd
from engine.flaring import *
from base.models import Shipment, Departure
from base.db import init_db


def test_flaring(app):
    init_db(drop_first=False)
    update()
    # get_fields()
    # download_nvf_date('2022-01-01')
    # download_nvf('2022-01-01', '2022-01-05')