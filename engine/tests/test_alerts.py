import pandas as pd
import pytest
from engines.alert import *
from base.models import Shipment, Departure
from base.db import init_db


# def imos_are_matching():
#     imos = session.query(Arrival.)


@pytest.mark.system
def test_alerts():
    init_db(drop_first=False)
    update()
