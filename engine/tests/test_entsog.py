import pandas as pd
from engine.entsog import *


def test_interconnections():
    ic = get_interconnections()
    assert isinstance(ic, pd.DataFrame)
    assert set(["pointKey", "pointKey"]) < set(ic.columns)
    return


def test_get_flows(test_db):
    get_flows(date_from='2022-01-01')
    return
#
# def test_departure(test_db):
#     departure.update()
#     return
#
#
# def test_arrival(test_db):
#     arrival.update()
#     return
#
#
# def test_shipment(test_db):
#     # shipment.update()
#     # shipment.update_positions()
#     berth.detect_arrival_berth()
#     return