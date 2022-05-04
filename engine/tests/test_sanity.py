import pandas as pd
from engine.entsog import *
from base.models import Shipment, Departure



# def imos_are_matching():
#     imos = session.query(Arrival.)


def test_manual_shipments():
    Shipment.query.join(Departure, Departure.id == Shipment.departure_id)