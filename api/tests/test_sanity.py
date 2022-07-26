import requests
import urllib
import pandas as pd
import json
import pandas as pd
import base

from base.models import Shipment

def test_voyage(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert all([x['status'] != 'completed' or x['arrival_date_utc'] > x['departure_date_utc'] for x in data])

        voyages = pd.DataFrame(data)
        assert voyages.duplicated(subset=['id']).any() == False
        voyages['former_arrival_date_utc'] = voyages.sort_values(['ship_imo', 'arrival_date_utc']).groupby("ship_imo")['arrival_date_utc'].shift(1)
        v = voyages.sort_values(['ship_imo', 'arrival_date_utc'])

        problematic = v.loc[(v.former_arrival_date_utc > v.departure_date_utc) & (v.status != base.UNDETECTED_ARRIVAL)]
        assert len(problematic) == 0


def test_no_coal_in_tanker(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json", "commodity": "coal"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0


        voyages = pd.DataFrame(data)
        assert voyages.duplicated(subset=['id']).any() == False
        assert not any(['tanker' in y for y in voyages.ship_type.str.lower().unique()])


def test_no_coal_sts(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"commodity": "coal",
                  "destination_iso2": "KR",
                  "commodity_destination_iso2": "CN"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))

        # assert empty response - we should have no situations where there is coal and
        # the destination != commodity_destination
        assert response.status_code == 204


def test_yeosu_sts(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json",
                  "destination_iso2": "KR"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0

        voyages = pd.DataFrame(data)

        voyages_sts = voyages[voyages['destination_iso2'] != voyages['commodity_destination_iso2']]

        assert not voyages_sts.empty
