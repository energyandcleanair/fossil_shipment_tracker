import requests
import urllib
import geopandas as gpd
import io
import pandas as pd
import base
import json
from base.models import Position, ShipmentArrivalBerth
from base.db import session

def test_flaring_facility(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {}
        response = test_client.get('/v0/flaring_facility?' + urllib.parse.urlencode(params))
        assert response.status_code == 200

        params = {'facility_id': 54}
        response = test_client.get('/v0/flaring_facility?' + urllib.parse.urlencode(params))
        assert response.status_code == 200



def test_flaring(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"date_from": -10}
        response = test_client.get('/v0/flaring?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert all([x["value"] >= 0 for x in data])

        # One value per date

