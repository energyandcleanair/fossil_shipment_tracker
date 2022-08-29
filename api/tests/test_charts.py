import requests
import urllib
import geopandas as gpd
import io
import pandas as pd
import base
import json
from base.models import Position, ShipmentArrivalBerth
from base.db import session


def test_gas_consumption(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {}
        response = test_client.get('/v0/chart/gas_consumption?' + urllib.parse.urlencode(params))
        assert response.status_code == 200

