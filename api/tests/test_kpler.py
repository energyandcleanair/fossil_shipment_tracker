import requests
import urllib
import geopandas as gpd
import io
import pandas as pd
import base
import json
import numpy as np
import datetime as dt

from base.models import Position, ShipmentArrivalBerth
from base.db import session
from base.utils import to_datetime
from base import PRICING_DEFAULT, PRICING_PRICECAP


def test_kpler(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json", "origin_iso2": "RU", "destination_iso2": "SE"}
        response = test_client.get("/v0/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) >= 4
        data_df = pd.DataFrame(data)
        expected_columns = set(["origin_iso2", "destination_iso2", "product", "date"])
        assert set(data_df.columns) >= expected_columns
