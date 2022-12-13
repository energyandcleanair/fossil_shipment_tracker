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
        response = test_client.get('/v0/chart/eu_gas_consumption?' + urllib.parse.urlencode(params))
        assert response.status_code == 200

        params = {'pivot_by_year': 'True'}
        response = test_client.get('/v0/chart/eu_gas_consumption?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)
        assert set(data_df.columns) >= set(['date', 'type', '2021', '2022'])



def test_monthly_payments(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {}
        response = test_client.get('/v0/chart/monthly_payments?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)
        assert set(data_df.columns) >= set(['destination_region', 'month', 'Oil', 'Coal', 'Gas'])


def test_departure_destination(app):
    with app.test_client() as test_client:
        params = {}
        response = test_client.get('/v0/chart/departure_by_destination?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)

        # Order matters for Flourish
        # If this changes, please update column selection in Flourish
        assert data_df.columns[0] == 'commodity_group_name'
        assert data_df.columns[1] == 'departure_date'
        assert len(data_df.columns) == 15


def test_departure_ownership(app):
    with app.test_client() as test_client:
        params = {}
        response = test_client.get('/v0/chart/departure_by_ownership?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)

        # Order matters for Flourish
        # If this changes, please update column selection in Flourish
        assert data_df.columns[0] == 'commodity_group_name'
        assert data_df.columns[1] == 'departure_date'
        assert len(data_df.columns) == 6


def test_product_on_water(app):
    with app.test_client() as test_client:
        params = {}
        response = test_client.get('/v0/chart/product_on_water?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)

        # Order matters for Flourish
        # If this changes, please update column selection in Flourish
        assert data_df.columns[0] == 'commodity'
        assert data_df.columns[1] == 'date'
