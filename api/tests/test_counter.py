import requests
import urllib
import geopandas as gpd
import io
import pandas as pd
import base
import json
from base.models import Position, ShipmentArrivalBerth
from base.db import session


def test_counter_last(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        response = test_client.get('/v0/counter')
        assert response.status_code == 200

        params = {"format": "json"}
        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) >= 4
        data_df = pd.DataFrame(data)
        expected_columns = set(['destination_region', 'date', 'commodity',
                                'commodity_group', 'eur_per_sec', 'total_eur'])
        assert set(data_df.columns) >= expected_columns

        params = {"aggregate_by": "commodity_group", "format": "json"}
        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) == 4 # 3 main commodities plus total
        data_df = pd.DataFrame(data)
        expected_columns = set(
            ['date', 'commodity_group', 'eur_per_sec', 'total_eur'])
        assert set(data_df.columns) >= expected_columns

        params = {"aggregate_by": "destination_region,commodity_group", "format": "json"}
        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) >= 4
        data_df = pd.DataFrame(data)
        expected_columns = set(
            ['destination_region', 'date', 'commodity_group', 'eur_per_sec', 'total_eur'])
        assert set(data_df.columns) >= expected_columns


def test_counter(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        response = test_client.get('/v0/counter')
        assert response.status_code == 200

        params = {"format": "json"}
        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) >= 4
        data_df = pd.DataFrame(data)

        expected_columns = set(['destination_region',
                                'date', 'commodity_group', 'eur_per_sec', 'total_eur'])
        assert set(data_df.columns) >= expected_columns

        params = {"format": "json"}
        response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        data_df = pd.DataFrame(data)

        expected_columns = set(['commodity', 'commodity_group', 'date',
                                'destination_iso2', 'destination_country', 'destination_region',
                                'value_tonne', 'value_eur'])
        assert set(data_df.columns) == expected_columns


def test_counter_cumulate(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json", "cumulate": True}
        response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        data_df = pd.DataFrame(data)


def test_counter_rolling(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        response = test_client.get('/v0/counter')
        assert response.status_code == 200

        params = {"format": "json", "rolling_days": 7}
        response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        data_df = pd.DataFrame(data)

        params = {"format": "json", "rolling_days": 7, "aggregate_by": "date,destination_region,commodity_group"}
        response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        data_df = pd.DataFrame(data)


def test_counter_aggregation(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:

        aggregate_bys = [
            [],
            ['destination_country', 'commodity'],
            ['destination_region', 'commodity_group'],
            ['destination_region', 'commodity'],
            ['destination_region'],
        ]

        for aggregate_by in aggregate_bys:
            params = {"format": "json", "aggregate_by": ','.join(aggregate_by)}
            response = test_client.get('/v0/counter?' + urllib.parse.urlencode(params))
            assert response.status_code == 200
            data = response.json["data"]
            assert len(data) > 0
            data_df = pd.DataFrame(data)

            expected_columns = set(aggregate_by + ['value_tonne', 'value_eur']) if aggregate_by \
                else set(['commodity', 'commodity_group', 'destination_region',
                          'destination_iso2', 'destination_country',
                          'date','value_tonne', 'value_eur'])

            if "commodity" in aggregate_by:
                expected_columns.update(["commodity_group"])

            if "destination_iso2" in aggregate_by or 'destination_country' in aggregate_by:
                expected_columns.update(["destination_country", 'destination_iso2', 'destination_region'])

            assert set(data_df.columns) == expected_columns



        # Test that sum countries is conserved
        params = {"format": "json"}
        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data1 = response.json["data"]
        assert len(data1) >= 4
        data1_df = pd.DataFrame(data1)
        sum1 = data1_df.total_eur.sum()

        params = {"format": "json", "aggregate_by": "destination_country,commodity"}
        response = test_client.get('/v0/counter_last?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data2 = response.json["data"]
        data2_df = pd.DataFrame(data2)
        sum2 = data2_df.total_eur.sum()

        assert round(sum1, 2) == round(sum2, 2)
        assert sum1 > 1e9