import requests
import urllib
import geopandas as gpd
import io
import pandas as pd
import base
import json
from base.models import Position, ShipmentArrivalBerth
from base.db import session



def test_ship(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {}
        response = test_client.get('/v0/ship?'+ urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert all([x['imo'] is not None for x in data])

        params = {'imo': data[0]["imo"]}
        response = test_client.get('/v0/ship?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data2 = response.json["data"]
        assert len(data2) == 1

        params = {'imo': ",".join([x["imo"] for x in data[0:3]])}
        response = test_client.get('/v0/ship?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data3 = response.json["data"]
        assert len(data3) == 3


def test_port(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {}
        response = test_client.get('/v0/port?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        # assert all([x['unlocode'] is not None for x in data])

        params = {'unlocode': data[0]["unlocode"]}
        response = test_client.get('/v0/port?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data2 = response.json["data"]
        assert len(data2) == 1

        params = {'unlocode': ",".join([x["unlocode"] for x in data[0:3]])}
        response = test_client.get('/v0/port?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data3 = response.json["data"]
        assert len(data3) == 3




def test_portprice(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {}
        response = test_client.get('/v0/portprice?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0


def test_price(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {}
        response = test_client.get('/v0/price?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0

        params = {'commodity': base.PIPELINE_GAS}
        response = test_client.get('/v0/price?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data2 = response.json["data"]
        assert set([x['commodity'] for x in data2]) == set([base.PIPELINE_GAS])



def test_position(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        ship_imo = session.query(Position.ship_imo).first()[0]
        params = {"ship_imo": ship_imo}
        response = test_client.get('/v0/position?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0


        # Test data required for finding missing berth
        params = {"has_arrival_berth": "false", "speed_max": "0.1", "status": "completed"}
        response = test_client.get('/v0/position?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0

def test_berth(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json"}
        response = test_client.get('/v0/berth?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0

        # Test commodity parameter
        params = {"format": "geojson"}
        response = test_client.get('/v0/berth?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        gdf = gpd.read_file(io.StringIO(json.dumps(data)))
        assert len(gdf) > 0
        assert "geometry" in gdf.columns


def test_pipelineflow_pricing(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json"}
        response = test_client.get('/v0/overland?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert all([x["value_eur"] > 0 for x in data])
        assert len(set([x['id'] for x in data])) == len(data)


def test_pipelineflow_aggregation(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        aggregate_bys = [
            [],
            ['destination_region', 'commodity'],
            ['destination_region', 'commodity', 'date'],
        ]

        for aggregate_by in aggregate_bys:
            params = {"format": "json", "aggregate_by": ','.join(aggregate_by)}
            response = test_client.get('/v0/pipelineflow?' + urllib.parse.urlencode(params))
            assert response.status_code == 200
            data = response.json["data"]
            assert len(data) > 0
            data_df = pd.DataFrame(data)

            expected_columns = set(aggregate_by + ['value_tonne', 'value_eur', 'value_m3']) if aggregate_by \
                else set(['id', 'commodity', 'destination_iso2', 'destination_country', 'destination_region',
                          'date', 'value_tonne', 'value_eur', 'value_m3'])
            assert set(data_df.columns) == expected_columns

