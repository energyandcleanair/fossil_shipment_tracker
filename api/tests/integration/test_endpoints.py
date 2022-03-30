import requests
import urllib
import geopandas as gpd
import io
import pandas as pd
import json
from base.models import Position
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
        assert all([x['unlocode'] is not None for x in data])

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


def test_voyage(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert all([x['arrival_date_utc'] > x['departure_date_utc'] for x in data])

        # Test commodity parameter
        params = {"format": "json", "commodity": "crude_oil"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert all([x['commodity'] == "crude_oil" for x in data])

        # Test id parameter
        params = {"format": "json", "id": data[0]["id"]}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) == 1


def test_voyage_geojson(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "geojson"}
        response = test_client.get('/v0/voyage?' + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        gdf = gpd.read_file(io.StringIO(json.dumps(data)))
        assert len(gdf) > 0
        assert "geometry" in gdf.columns

def test_position(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        flow_id = session.query(Position.flow_id).first()[0]
        params = {"voyage_id": flow_id}
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
