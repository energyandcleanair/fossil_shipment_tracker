import requests
import urllib
import pandas as pd
import json



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

