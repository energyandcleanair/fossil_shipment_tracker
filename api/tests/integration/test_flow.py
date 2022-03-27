import requests
import urllib
import pandas as pd
import json


def test_flow(app):

    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json"}
        response = test_client.get('/v0/flow?'+ urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) > 0
        assert all([x['arrival_date_utc'] > x['departure_date_utc'] for x in data])

