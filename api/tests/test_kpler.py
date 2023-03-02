import requests
import urllib
import pandas as pd


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


def test_kpler_pricing(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {"format": "json", "origin_iso2": "RU", "destination_iso2": "SE", "pricing_scenario": "default"}
        response = test_client.get("/v0/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) >= 4
        data_df = pd.DataFrame(data)
        expected_columns = set(["origin_iso2", "destination_iso2", "product", "date", "value_eur"])
        assert set(data_df.columns) >= expected_columns
        # check pricing was found for these shipments
        assert data_df[data_df['value_eur'].isnull()].empty
