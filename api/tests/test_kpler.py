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


def test_kpler_v1(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        from_bys = ["country", "port"]
        to_bys = ["country", "port"]
        results = []
        for from_by in from_bys:
            for to_by in to_bys:
                params = {
                    "format": "json",
                    "origin_iso2": "RU",
                    "product": "Crude",
                    "date_from": "2022-01-01",
                    "date_to": "2022-12-31",
                    "origin_by": from_by,
                    "destination_by": to_by,
                }
            response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
            assert response.status_code == 200
            results.append(pd.DataFrame(response.json["data"]))

        expected_columns = set(["origin_iso2", "destination_iso2", "product", "date"])
        assert set(data_df.columns) >= expected_columns


def test_kpler_pricing(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {
            "format": "json",
            "origin_iso2": "RU",
            "destination_iso2": "SE",
            "pricing_scenario": "default",
        }
        response = test_client.get("/v0/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        assert len(data) >= 4
        data_df = pd.DataFrame(data)
        expected_columns = set(["origin_iso2", "destination_iso2", "product", "date", "value_eur"])
        assert set(data_df.columns) >= expected_columns
        # check pricing was found for these shipments
        assert data_df[data_df["value_eur"].isnull()].empty
