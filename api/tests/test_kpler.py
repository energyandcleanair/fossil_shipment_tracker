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
        params = {
            "format": "json",
            "origin_iso2": "RU,CN",
            "product": "Crude,Diesel",
            "date_from": "2022-12-01",
            "date_to": "2022-12-31",
            "origin_by": "country,port",
            "destination_by": "country,port",
            # "aggregate_by": "origin_iso2,product, origin_type,destination_type"
        }
        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)

        grouped = (
            data_df.groupby(["origin_type", "destination_type", "origin_iso2", "product"])
            .value_tonne.sum()
            .reset_index()
        )

        # Russia - Crude - Dec 2022 = 18.7
        ru_crude = grouped[(grouped.origin_iso2 == "RU") & (grouped["product"] == "Crude")]
        assert len(ru_crude) == 4
        assert all(round(ru_crude.value_tonne / 1e6) == 19)
        assert all(abs(ru_crude.value_tonne - ru_crude.value_tonne.iloc[0]) < 1e-6)

        # China - Diesel - Dec 2022 = 2.16
        cn_diesel = grouped[(grouped.origin_iso2 == "CN") & (grouped["product"] == "Diesel")]
        assert len(cn_diesel) == 4
        assert all(round(cn_diesel.value_tonne / 1e6) == 2)
        assert all(abs(cn_diesel.value_tonne - cn_diesel.value_tonne.iloc[0]) < 1e-6)

        expected_columns = set(
            [
                "origin_iso2",
                "destination_iso2",
                "product",
                "date",
                "value_tonne",
                "value_eur",
                "value_usd",
            ]
        )
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
