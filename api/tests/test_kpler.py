import numpy as np
import requests
import urllib
import pandas as pd
from sqlalchemy import func

from base.models import KplerFlow
from base.db import session
from base.env import get_env


def test_kpler_v1_pricing(app):

    with app.test_client() as test_client:
        # Test that the join with pricing doesn't remove flows
        # To do so, we query both from the api and directly from the flow table
        # and compare the results

        iso2s = ["RU", "TR", "CN", "MY"]
        products = ["Crude", "Diesel", "Crude/Co"]
        params = {
            "format": "json",
            "origin_iso2": ",".join(iso2s),
            "product": ",".join(products),
            "date_from": "2022-12-01",
            "date_to": "2022-12-31",
            "aggregate_by": "origin_iso2,product",
            "api_key": get_env("API_KEY"),
        }
        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        api = pd.DataFrame(response.json["data"])
        assert len(api) > 0  # Not all countries export all products

        # Collect directly from table
        raw_query = (
            session.query(
                KplerFlow.from_iso2.label("origin_iso2"),
                KplerFlow.product,
                func.sum(KplerFlow.value).label("value_tonne"),
            )
            .filter(
                KplerFlow.from_iso2.in_(iso2s),
                KplerFlow.product.in_(products),
                KplerFlow.date >= "2022-12-01",
                KplerFlow.date <= "2022-12-31",
                KplerFlow.unit == "t",
                KplerFlow.from_split == "country",
                KplerFlow.to_split == "country",
            )
            .group_by(KplerFlow.from_iso2, KplerFlow.product)
        )
        raw = pd.read_sql(raw_query.statement, session.bind)
        assert len(raw) == len(api)

        # Compare
        merge = api[["origin_iso2", "product", "value_tonne"]].merge(
            raw, how="left", on=["origin_iso2", "product"], suffixes=("_api", "_raw")
        )

        assert all(merge.value_tonne_api == merge.value_tonne_raw)


def test_kpler_v1(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {
            "format": "json",
            "origin_iso2": "RU,CN",
            "product": "Crude,Diesel",
            "date_from": "2022-12-01",
            "date_to": "2022-12-31",
            "origin_type": "country,port",
            "destination_type": "country,port",
            "api_key": get_env("API_KEY"),
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

        assert set(grouped["product"].unique()) == set(["Crude", "Diesel"])
        # Russia - Crude - Dec 2022 = 18.7
        ru_crude = grouped[(grouped.origin_iso2 == "RU") & (grouped["product"] == "Crude")]
        assert len(ru_crude) == 4
        assert all(round(ru_crude.value_tonne / 1e6) == 19)
        assert all(np.isclose(ru_crude.value_tonne, ru_crude.value_tonne, rtol=1e-6))

        # China - Diesel - Dec 2022 = 2.16
        cn_diesel = grouped[(grouped.origin_iso2 == "CN") & (grouped["product"] == "Diesel")]
        assert len(cn_diesel) == 4
        assert all(round(cn_diesel.value_tonne / 1e6) == 2)
        assert all(np.isclose(cn_diesel.value_tonne, cn_diesel.value_tonne, rtol=1e-6))

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


def test_kpler_v1_key(app):

    with app.test_client() as test_client:
        # Test that the join with pricing doesn't remove flows
        # To do so, we query both from the api and directly from the flow table
        # and compare the results

        iso2s = ["RU", "TR", "CN", "MY"]
        products = ["Crude", "Diesel", "Crude/Co"]
        params = {
            "format": "json",
            "origin_iso2": ",".join(iso2s),
            "product": ",".join(products),
            "date_from": "2022-12-01",
            "date_to": "2022-12-31",
            "aggregate_by": "origin_iso2,product",
            "api_key": "THISISNOTACORRECTKEY",
        }

        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 403


def test_kpler_values(app):
    """
    Test values against manually collected ones
    :param app:
    :return:
    """
    import country_converter as coco

    cc = coco.CountryConverter()

    manual_values = pd.read_csv("assets/kpler/crude_yearly.csv")
    # Melt
    manual_values = manual_values.melt(
        id_vars=["date"], var_name="country", value_name="value_ktonne"
    )
    manual_values["value_tonne"] = manual_values["value_ktonne"] * 1e3
    manual_values["origin_iso2"] = manual_values["country"].map(lambda x: cc.convert(x, to="ISO2"))
    manual_values.rename(columns={"date": "year"}, inplace=True)

    with app.test_client() as test_client:

        params = {
            "format": "json",
            "date_from": "2022-01-01",
            "date_to": "2022-12-31",
            "origin_iso2": ",".join(["RU", "TR", "CN", "MY", "US", "EG", "AE"]),
            "aggregate_by": "origin_iso2,product,year",
            "product": ",".join(["Crude", "Crude/Co", "Condensate"]),
            "api_key": get_env("API_KEY"),
        }

        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)
        data_df = data_df.groupby(["origin_iso2", "year"]).value_tonne.sum().reset_index()
        # Merge
        merge = data_df[["origin_iso2", "year", "value_tonne"]].merge(
            manual_values[["origin_iso2", "year", "value_tonne"]],
            how="left",
            on=["origin_iso2", "year"],
            suffixes=("_api", "_manual"),
        )

        assert all(np.isclose(merge.value_tonne_api, merge.value_tonne_manual, rtol=1e-2))


def test_kpler_crude_export(app):
    """
    Test values against manually collected ones
    :param app:
    :return:
    """
    import country_converter as coco

    cc = coco.CountryConverter()

    manual_values = pd.read_csv("assets/kpler/crude_exports.csv")
    manual_values = manual_values.melt(
        id_vars=["date"], var_name="country", value_name="value_ktonne"
    )
    manual_values["value_tonne"] = manual_values["value_ktonne"] * 1e3
    manual_values["origin_iso2"] = manual_values["country"].map(lambda x: cc.convert(x, to="ISO2"))
    manual_values.rename(columns={"date": "year"}, inplace=True)

    with app.test_client() as test_client:

        params = {
            "format": "json",
            "date_from": "2022-01-01",
            "date_to": "2022-12-31",
            "origin_iso2": ",".join(["RU", "TR", "CN", "MY", "US", "EG", "AE"]),
            "aggregate_by": "origin_iso2,product,year",
            "product": ",".join(["Crude", "Crude/Co", "Condensate"]),
            "api_key": get_env("API_KEY"),
        }

        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)
        data_df = data_df.groupby(["origin_iso2", "year"]).value_tonne.sum().reset_index()
        # Merge
        merge = data_df[["origin_iso2", "year", "value_tonne"]].merge(
            manual_values[["origin_iso2", "year", "value_tonne"]],
            how="left",
            on=["origin_iso2", "year"],
            suffixes=("_api", "_manual"),
        )

        assert all(np.isclose(merge.value_tonne_api, merge.value_tonne_manual, rtol=1e-2))


def test_kpler_crude_ru_exports(app):
    """
    Test values against manually collected ones
    :param app:
    :return:
    """
    import country_converter as coco

    cc = coco.CountryConverter()

    manual_values = pd.read_csv("assets/kpler/crude_ru_exports.csv")
    manual_values = manual_values.melt(
        id_vars=["date"], var_name="country", value_name="value_ktonne"
    )
    manual_values["value_tonne"] = manual_values["value_ktonne"] * 1e3
    manual_values["destination_iso2"] = manual_values["country"].map(
        lambda x: cc.convert(x, to="ISO2")
    )
    manual_values.rename(columns={"date": "year"}, inplace=True)

    with app.test_client() as test_client:

        params = {
            "format": "json",
            "date_from": "2022-01-01",
            "date_to": "2022-12-31",
            "origin_iso2": "RU",
            "aggregate_by": "destination_iso2,product,year",
            "product": ",".join(["Crude", "Crude/Co", "Condensate"]),
            "api_key": get_env("API_KEY"),
        }

        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)
        data_df = data_df.groupby(["destination_iso2", "year"]).value_tonne.sum().reset_index()
        # Merge
        merge = data_df[["destination_iso2", "year", "value_tonne"]].merge(
            manual_values[["destination_iso2", "year", "value_tonne"]],
            how="left",
            on=["destination_iso2", "year"],
            suffixes=("_api", "_manual"),
        )

        assert all(np.isclose(merge.value_tonne_api, merge.value_tonne_manual, rtol=1e-2))


def test_kpler_diesel_exports(app):
    """
    Test values against manually collected ones
    :param app:
    :return:
    """
    import country_converter as coco

    cc = coco.CountryConverter()

    manual_values = pd.read_csv("assets/kpler/diesel_exports.csv")
    manual_values = manual_values.melt(
        id_vars=["date"], var_name="country", value_name="value_ktonne"
    )
    manual_values["value_tonne"] = manual_values["value_ktonne"] * 1e3
    manual_values["origin_iso2"] = manual_values["country"].map(lambda x: cc.convert(x, to="ISO2"))
    manual_values.rename(columns={"date": "year"}, inplace=True)

    with app.test_client() as test_client:
        params = {
            "format": "json",
            "date_from": "2022-01-01",
            "date_to": "2022-12-31",
            "origin_iso2": ",".join(["RU", "CN", "IN", "SG", "TR", "AE"]),
            "aggregate_by": "origin_iso2,product,year",
            "product": "Diesel",
            "api_key": get_env("API_KEY"),
        }

        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)
        data_df = data_df.groupby(["origin_iso2", "year"]).value_tonne.sum().reset_index()
        # Merge
        merge = data_df[["origin_iso2", "year", "value_tonne"]].merge(
            manual_values[["origin_iso2", "year", "value_tonne"]],
            how="left",
            on=["origin_iso2", "year"],
            suffixes=("_api", "_manual"),
        )

        assert all(np.isclose(merge.value_tonne_api, merge.value_tonne_manual, rtol=1e-2))


def test_kpler_gasoline_exports(app):
    """
    Test values against manually collected ones
    :param app:
    :return:
    """
    import country_converter as coco

    cc = coco.CountryConverter()

    manual_values = pd.read_csv("assets/kpler/gasoline_exports.csv")
    manual_values = manual_values.melt(
        id_vars=["date"], var_name="country", value_name="value_ktonne"
    )
    manual_values["value_tonne"] = manual_values["value_ktonne"] * 1e3
    manual_values["origin_iso2"] = manual_values["country"].map(lambda x: cc.convert(x, to="ISO2"))
    manual_values.rename(columns={"date": "year"}, inplace=True)

    with app.test_client() as test_client:
        params = {
            "format": "json",
            "date_from": "2022-01-01",
            "date_to": "2022-12-31",
            "origin_iso2": ",".join(["RU", "CN", "IN", "SG", "TR", "AE"]),
            "aggregate_by": "origin_iso2,product,year",
            "product": "Gasoline",
            "api_key": get_env("API_KEY"),
        }

        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)
        data_df = data_df.groupby(["origin_iso2", "year"]).value_tonne.sum().reset_index()
        # Merge
        merge = data_df[["origin_iso2", "year", "value_tonne"]].merge(
            manual_values[["origin_iso2", "year", "value_tonne"]],
            how="left",
            on=["origin_iso2", "year"],
            suffixes=("_api", "_manual"),
        )

        assert all(np.isclose(merge.value_tonne_api, merge.value_tonne_manual, rtol=1e-2))
