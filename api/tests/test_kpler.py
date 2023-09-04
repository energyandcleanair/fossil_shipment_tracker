import numpy as np
import requests
from http import HTTPStatus
import urllib
import pandas as pd
from sqlalchemy import func
import sqlalchemy as sa

from base.models import KplerFlow
from base.db import session
from base.env import get_env


def test_kpler_v1_pricing(app):

    with app.test_client() as test_client:
        # Test that the join with pricing doesn't remove flows
        # To do so, we query both from the api and directly from the flow table
        # and compare the results

        iso2s = ["RU", "TR", "CN", "MY"]
        groups = ["Gasoil/Diesel", "Crude/Co"]
        params = {
            "format": "json",
            "origin_iso2": ",".join(iso2s),
            "group": ",".join(groups),
            "date_from": "2022-12-01",
            "date_to": "2022-12-31",
            "aggregate_by": "origin_iso2,group",
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
                KplerFlow.group,
                func.sum(KplerFlow.value).label("value_tonne"),
            )
            .filter(
                KplerFlow.is_valid,
                KplerFlow.from_iso2.in_(iso2s),
                KplerFlow.group.in_(groups),
                sa.or_(KplerFlow.commodity != "Condensate", KplerFlow.commodity == None),
                KplerFlow.date >= "2022-12-01",
                KplerFlow.date <= "2022-12-31",
                KplerFlow.unit == "t",
                KplerFlow.from_split == "country",
                KplerFlow.to_split == "country",
            )
            .group_by(KplerFlow.from_iso2, KplerFlow.group)
        )
        raw = pd.read_sql(raw_query.statement, session.bind)
        assert len(raw) == len(api)

        # Compare
        merge = api[["origin_iso2", "group", "value_tonne"]].merge(
            raw, how="left", on=["origin_iso2", "group"], suffixes=("_api", "_raw")
        )

        assert np.isclose(merge.value_tonne_api, merge.value_tonne_raw, rtol=1e-6).all()


def test_kpler_v1(app):
    # Create a test client using the Flask application configured for testing
    with app.test_client() as test_client:
        params = {
            "format": "json",
            "origin_iso2": "RU,CN",
            "commodity": "Crude,Diesel",
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
            data_df.groupby(["origin_type", "destination_type", "origin_iso2", "commodity"])
            .value_tonne.sum()
            .reset_index()
        )

        assert set(grouped["commodity"].unique()) == set(["Crude", "Diesel"])
        # Russia - Crude - Dec 2022 = 18.7
        ru_crude = grouped[(grouped.origin_iso2 == "RU") & (grouped["commodity"] == "Crude")]
        assert len(ru_crude) >= 3
        assert all(round(ru_crude.value_tonne / 1e6) == 19)
        assert all(np.isclose(ru_crude.value_tonne, ru_crude.value_tonne, rtol=1e-6))

        # China - Diesel - Dec 2022 = 2.16
        cn_diesel = grouped[(grouped.origin_iso2 == "CN") & (grouped["commodity"] == "Diesel")]
        assert len(cn_diesel) >= 3
        assert all(round(cn_diesel.value_tonne / 1e6) == 2)
        assert all(np.isclose(cn_diesel.value_tonne, cn_diesel.value_tonne, rtol=1e-6))

        expected_columns = set(
            [
                "origin_iso2",
                "destination_iso2",
                "commodity_origin_iso2",
                "commodity_destination_iso2",
                "commodity",
                "grade",
                "commodity",
                "group",
                "family",
                "date",
                "value_tonne",
                "value_eur",
                "value_usd",
                "pricing_scenario",
            ]
        )
        assert set(data_df.columns) >= expected_columns


def test_kpler_v1_key(app):

    with app.test_client() as test_client:
        # Test that the join with pricing doesn't remove flows
        # To do so, we query both from the api and directly from the flow table
        # and compare the results

        iso2s = ["RU", "TR", "CN", "MY"]
        groups = ["Gasoil/Diesel", "Crude/Co"]
        params = {
            "format": "json",
            "origin_iso2": ",".join(iso2s),
            "group": ",".join(groups),
            "date_from": "2022-12-01",
            "date_to": "2022-12-31",
            "aggregate_by": "origin_iso2,group",
            "api_key": "THISISNOTACORRECTKEY",
            "currency": ["EUR", "USD"],
        }

        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 403


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
    manual_values["date"] = pd.to_datetime(manual_values["date"] + "-01")
    manual_values.rename(columns={"date": "month"}, inplace=True)

    with app.test_client() as test_client:

        params = {
            "format": "json",
            "date_from": "2022-01-01",
            "date_to": "2023-07-31",
            "origin_iso2": ",".join(["RU", "TR", "EG", "AE"]),
            "aggregate_by": "origin_iso2,group,month",
            "group": ",".join(["Crude", "Crude/Co"]),
            "api_key": get_env("API_KEY"),
        }

        params_flow = params.copy()
        params_flow["origin_type"]: "country"

        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params_flow))
        assert response.status_code == 200
        flow = pd.DataFrame(response.json["data"])
        assert len(flow) > 0  # Not all cou

        params_trade = params.copy()
        params_trade["aggregate_by"] = "origin_iso2,group,origin_month"
        response = test_client.get("/v1/kpler_trade?" + urllib.parse.urlencode(params_trade))
        assert response.status_code == 200
        trade = pd.DataFrame(response.json["data"])
        trade = trade.rename(columns={"origin_month": "month"})
        assert len(trade) > 0  # Not all cou

        # Remove timezone from both data_dt.month and manual_values.month
        flow["month"] = pd.to_datetime(flow["month"]).dt.tz_localize(None)
        trade["month"] = pd.to_datetime(trade["month"]).dt.tz_localize(None)
        manual_values["month"] = manual_values["month"].dt.tz_localize(None)
        # Merge
        merge = (
            manual_values[["origin_iso2", "month", "value_tonne"]]
            .merge(
                trade[["origin_iso2", "month", "value_tonne"]],
                how="left",
                on=["origin_iso2", "month"],
                suffixes=("_manual", "_trade"),
            )
            .merge(
                flow[["origin_iso2", "month", "value_tonne"]].rename(
                    columns={"value_tonne": "value_tonne_flow"}
                ),
                how="left",
                on=["origin_iso2", "month"],
            )
        )

        # Remove rows where both value_tonne_flow and value_tonne_trade are NaN
        merge = merge[~(merge.value_tonne_flow.isna() & merge.value_tonne_trade.isna())]

        merge["ok_flow"] = np.isclose(merge.value_tonne_flow, merge.value_tonne_manual, rtol=4e-2)
        merge["ok_trade"] = np.isclose(merge.value_tonne_trade, merge.value_tonne_manual, rtol=4e-2)
        merge["ok"] = merge["ok_flow"] & merge["ok_trade"]
        assert all(merge.ok)


def test_kpler_gasoline_export(app):
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
    manual_values["date"] = pd.to_datetime(manual_values["date"] + "-01")
    manual_values.rename(columns={"date": "month"}, inplace=True)

    with app.test_client() as test_client:

        params = {
            "format": "json",
            "date_from": "2022-01-01",
            "date_to": "2023-07-31",
            "origin_iso2": ",".join(["RU", "TR", "EG", "AE"]),
            "aggregate_by": "origin_iso2,group,month",
            "commodity": "Gasoline",
            "api_key": get_env("API_KEY"),
        }

        params_flow = params.copy()
        params_flow["origin_type"]: "country"

        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params_flow))
        assert response.status_code == 200
        flow = pd.DataFrame(response.json["data"])
        assert len(flow) > 0

        params_trade = params.copy()
        params_trade["aggregate_by"] = "origin_iso2,group,origin_month"
        response = test_client.get("/v1/kpler_trade?" + urllib.parse.urlencode(params_trade))
        assert response.status_code == 200
        trade = pd.DataFrame(response.json["data"])
        trade = trade.rename(columns={"origin_month": "month"})
        assert len(trade) > 0

    # Remove timezone from both data_dt.month and manual_values.month
    flow["month"] = pd.to_datetime(flow["month"]).dt.tz_localize(None)
    trade["month"] = pd.to_datetime(trade["month"]).dt.tz_localize(None)
    manual_values["month"] = pd.to_datetime(manual_values["month"]).dt.tz_localize(None)
    # Merge
    merge = (
        manual_values[["origin_iso2", "month", "value_tonne"]]
        .merge(
            trade[["origin_iso2", "month", "value_tonne"]],
            how="left",
            on=["origin_iso2", "month"],
            suffixes=("_manual", "_trade"),
        )
        .merge(
            flow[["origin_iso2", "month", "value_tonne"]].rename(
                columns={"value_tonne": "value_tonne_flow"}
            ),
            how="left",
            on=["origin_iso2", "month"],
        )
    )

    # Remove rows where both value_tonne_flow and value_tonne_trade are NaN
    merge = merge[~(merge.value_tonne_flow.isna() & merge.value_tonne_trade.isna())]

    merge["ok_flow"] = np.isclose(merge.value_tonne_flow, merge.value_tonne_manual, rtol=4e-2)
    merge["ok_trade"] = np.isclose(merge.value_tonne_trade, merge.value_tonne_manual, rtol=4e-2)
    merge["ok"] = merge["ok_flow"] & merge["ok_trade"]
    assert all(merge.ok)


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
            "aggregate_by": "origin_iso2,group,year",
            "commodity": "Diesel",
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

        assert all(np.isclose(merge.value_tonne_api, merge.value_tonne_manual, rtol=5e-2))


def test_kpler_flow_lng_exports_monthly(app):
    """
    Test values against manually collected ones
    :param app:
    :return:
    """
    import country_converter as coco

    cc = coco.CountryConverter()

    manual_values = pd.read_csv("assets/kpler/lng_exports_monthly_2023.csv")
    manual_values = manual_values.melt(
        id_vars=["date"], var_name="country", value_name="value_ktonne"
    )
    manual_values["value_tonne"] = manual_values["value_ktonne"] * 1e3
    manual_values["origin_iso2"] = manual_values["country"].map(lambda x: cc.convert(x, to="ISO2"))
    manual_values.rename(columns={"date": "month"}, inplace=True)

    countries = ["RU"]
    manual_values = manual_values[manual_values["origin_iso2"].isin(countries)]

    with app.test_client() as test_client:
        params = {
            "format": "json",
            "date_from": "2023-01-01",
            "date_to": "2023-07-31",
            "origin_iso2": ",".join(countries),
            "aggregate_by": "origin_iso2,group,month",
            "commodity": "lng",
            "api_key": get_env("API_KEY"),
        }

        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        flow = response.json["data"]
        flow = pd.DataFrame(flow)
        flow = flow.groupby(["origin_iso2", "month"]).value_tonne.sum().reset_index()
        # Month to YYYY-MM
        flow["month"] = flow["month"].map(lambda x: pd.to_datetime(x).strftime("%Y-%m"))
        # Merge
        merge_flow = manual_values[["origin_iso2", "month", "value_tonne"]].merge(
            flow[["origin_iso2", "month", "value_tonne"]],
            how="left",
            on=["origin_iso2", "month"],
            suffixes=("_manual", "_api"),
        )

        # Cut last month, not complete
        merge_flow = merge_flow[merge_flow.month != merge_flow.month.max()]
        merge_flow["value_tonne_api"] = merge_flow["value_tonne_api"].fillna(0)

        merge_flow_agg = merge_flow.groupby("origin_iso2").agg(
            {"value_tonne_api": "sum", "value_tonne_manual": "sum"}
        )
        assert all(
            np.isclose(
                merge_flow_agg.value_tonne_api, merge_flow_agg.value_tonne_manual, rtol=10e-2
            )
        )

        # Much more strict for Russia
        idx = merge_flow.origin_iso2 == "RU"
        assert all(
            np.isclose(
                merge_flow[idx].value_tonne_api, merge_flow[idx].value_tonne_manual, rtol=1e-2
            )
        )


def test_kpler_trade_lng_exports_monthly(app):
    """
    Test values against manually collected ones
    :param app:
    :return:
    """
    import country_converter as coco

    cc = coco.CountryConverter()

    manual_values = pd.read_csv("assets/kpler/lng_exports_monthly_2023.csv")
    manual_values = manual_values.melt(
        id_vars=["date"], var_name="country", value_name="value_ktonne"
    )
    manual_values["value_tonne"] = manual_values["value_ktonne"] * 1e3
    manual_values["origin_iso2"] = manual_values["country"].map(lambda x: cc.convert(x, to="ISO2"))
    manual_values.rename(columns={"date": "month"}, inplace=True)

    countries = ["RU", "CN", "IN", "SG", "TR"]
    manual_values = manual_values[manual_values["origin_iso2"].isin(countries)]

    with app.test_client() as test_client:
        params = {
            "format": "json",
            "date_from": "2023-01-01",
            "date_to": "2023-07-31",
            "origin_iso2": ",".join(countries),
            "aggregate_by": "origin_iso2,group,origin_month",
            "commodity": "lng",
            "api_key": get_env("API_KEY"),
        }

        response = test_client.get("/v1/kpler_trade?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        trade = response.json["data"]
        trade = pd.DataFrame(trade)
        trade = trade.groupby(["origin_iso2", "month"]).value_tonne.sum().reset_index()
        trade["month"] = trade["month"].map(lambda x: pd.to_datetime(x).strftime("%Y-%m"))
        # Merge
        merge_trade = manual_values[["origin_iso2", "month", "value_tonne"]].merge(
            trade[["origin_iso2", "month", "value_tonne"]],
            how="left",
            on=["origin_iso2", "month"],
            suffixes=("_manual", "_api"),
        )

        # Cut last month, not complete
        merge_trade = merge_trade[merge_trade.month != merge_trade.month.max()]
        merge_trade["value_tonne_api"] = merge_trade["value_tonne_api"].fillna(0)

        assert all(
            np.isclose(merge_trade.value_tonne_api, merge_trade.value_tonne_manual, rtol=10e-2)
        )


def test_kpler_trade_no_duplicates(app):
    with app.test_client() as test_client:
        params = {
            "format": "json",
            "date_from": "2023-01-01",
            "date_to": "2023-07-31",
            "origin_iso2": ",".join(["RU", "CN", "IN", "SG", "TR"]),
            "api_key": get_env("API_KEY"),
        }

        response = test_client.get("/v1/kpler_trade?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        trade = response.json["data"]
        trade = pd.DataFrame(trade)

        grouped_trades = (
            trade.groupby(["trade_id", "commodity", "grade"]).size().to_frame("size").reset_index()
        )

        assert len(grouped_trades[grouped_trades["size"] > 1]) == 0


def test_kpler_trade_pricing(app):

    with app.test_client() as test_client:

        ID__FLOWS_1__SHIPS_1 = {
            "trade_id": 18583808,
            "flows": 1,
            "commodity": ["kpler_diesel"],
            "total_value_sum": 43103.43 * 789.18,
        }

        ID__FLOWS_2__SHIPS_1 = {
            "trade_id": 17941591,
            "flows": 2,
            "commodity": ["kpler_vgo", "kpler_fo"],
            "total_value_sum": 33999.20 * 721.69 + 66000.35 * 420.29,
        }

        ID__FLOWS_1__SHIPS_2 = {
            "trade_id": 18485627,
            "flows": 1,
            "commodity": ["kpler_gasoil"],
            "total_value_sum": 19481.73 * 727.30,
        }

        ID__COM_no = {
            "trade_id": 18552571,
            "flows": 1,
            "commodity": [None],
            "total_value_sum": None,
            "expected_missing": True,
        }
        ID__INS_n__DES_n = {
            "trade_id": 13556034,
            "flows": 1,
            "commodity": ["crude_oil_urals"],
            "total_value_sum": 76790.56 * 680.79,
        }
        ID__INS_y__DES_n = {
            "trade_id": 18354609,
            "flows": 1,
            "commodity": ["crude_oil_urals"],
            "total_value_sum": 134598.24 * 393.12,
        }
        ID__INS_n__DES_y = {
            "trade_id": 18377410,
            "flows": 1,
            "commodity": ["crude_oil_urals"],
            "total_value_sum": 86115.77 * 390.407,
        }
        ID__INS_y__DES_y = {
            "trade_id": 17491334,
            "flows": 1,
            "commodity": ["crude_oil_urals"],
            "total_value_sum": 86115.77 * 393.507,
        }

        expected = pd.DataFrame.from_dict(
            [
                ID__FLOWS_1__SHIPS_1,
                ID__FLOWS_2__SHIPS_1,
                ID__FLOWS_1__SHIPS_2,
                ID__COM_no,
                ID__INS_n__DES_n,
                ID__INS_y__DES_n,
                ID__INS_n__DES_y,
                ID__INS_y__DES_y,
            ]
        )

        params = {
            "format": "json",
            "trade_ids": ",".join(str(x) for x in expected["trade_id"].tolist()),
            "api_key": get_env("API_KEY"),
        }

        response = test_client.get("/v1/kpler_trade?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)

        grouped_actual = (
            data_df.groupby("trade_id")
            .agg({"value_eur": "sum", "flow_id": "nunique", "pricing_commodity": "unique"})
            .rename(
                columns={
                    "flow_id": "flows",
                    "value_eur": "total_value_sum",
                    "pricing_commodity": "commodity",
                }
            )
            .reset_index()
        )[["trade_id", "flows", "total_value_sum", "commodity"]]

        merged = expected.merge(
            grouped_actual, on="trade_id", how="outer", suffixes=("_expected", "_actual")
        )

        assert all(np.isnan(merged[merged.expected_missing == True]["flows_actual"]))

        excluding_missing = merged[merged.expected_missing == False]

        assert all(excluding_missing["flows_expected"] == excluding_missing["flows_actual"])
        assert all(excluding_missing["commodity_expected"] == excluding_missing["commodity_actual"])
        assert all(
            np.isclose(
                excluding_missing["total_value_sum_expected"],
                excluding_missing["total_value_sum_actual"],
            )
        )


def test_kpler_crude_export_byport(app):
    """
    Test values against manually collected ones
    :param app:
    :return:
    """
    with app.test_client() as test_client:

        params = {
            "format": "json",
            "date_from": "2023-04-01",
            "date_to": "2023-04-30",
            "origin_iso2": "RU",
            "origin_type": "port",
            "aggregate_by": "origin,group,year",
            "commodity": ",".join(["Crude"]),
            "api_key": get_env("API_KEY"),
        }

        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        data = response.json["data"]
        data_df = pd.DataFrame(data)
        data_df = (
            data_df.groupby(["origin_name"], dropna=False)
            .value_tonne.sum()
            .reset_index()
            .sort_values("value_tonne", ascending=False)
        )
        # Merge

        assert np.isclose(data_df.value_tonne.sum(), 21.17e6, rtol=1e-2)


def test_kpler_wrong_arg(app):

    with app.test_client() as test_client:
        # Test that it returns an error if a wrong argument is given
        # We force this so that old queries with e.g. product argument
        # isn't anymore valid
        params = {"product": "shouldntwork"}
        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == HTTPStatus.BAD_REQUEST


def test_kpler_v1_commodity_origin(app):

    with app.test_client() as test_client:
        # Test that the join with pricing doesn't remove flows
        # To do so, we query both from the api and directly from the flow table
        # and compare the results

        iso2s = ["RU"]
        commodities = ["Crude"]  # , "Gasoline"]
        params = {
            "format": "json",
            "origin_iso2": ",".join(iso2s),
            "commodity": ",".join(commodities),
            "date_from": "2022-12-01",
            "date_to": "2022-12-31",
            "aggregate_by": "origin_iso2,commodity",
            "api_key": get_env("API_KEY"),
        }
        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        api = pd.DataFrame(response.json["data"])
        assert len(api) > 0  # Not all cou


def test_kpler_south_korea(app):

    with app.test_client() as test_client:
        params = {
            "format": "json",
            "origin_iso2": "RU",
            "destination_iso2": "KR",
            "group": "Coal",
            "date_from": "2022-12-01",
            "date_to": "2022-12-31",
            "aggregate_by": "commodity_origin_iso2,destination_iso2,commodity",
            "api_key": get_env("API_KEY"),
        }
        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        api = pd.DataFrame(response.json["data"])
        assert len(api) > 0  # Not all cou


def test_kpler_urals_espo(app):

    with app.test_client() as test_client:
        params = {
            "format": "json",
            "origin_iso2": "RU",
            "commodity_equivalent": "crude_oil",
            "date_from": "2023-01-01",
            "date_to": "2023-05-30",
            "origin_type": "country,port",
            "api_key": get_env("API_KEY"),
        }
        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        api = pd.DataFrame(response.json["data"])
        assert len(api) > 0  # Not all cou

        # Assert value_tonne is equal across origin_type but not value_eur
        api_by_type = (
            api.groupby(["origin_type"])
            .agg({"value_tonne": "sum", "value_eur": "sum"})
            .reset_index()
        )
        assert all(np.isclose(api_by_type.value_tonne, api_by_type.value_tonne[0], rtol=5e-2))
        assert not all(np.isclose(api_by_type.value_eur, api_by_type.value_eur[0], rtol=1e-3))
        assert set(api.pricing_commodity.unique()) >= set(["crude_oil_espo", "crude_oil_urals"])
        assert all(api.groupby("pricing_commodity").value_tonne.sum() > 0)
        assert all(api.groupby("pricing_commodity").value_eur.sum() > 0)

        # There shouldn't be any crude_oil that is not crude_oil_espo or crude_oil_urals
        assert not any(
            (api.pricing_commodity == "crude_oil")
            & (api.origin_type == "port")
            & (api.commodity_origin_iso2 == "RU")
        )


def test_flow_equals_trade(app):

    with app.test_client() as test_client:

        date_from = "2023-01-01"
        date_to = "2023-01-31"

        params = {
            "format": "json",
            "origin_iso2": "RU",
            "commodity_equivalent": "crude_oil",
            "commodity": "Crude",
            "date_from": date_from,
            "date_to": date_to,
            "api_key": get_env("API_KEY"),
        }

        params_flow = params.copy()
        params_flow["origin_type"]: "country"

        response = test_client.get("/v1/kpler_flow?" + urllib.parse.urlencode(params_flow))
        assert response.status_code == 200
        flow = pd.DataFrame(response.json["data"])
        assert len(flow) > 0  # Not all cou

        params_trade = params.copy()
        response = test_client.get("/v1/kpler_trade?" + urllib.parse.urlencode(params_trade))
        assert response.status_code == 200
        trade = pd.DataFrame(response.json["data"])
        assert len(trade) > 0  # Not all cou

        assert np.isclose(flow.value_tonne.sum(), trade.value_tonne.sum(), rtol=1e-3)
        assert np.isclose(flow.value_eur.sum(), trade.value_eur.sum(), rtol=10e-2)

    return


def test_kpler_trade_commodity_origin(app):
    """
    Test that CPC is correctly aggregated to KZ
    :param app:
    :return:
    """
    with app.test_client() as test_client:
        date_from = "2023-01-01"
        date_to = "2023-01-31"

        params = {
            "format": "json",
            "origin_iso2": "RU",
            "commodity_equivalent": "crude_oil",
            "commodity": "Crude",
            "date_from": date_from,
            "date_to": date_to,
            "api_key": get_env("API_KEY"),
        }

        response = test_client.get("/v1/kpler_trade?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        trades = pd.DataFrame(response.json["data"])
        assert len(trades) > 0  # Not all cou

        assert set(trades.commodity_origin_iso2.unique()) == set(["RU", "KZ"])
        assert set(trades[trades.commodity_origin_iso2 == "KZ"].grade.unique()) == set(
            ["CPC Kazakhstan", "KEBCO"]
        )
        assert not any(
            (trades.commodity_origin_iso2 == "RU")
            & (trades.grade.isin(["CPC Kazakhstan", "KEBCO"]))
        )

    return


def test_kpler_trade_ship_insurer(app):
    with app.test_client() as test_client:
        date_from = "2023-01-01"
        date_to = "2023-01-31"

        # Confirmed all ships of these against the original P&I providers.
        SINGLE_SHIP_UNKNOWN_INSURER = {
            "trade_id": 3108824,
            "ship_insurer_names": ["unknown"],
            "ship_insurer_iso2s": [None],
            "ship_insurer_regions": [None],
        }
        SINGLE_SHIP_WITH_INSURER = {
            "trade_id": 794454,
            "ship_insurer_names": ["North of England P&I Association"],
            "ship_insurer_iso2s": ["GB"],
            "ship_insurer_regions": ["Global"],
        }
        MULTI_SHIP_ONE_INSURER = {
            "trade_id": 17145711,
            "ship_insurer_names": ["Assuranceforeningen Gard - Norway"],
            "ship_insurer_iso2s": ["NO"],
            "ship_insurer_regions": ["Global"],
        }
        MULTI_SHIP_MULTIPLE_INSURERS = {
            "trade_id": 17069592,
            "ship_insurer_names": [
                "Britannia Steamship insurance Association Ld",
                "UK P&I Club",
            ],
            "ship_insurer_iso2s": ["GB"],
            "ship_insurer_regions": [
                "Global",
                "United Kingdom",
            ],
        }

        expected = pd.DataFrame.from_dict(
            [
                SINGLE_SHIP_UNKNOWN_INSURER,
                SINGLE_SHIP_WITH_INSURER,
                MULTI_SHIP_ONE_INSURER,
                MULTI_SHIP_MULTIPLE_INSURERS,
            ]
        )

        params = {
            "format": "json",
            "trade_ids": ",".join(str(x) for x in expected["trade_id"].tolist()),
            "api_key": get_env("API_KEY"),
        }

        response = test_client.get("/v1/kpler_trade?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        actual = pd.DataFrame(response.json["data"])
        assert len(actual) > 0

        merged = expected.merge(
            actual, on="trade_id", how="left", suffixes=("_expected", "_actual")
        )

        for index, row in merged.iterrows():
            assert np.array_equiv(
                row["ship_insurer_names_expected"], row["ship_insurer_names_actual"]
            )
            assert np.array_equiv(
                row["ship_insurer_iso2s_expected"], row["ship_insurer_iso2s_actual"]
            )
            assert np.array_equiv(
                row["ship_insurer_regions_expected"], row["ship_insurer_regions_actual"]
            )

    return


def test_kpler_trade_ship_owner(app):
    with app.test_client() as test_client:
        date_from = "2023-01-01"
        date_to = "2023-01-31"

        # Confirmed in equasis
        SINGLE_SHIP_WITH_OWNER = {
            "trade_id": 804124,
            "ship_owner_names": ["CORAL ENERGY SHIPPING BV"],
            "ship_owner_iso2s": ["NO"],
            "ship_owner_regions": ["Global"],
        }
        MULTI_SHIP_ONE_OWNER = {
            "trade_id": 16468265,
            "ship_owner_names": ["ARAB MARITIME PETROLEUM TRANS"],
            "ship_owner_iso2s": ["KW"],
            "ship_owner_regions": ["Global", "Others"],
        }
        MULTI_SHIP_MULTIPLE_OWNERS = {
            "trade_id": 794079,
            "ship_owner_names": [
                "HAI FENG 1716 LTD",
                "HAI KUO SHIPPING 1605 LTD",
            ],
            "ship_owner_iso2s": ["GR", "NO"],
            "ship_owner_regions": [
                "EU28",
                "Global",
            ],
        }

        expected = pd.DataFrame.from_dict(
            [
                SINGLE_SHIP_WITH_OWNER,
                MULTI_SHIP_ONE_OWNER,
                MULTI_SHIP_MULTIPLE_OWNERS,
            ]
        )

        params = {
            "format": "json",
            "trade_ids": ",".join(str(x) for x in expected["trade_id"].tolist()),
            "api_key": get_env("API_KEY"),
        }

        response = test_client.get("/v1/kpler_trade?" + urllib.parse.urlencode(params))
        assert response.status_code == 200
        actual = pd.DataFrame(response.json["data"])
        assert len(actual) > 0

        merged = expected.merge(
            actual, on="trade_id", how="left", suffixes=("_expected", "_actual")
        )

        for index, row in merged.iterrows():
            assert np.array_equiv(row["ship_owner_names_expected"], row["ship_owner_names_actual"])
            assert np.array_equiv(row["ship_owner_iso2s_expected"], row["ship_owner_iso2s_actual"])
            assert np.array_equiv(
                row["ship_owner_regions_expected"], row["ship_owner_regions_actual"]
            )

    return
