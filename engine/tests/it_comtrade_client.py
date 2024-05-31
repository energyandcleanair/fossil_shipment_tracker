from base.env import get_env
import pandas as pd
import pytest

from engines.comtrade_client.comtrade import ComtradeClient, ComtradeCommodities

from datetime import date


@pytest.mark.integration
def test_ComtradeClient_get_data_availability():
    client = ComtradeClient(api_key=get_env("COMTRADE_API_KEY"))

    periods = pd.date_range("2021-01-01", "2021-03-31", freq="M").to_period()

    availability = client.get_data_availability(periods=periods)

    assert not availability.empty
    assert availability["period"].min() == pd.Period("2021-01", freq="M")
    assert availability["period"].max() == pd.Period("2021-03", freq="M")
    assert ["reporter_iso2", "period", "last_released"] == availability.columns.tolist()
    # Assert reporterISO character lengths are 2
    assert all(availability["reporter_iso2"].str.len() == 2)
    # Assert that period type is pd.Period
    assert all(availability["period"].apply(type) == pd.Period)
    # Assert that lastReleased is Date or None
    assert all(availability["last_released"].apply(lambda x: x is None or isinstance(x, date)))


@pytest.mark.integration
def test_ComtradeClient_get_data():
    client = ComtradeClient(api_key=get_env("COMTRADE_API_KEY"))

    periods = pd.date_range("2021-01-01", "2021-12-31", freq="M").to_period()

    data = client.get_monthly_trades_for_periods(
        reporter="US",
        periods=periods.tolist(),
        commodities=[e for e in ComtradeCommodities],
    )

    assert not data.empty, "Expected data to be returned"
    assert set(periods.strftime("%Y-%m").tolist()) == set(
        data["period"].unique().strftime("%Y-%m")
    ), "Expected all of the periods in the response"
    assert set(data["reporter_iso2"].unique()) == set(["US"]), "Expect only US to be the reporter"
    assert set(data["commodity_code"].unique()) == set(
        [e.value for e in ComtradeCommodities]
    ), "Expect all commodities to be present"
    assert set(data["flow_direction"]) == set(
        ["Import", "Export"]
    ), "Expect both import and export flows"


@pytest.mark.integration
def test_ComtradeClient_get_data__next_month():
    client = ComtradeClient(api_key=get_env("COMTRADE_API_KEY"))

    next_month = pd.Period(date.today().replace(day=1) + pd.offsets.MonthBegin(1), freq="M")

    data = client.get_monthly_trades_for_periods(
        reporter="US",
        periods=[next_month],
        commodities=[e for e in ComtradeCommodities],
    )

    assert data.empty, "Expected no data to be returned for the next month"
    assert set(data.columns) == set(
        [
            "reporter_iso2",
            "partner_iso2",
            "commodity_code",
            "flow_direction",
            "period",
            "quantity",
            "quantity_unit",
            "value_usd",
        ]
    ), "Expected the columns to be present"
