from base.env import get_env
import pandas as pd
import pytest

from engines.comtrade_client.comtrade import (
    ComtradeClient,
    ComtradeCommodities,
    ComtradeRateLimitReached,
)

from datetime import date


@pytest.fixture(scope="module")
def api_key():
    key = get_env("COMTRADE_API_KEY")
    assert key is not None, "COMTRADE_API_KEY must be set in the environment variables"
    return key


def test_ComtradeClient_get_data_availability(api_key):
    client = ComtradeClient(api_key=api_key)

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


def test_ComtradeClient_get_data(api_key):
    client = ComtradeClient(api_key=api_key)

    periods = pd.date_range("2021-01-01", "2021-12-31", freq="M").to_period()

    data = client.get_monthly_trades_for_periods(
        reporter="US",
        periods=periods.tolist(),
        commodities=[e for e in ComtradeCommodities],
    )

    assert not data.empty, "Expected data to be returned"
    expected_periods = set(periods.strftime("%Y-%m").tolist())
    actual_periods = set(data["period"].unique().strftime("%Y-%m"))
    assert expected_periods == actual_periods, "Expected all of the periods in the response"
    assert set(data["reporter_iso2"].unique()) == set(["US"]), "Expect only US to be the reporter"
    assert set(data["commodity_code"].unique()) == set(
        [e.value for e in ComtradeCommodities]
    ), "Expect all commodities to be present"
    assert set(data["flow_direction"]) == set(
        ["Import", "Export"]
    ), "Expect both import and export flows"
    assert "value_kg" in data.columns
    assert "value_kg_estimated" in data.columns
    assert "value_usd" in data.columns


def test_ComtradeClient_get_data__next_month(api_key):
    client = ComtradeClient(api_key=api_key)

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
            "value_kg",
            "value_kg_estimated",
            "value_usd",
        ]
    ), "Expected the columns to be present"


def test_ComtradeClient_get_all_reporters__gets_all_reporters():
    client = ComtradeClient(api_key=get_env("COMTRADE_API_KEY"))

    reporters = client.get_all_reporters()

    assert reporters is not None
    assert len(reporters) > 0
    assert set(reporters.columns) == set(["reporter_iso2"])
    assert all(reporters["reporter_iso2"].str.len() == 2)
    assert "IN" in reporters["reporter_iso2"].values
