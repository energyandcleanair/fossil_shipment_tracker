from numpy import NaN
import pandas as pd
import pytest
from engines.comtrade import (
    ComtradeClient,
)

from pandas.testing import assert_frame_equal

from datetime import datetime, date

basic_response = pd.DataFrame(
    columns=["reporterISO", "period", "lastReleased"], data=[["USA", "202101", "2021-01-01"]]
)

no_results = pd.DataFrame(columns=["reporterISO", "period", "lastReleased"], data=[])

multiple_repeated_results = pd.DataFrame(
    columns=["reporterISO", "period", "lastReleased"],
    data=[
        ["USA", "202101", "2021-01-01"],
        ["USA", "202101", "2021-01-01"],
    ],
)

multiple_countries_some_empty = pd.DataFrame(
    columns=["reporterISO", "period", "lastReleased"],
    data=[
        ["USA", "202101", "2021-01-01"],
        ["USA", "202102", "2021-02-01"],
        ["USA", "202103", "2021-03-01"],
        ["NZL", "202103", "2021-03-01"],
    ],
)

r4_and_eur_countries_returned = pd.DataFrame(
    columns=["reporterISO", "period", "lastReleased"],
    data=[
        ["USA", "202101", "2021-01-01"],
        ["USA", "202102", "2021-02-01"],
        ["USA", "202103", "2021-03-01"],
        ["NZL", "202101", "2021-01-01"],
        ["NZL", "202102", "2021-02-01"],
        ["NZL", "202103", "2021-03-01"],
        ["R4", "202101", "2021-01-01"],
        ["R4", "202102", "2021-02-01"],
        ["R4", "202103", "2021-03-01"],
        ["EUR", "202101", "2021-01-01"],
        ["EUR", "202102", "2021-02-01"],
        ["EUR", "202103", "2021-03-01"],
    ],
)


def test_ComtradeClient_get_data_availability__called_with_correct_arguments(mocker):
    mocked_getFinalDataAvailability = mocker.patch("engines.comtrade.getFinalDataAvailability")
    mocked_getFinalDataAvailability.return_value = basic_response

    client = ComtradeClient(api_key="api_key")

    expected_month_args = "202101,202102,202103"

    client.get_data_availability(start="2021-01-01", end="2021-03-31")

    # Check mock arguments
    _, kwargs = mocked_getFinalDataAvailability.call_args

    assert kwargs.get("subscription_key") == "api_key"
    assert kwargs.get("typeCode") == "C"
    assert kwargs.get("freqCode") == "M"
    assert kwargs.get("clCode") == "HS"
    assert kwargs.get("reporterCode") is None
    assert kwargs.get("period") == expected_month_args


def test_ComtradeClient_get_data_availability__no_results__assertion_error(mocker):
    mocked_getFinalDataAvailability = mocker.patch("engines.comtrade.getFinalDataAvailability")
    mocked_getFinalDataAvailability.return_value = no_results

    client = ComtradeClient(api_key="api_key")

    with pytest.raises(AssertionError):
        client.get_data_availability(start="2021-01-01", end="2021-03-31")


def test_ComtradeClient_get_data_availability__multiple_repeated_results__assertion_error(mocker):
    mocked_getFinalDataAvailability = mocker.patch("engines.comtrade.getFinalDataAvailability")
    mocked_getFinalDataAvailability.return_value = multiple_repeated_results

    client = ComtradeClient(api_key="api_key")

    with pytest.raises(AssertionError):
        client.get_data_availability(start="2021-01-01", end="2021-03-31")


def test_ComtradeClient_get_data_availability__multiple_countries_some_empty__correct_response(
    mocker,
):
    mocked_getFinalDataAvailability = mocker.patch("engines.comtrade.getFinalDataAvailability")
    mocked_getFinalDataAvailability.return_value = multiple_countries_some_empty

    client = ComtradeClient(api_key="api_key")

    result = client.get_data_availability(start="2021-01-01", end="2021-03-31")

    expected = pd.DataFrame(
        columns=["reporterISO", "period", "lastReleased"],
        data=[
            ["US", to_month("2021-01-01"), to_date("2021-01-01")],
            ["US", to_month("2021-02-01"), to_date("2021-02-01")],
            ["US", to_month("2021-03-01"), to_date("2021-03-01")],
            ["NZ", to_month("2021-01-01"), None],
            ["NZ", to_month("2021-02-01"), None],
            ["NZ", to_month("2021-03-01"), to_date("2021-03-01")],
        ],
    )

    assert_frame_equal(result, expected)


def test_ComtradeClient_get_data_availability__r4_and_eur_countries_returned__r4_and_eur_excluded(
    mocker,
):
    mocked_getFinalDataAvailability = mocker.patch("engines.comtrade.getFinalDataAvailability")
    mocked_getFinalDataAvailability.return_value = r4_and_eur_countries_returned

    client = ComtradeClient(api_key="api_key")

    result = client.get_data_availability(start="2021-01-01", end="2021-03-31")

    expected = pd.DataFrame(
        columns=["reporterISO", "period", "lastReleased"],
        data=[
            ["US", to_month("2021-01-01"), to_date("2021-01-01")],
            ["US", to_month("2021-02-01"), to_date("2021-02-01")],
            ["US", to_month("2021-03-01"), to_date("2021-03-01")],
            ["NZ", to_month("2021-01-01"), to_date("2021-01-01")],
            ["NZ", to_month("2021-02-01"), to_date("2021-02-01")],
            ["NZ", to_month("2021-03-01"), to_date("2021-03-01")],
        ],
    )

    assert_frame_equal(result, expected)


def to_month(date: str) -> pd.Period:
    return pd.Period(date, freq="M")


def to_date(date: str) -> date:
    return datetime.fromisoformat(date).date()
