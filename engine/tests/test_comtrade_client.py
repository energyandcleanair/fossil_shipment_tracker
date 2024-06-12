from numpy import NaN
import pandas as pd
import pytest
from engines.comtrade_client.comtrade import (
    ComtradeClient,
    ComtradeCommodities,
)

from pandas.testing import assert_frame_equal

from datetime import datetime, date

import comtradeapicall

availability__basic_response = pd.DataFrame(
    columns=["reporterISO", "period", "lastReleased"], data=[["USA", "202101", "2021-01-01"]]
)

availability__no_results = pd.DataFrame(columns=["reporterISO", "period", "lastReleased"], data=[])

availability__multiple_repeated_results = pd.DataFrame(
    columns=["reporterISO", "period", "lastReleased", "extra"],
    data=[
        ["USA", "202101", "2021-01-01", "extra"],
        ["USA", "202101", "2021-01-01", "extra"],
    ],
)

availability__multiple_countries_some_empty = pd.DataFrame(
    columns=["reporterISO", "period", "lastReleased", "extra"],
    data=[
        ["USA", "202101", "2021-01-01", "extra"],
        ["USA", "202102", "2021-02-01", "extra"],
        ["USA", "202103", "2021-03-01", "extra"],
        ["NZL", "202103", "2021-03-01", "extra"],
    ],
)

availability__r4_and_eur_countries = pd.DataFrame(
    columns=["reporterISO", "period", "lastReleased", "extra"],
    data=[
        ["USA", "202101", "2021-01-01", "extra"],
        ["USA", "202102", "2021-02-01", "extra"],
        ["USA", "202103", "2021-03-01", "extra"],
        ["NZL", "202101", "2021-01-01", "extra"],
        ["NZL", "202102", "2021-02-01", "extra"],
        ["NZL", "202103", "2021-03-01", "extra"],
        ["R4", "202101", "2021-01-01", "extra"],
        ["R4", "202102", "2021-02-01", "extra"],
        ["R4", "202103", "2021-03-01", "extra"],
        ["EUR", "202101", "2021-01-01", "extra"],
        ["EUR", "202102", "2021-02-01", "extra"],
        ["EUR", "202103", "2021-03-01", "extra"],
    ],
)

data__no_results = pd.DataFrame(columns=[], data=[])

data__basic_response = pd.DataFrame(
    data=[
        {
            "period": "202101",
            "reporterISO": "USA",
            "partnerISO": "NZL",
            "cmdCode": "2701",
            "flowDesc": "Import",
            "netWgt": 1000,
            "isNetWgtEstimated": False,
            "qty": 900,
            "qtyUnitAbbr": "kg",  # no longer used
            "primaryValue": 1000,  # no longer used
            "extra": "extra",
        },
        {
            "period": "202101",
            "reporterISO": "USA",
            "partnerISO": "NZL",
            "cmdCode": "2701",
            "flowDesc": "Export",
            "netWgt": 1000,
            "isNetWgtEstimated": True,
            "qty": 900,  # no longer used
            "qtyUnitAbbr": "kg",  # no longer used
            "primaryValue": 1000,
            "extra": "extra",
        },
    ]
)

data__basic_with_not_found_isos = pd.DataFrame(
    data=[
        {
            "period": "202101",
            "reporterISO": "USA",
            "partnerISO": "R4",
            "cmdCode": "2701",
            "flowDesc": "Import",
            "netWgt": 1000,
            "isNetWgtEstimated": False,
            "qty": 900,  # no longer used
            "qtyUnitAbbr": "kg",  # no longer used
            "primaryValue": 1000,
            "extra": "extra",
        },
        {
            "period": "202101",
            "reporterISO": "USA",
            "partnerISO": "W00",
            "cmdCode": "2701",
            "flowDesc": "Export",
            "netWgt": 1000,
            "isNetWgtEstimated": False,
            "qty": 900,  # no longer used
            "qtyUnitAbbr": "kg",  # no longer used
            "primaryValue": 1000,
            "extra": "extra",
        },
        {
            "period": "202101",
            "reporterISO": "USA",
            "partnerISO": "S19",
            "cmdCode": "2701",
            "flowDesc": "Export",
            "netWgt": 1000,
            "isNetWgtEstimated": False,
            "qty": 900,  # no longer used
            "qtyUnitAbbr": "kg",  # no longer used
            "primaryValue": 1000,
            "extra": "extra",
        },
        {
            "period": "202101",
            "reporterISO": "USA",
            "partnerISO": "EUR",
            "cmdCode": "2701",
            "flowDesc": "Export",
            "netWgt": 1000,
            "isNetWgtEstimated": False,
            "qty": 900,  # no longer used
            "qtyUnitAbbr": "kg",  # no longer used
            "primaryValue": 1000,
            "extra": "extra",
        },
    ]
)

data__reporters_repsonse = pd.DataFrame(
    columns=["reporterCodeIsoAlpha2", "extra"],
    data=[
        ["US", "extra"],
        ["NZ", "extra"],
    ],
)

data__reporters_response_with_not_found = pd.DataFrame(
    columns=["reporterCodeIsoAlpha2", "extra"],
    data=[
        ["US", "extra"],
        ["NZ", "extra"],
        ["EU", "extra"],
        ["AN", "extra"],
    ],
)


default_periods = pd.date_range("2021-01-01", "2021-03-31", freq="M").to_period()


def test_ComtradeClient_get_data_availability__called_with_correct_arguments(mocker):
    mocked_getFinalDataAvailability = mocker.patch(
        "engines.comtrade_client.comtrade.getFinalDataAvailability"
    )
    mocked_getFinalDataAvailability.return_value = availability__basic_response

    client = ComtradeClient(api_key="api_key")

    expected_month_args = "202101,202102,202103"

    client.get_data_availability(periods=default_periods)

    # Check mock arguments
    _, kwargs = mocked_getFinalDataAvailability.call_args

    assert kwargs.get("subscription_key") == "api_key"
    assert kwargs.get("typeCode") == "C"
    assert kwargs.get("freqCode") == "M"
    assert kwargs.get("clCode") == "HS"
    assert kwargs.get("reporterCode") is None
    assert kwargs.get("period") == expected_month_args


def test_ComtradeClient_get_data_availability__no_results__assertion_error(mocker):
    mocked_getFinalDataAvailability = mocker.patch(
        "engines.comtrade_client.comtrade.getFinalDataAvailability"
    )
    mocked_getFinalDataAvailability.return_value = availability__no_results

    client = ComtradeClient(api_key="api_key")

    with pytest.raises(AssertionError):
        client.get_data_availability(periods=default_periods)


def test_ComtradeClient_get_data_availability__multiple_repeated_results__assertion_error(mocker):
    mocked_getFinalDataAvailability = mocker.patch(
        "engines.comtrade_client.comtrade.getFinalDataAvailability"
    )
    mocked_getFinalDataAvailability.return_value = availability__multiple_repeated_results

    client = ComtradeClient(api_key="api_key")

    with pytest.raises(AssertionError):
        client.get_data_availability(periods=default_periods)


def test_ComtradeClient_get_data_availability__multiple_countries_some_empty__correct_response(
    mocker,
):
    mocked_getFinalDataAvailability = mocker.patch(
        "engines.comtrade_client.comtrade.getFinalDataAvailability"
    )
    mocked_getFinalDataAvailability.return_value = availability__multiple_countries_some_empty

    client = ComtradeClient(api_key="api_key")

    result = client.get_data_availability(periods=default_periods)

    expected = pd.DataFrame(
        columns=["reporter_iso2", "period", "last_released"],
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
    mocked_getFinalDataAvailability = mocker.patch(
        "engines.comtrade_client.comtrade.getFinalDataAvailability"
    )
    mocked_getFinalDataAvailability.return_value = availability__r4_and_eur_countries

    client = ComtradeClient(api_key="api_key")

    result = client.get_data_availability(periods=default_periods)

    expected = pd.DataFrame(
        columns=["reporter_iso2", "period", "last_released"],
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


def test_ComtradeClient_get_monthly_trades_for_periods__called_with_correct_arguments(mocker):
    mocked_getFinalData = mocker.patch("engines.comtrade_client.comtrade.getFinalData")
    mocked_getFinalData.return_value = availability__basic_response

    client = ComtradeClient(api_key="api_key")

    client._get_monthly_imports_for_period_subset(
        reporter="US",
        periods=[to_month("2021-01"), to_month("2021-02")],
        commodities=[ComtradeCommodities.COAL],
    )

    # Check mock arguments
    _, kwargs = mocked_getFinalData.call_args
    assert kwargs.get("subscription_key") == "api_key"
    assert kwargs.get("typeCode") == "C"
    assert kwargs.get("freqCode") == "M"
    assert kwargs.get("clCode") == "HS"
    assert kwargs.get("flowCode") == "M,X"
    assert kwargs.get("period") == "202101,202102"
    assert kwargs.get("reporterCode") == comtradeapicall.convertCountryIso3ToCode("USA")
    assert kwargs.get("cmdCode") == "2701"
    assert kwargs.get("partnerCode") is None
    assert kwargs.get("partner2Code") is None
    assert kwargs.get("customsCode") == "C00"
    assert kwargs.get("motCode") == "0"
    assert kwargs.get("includeDesc") == True


def test_ComtradeClient_get_monthly_trades_for_periods__no_results__empty_results_returned(mocker):
    mocked_getFinalData = mocker.patch("engines.comtrade_client.comtrade.getFinalData")
    mocked_getFinalData.return_value = availability__no_results

    client = ComtradeClient(api_key="api_key")

    result = client.get_monthly_trades_for_periods(
        reporter="US",
        periods=[to_month("2021-01"), to_month("2021-02")],
        commodities=[ComtradeCommodities.COAL],
    )

    expected = pd.DataFrame(
        columns=[
            "reporter_iso2",
            "partner_iso2",
            "commodity_code",
            "flow_direction",
            "period",
            "value_kg",
            "value_kg_estimated",
            "value_usd",
        ],
        data=[],
    )

    assert_frame_equal(result, expected)


def test_ComtradeClient_get_monthly_trades_for_periods__basic_response__correct_response(mocker):
    mocked_getFinalData = mocker.patch("engines.comtrade_client.comtrade.getFinalData")
    mocked_getFinalData.return_value = data__basic_response

    client = ComtradeClient(api_key="api_key")

    result = client.get_monthly_trades_for_periods(
        reporter="US",
        periods=[to_month("2021-01")],
        commodities=[ComtradeCommodities.COAL],
    )

    expected = pd.DataFrame(
        data=[
            {
                "reporter_iso2": "US",
                "partner_iso2": "NZ",
                "commodity_code": "2701",
                "flow_direction": "Import",
                "period": to_month("2021-01"),
                "value_kg": 1000,
                "value_kg_estimated": False,
                "value_usd": 1000,
            },
            {
                "reporter_iso2": "US",
                "partner_iso2": "NZ",
                "commodity_code": "2701",
                "flow_direction": "Export",
                "period": to_month("2021-01"),
                "value_kg": 1000,
                "value_kg_estimated": True,
                "value_usd": 1000,
            },
        ]
    )

    assert_frame_equal(result, expected)


def test_ComtradeClient_get_monthly_trades_for_periods__called_with_more_than_12_months__requests_twice(
    mocker,
):
    mocked_getFinalData = mocker.patch("engines.comtrade_client.comtrade.getFinalData")
    mocked_getFinalData.return_value = data__basic_response

    client = ComtradeClient(api_key="api_key")

    periods = pd.period_range(start="2021-01", end="2022-12", freq="M")

    client.get_monthly_trades_for_periods(
        reporter="US",
        periods=periods,
        commodities=[ComtradeCommodities.COAL],
    )

    assert mocked_getFinalData.call_count == 2


def test_ComtradeClient_get_monthly_trades_for_periods__not_found_iso2s__excluded_from_response(
    mocker,
):
    mocked_getFinalData = mocker.patch("engines.comtrade_client.comtrade.getFinalData")
    mocked_getFinalData.return_value = data__basic_with_not_found_isos

    client = ComtradeClient(api_key="api_key")

    result = client.get_monthly_trades_for_periods(
        reporter="US",
        periods=[to_month("2021-01")],
        commodities=[ComtradeCommodities.COAL],
    )

    assert result.empty


def test_ComtradeClient_get_all_reporters__called_with_correct_arguments(mocker):
    mocked_getReference = mocker.patch("engines.comtrade_client.comtrade.getReference")
    mocked_getReference.return_value = data__reporters_repsonse

    client = ComtradeClient(api_key="api_key")

    result = client.get_all_reporters()

    mocked_getReference.assert_called_once_with(category="reporter")


def test_ComtradeClient_get_all_reporters__basic_response__correct_response(mocker):
    mocked_getReference = mocker.patch("engines.comtrade_client.comtrade.getReference")
    mocked_getReference.return_value = data__reporters_repsonse

    client = ComtradeClient(api_key="api_key")

    result = client.get_all_reporters()

    expected = pd.DataFrame(
        columns=["reporter_iso2"],
        data=[
            ["US"],
            ["NZ"],
        ],
    )

    assert_frame_equal(result, expected)


def test_ComtradeClient_get_data_availability__with_not_found__not_found_excluded(mocker):
    mocked_getReference = mocker.patch("engines.comtrade_client.comtrade.getReference")
    mocked_getReference.return_value = data__reporters_response_with_not_found

    client = ComtradeClient(api_key="api_key")

    result = client.get_all_reporters()

    expected = pd.DataFrame(
        columns=["reporter_iso2"],
        data=[
            ["US"],
            ["NZ"],
        ],
    )

    assert_frame_equal(result, expected)


def to_month(date: str) -> pd.Period:
    return pd.Period(date, freq="M")


def to_date(date: str) -> date:
    return datetime.fromisoformat(date).date()
