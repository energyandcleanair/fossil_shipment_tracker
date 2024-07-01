from .mock_db_module import *

import pandas as pd
from pandas.testing import assert_frame_equal

import datetime as dt
import pytest

from engines import comtrade

from base.models.comtrade import ComtradeHsTradeRecord, ComtradeSyncHistory

availability__response__multiple_countries_one_month = lambda **_: pd.DataFrame(
    [
        {
            "reporter_iso2": "US",
            "period": pd.Period("2021-01"),
            "last_released": dt.date(2021, 1, 31),
        }
    ]
)

availability__response__multiple_countries_one_month = lambda **_: pd.DataFrame(
    [
        {
            "reporter_iso2": "US",
            "period": pd.Period("2021-01"),
            "last_released": dt.date(2021, 1, 31),
        },
        {
            "reporter_iso2": "CA",
            "period": pd.Period("2021-01"),
            "last_released": dt.date(2021, 1, 31),
        },
    ]
)

availability__response__us_two_months = lambda **_: pd.DataFrame(
    [
        {
            "reporter_iso2": "US",
            "period": pd.Period("2021-01"),
            "last_released": dt.date(2021, 1, 31),
        },
        {
            "reporter_iso2": "US",
            "period": pd.Period("2021-02"),
            "last_released": dt.date(2021, 2, 28),
        },
    ]
)

availability__response__us_months_with_gap = lambda **_: pd.DataFrame(
    [
        {
            "reporter_iso2": "US",
            "period": pd.Period("2021-01"),
            "last_released": dt.date(2021, 3, 31),
        },
        {
            "reporter_iso2": "US",
            "period": pd.Period("2021-02"),
            "last_released": dt.date(2021, 1, 1),
        },
        {
            "reporter_iso2": "US",
            "period": pd.Period("2021-03"),
            "last_released": dt.date(2021, 3, 31),
        },
    ]
)


def trade__responses__row_per_request_combination(
    reporter: str,
    periods: list[pd.Period],
    commodities: list[comtrade.ComtradeCommodities],
):
    return pd.DataFrame(
        [
            {
                "reporter_iso2": reporter,
                "partner_iso2": "CA",
                "commodity_code": commodity.value,
                "flow_direction": "Imports",
                "period": period,
                "quantity": 100,
                "quantity_unit": "kg",
                "value_usd": 100,
            }
            for period in periods
            for commodity in commodities
        ]
    )


history__no_history = lambda *_a, **_b: pd.DataFrame()

history__us_one_month_updated_recently = lambda *_a, **_b: pd.DataFrame(
    [
        {
            "reporter_iso2": "US",
            "period": dt.date(2021, 1, 1),
            "commodity_code": "2711",
            "last_updated": dt.datetime(2023, 1, 1),
        }
    ]
)
history__us_one_month_updated_earlier = lambda *_a, **_b: pd.DataFrame(
    [
        {
            "reporter_iso2": "US",
            "period": dt.date(2021, 1, 1),
            "commodity_code": "2711",
            "last_updated": dt.datetime(2021, 1, 1),
        }
    ]
)

history__middle_month_updated = lambda *_a, **_b: pd.DataFrame(
    [
        {
            "reporter_iso2": "US",
            "period": dt.date(2021, 2, 1),
            "commodity_code": "2711",
            "last_updated": dt.datetime(2021, 3, 1),
        }
    ]
)


def test_create_sync_definition__single_month_commodity_country__single_entry():
    sync_definition = comtrade.create_sync_definitions(
        reporter_iso2s=["US"],
        commodities=[comtrade.ComtradeCommodities.NATURAL_GAS],
        start=dt.date(2021, 1, 1),
        end=dt.date(2021, 1, 31),
    )

    assert_frame_equal(
        sync_definition,
        pd.DataFrame(
            [
                {
                    "reporter_iso2": "US",
                    "period": pd.Period("2021-01"),
                    "commodity_code": "2711",
                }
            ]
        ),
    )


def test_create_sync_definition__multiple_month_multiple_commodities__multiple_entries():
    # Act
    actual_sync_definition = comtrade.create_sync_definitions(
        reporter_iso2s=["US", "CA"],
        commodities=[
            comtrade.ComtradeCommodities.NATURAL_GAS,
            comtrade.ComtradeCommodities.COAL,
        ],
        start=dt.date(2021, 1, 1),
        end=dt.date(2021, 2, 28),
    )

    # Assert
    def ignoring_order(df):
        return df.sort_values(by=["reporter_iso2", "period", "commodity_code"]).reset_index(
            drop=True
        )

    expected_sync_definition = pd.DataFrame(
        [
            {
                "reporter_iso2": "US",
                "period": pd.Period("2021-01"),
                "commodity_code": "2711",
            },
            {
                "reporter_iso2": "CA",
                "period": pd.Period("2021-01"),
                "commodity_code": "2711",
            },
            {
                "reporter_iso2": "US",
                "period": pd.Period("2021-02"),
                "commodity_code": "2711",
            },
            {
                "reporter_iso2": "CA",
                "period": pd.Period("2021-02"),
                "commodity_code": "2711",
            },
            {
                "reporter_iso2": "US",
                "period": pd.Period("2021-01"),
                "commodity_code": "2701",
            },
            {
                "reporter_iso2": "CA",
                "period": pd.Period("2021-01"),
                "commodity_code": "2701",
            },
            {
                "reporter_iso2": "US",
                "period": pd.Period("2021-02"),
                "commodity_code": "2701",
            },
            {
                "reporter_iso2": "CA",
                "period": pd.Period("2021-02"),
                "commodity_code": "2701",
            },
        ]
    )

    assert_frame_equal(
        ignoring_order(actual_sync_definition),
        ignoring_order(expected_sync_definition),
    )


def test_comtrade_engine__nothing_yet_synced__syncs_new_data(mocker):

    # Arrange
    mocked_client = create_mocked_comtrade_responses(
        mocker,
        availability_response=availability__response__multiple_countries_one_month,
        trade_responses=trade__responses__row_per_request_combination,
    )
    create_mocked_db_sync_history(
        mocker,
        history=history__no_history,
    )
    mocked_upsert = mocker.patch("engines.comtrade.upsert")

    sync_definition = comtrade.create_sync_definitions(
        reporter_iso2s=["US"],
        commodities=[comtrade.ComtradeCommodities.NATURAL_GAS],
        start=dt.date(2021, 1, 1),
        end=dt.date(2021, 1, 31),
    )

    start_time = dt.datetime(2021, 1, 1)

    # Act
    comtrade.update_comtrade_data(sync_definition)

    # Assert
    mocked_client.get_monthly_trades_for_periods.assert_called_once_with(
        reporter="US",
        periods=[pd.Period("2021-01")],
        commodities=[comtrade.ComtradeCommodities.NATURAL_GAS],
    )

    upsert_records = mocked_upsert.call_args_list[0][1]
    assert upsert_records["table"] == ComtradeHsTradeRecord.__tablename__
    assert upsert_records["constraint_name"] == "comtrade_hs_record_unique"
    assert_frame_equal(
        upsert_records["df"],
        pd.DataFrame(
            [
                {
                    "reporter_iso2": "US",
                    "partner_iso2": "CA",
                    "commodity_code": "2711",
                    "flow_direction": "Imports",
                    "period": dt.datetime(2021, 1, 1),
                    "quantity": 100,
                    "quantity_unit": "kg",
                    "value_usd": 100,
                }
            ]
        ),
    )

    upsert_history = mocked_upsert.call_args_list[1][1]
    assert upsert_history["table"] == ComtradeSyncHistory.__tablename__
    assert upsert_history["constraint_name"] == "comtrade_sync_history_unique"
    assert_frame_equal(
        upsert_history["df"].drop(columns=["last_updated"]),
        pd.DataFrame(
            [{"reporter_iso2": "US", "period": dt.datetime(2021, 1, 1), "commodity_code": "2711"}]
        ),
    )
    assert upsert_history["df"]["last_updated"].iloc[0] is not None
    assert (
        upsert_history["df"]["last_updated"].iloc[0] >= start_time
        and upsert_history["df"]["last_updated"].iloc[0] <= dt.datetime.now()
    )


def test_comtrade_engine__data_already_synced__does_not_sync(mocker):

    # Arrange
    mocked_client = create_mocked_comtrade_responses(
        mocker,
        availability_response=availability__response__multiple_countries_one_month,
        trade_responses=trade__responses__row_per_request_combination,
    )
    create_mocked_db_sync_history(
        mocker,
        history=history__us_one_month_updated_recently,
    )
    mocked_upsert = mocker.patch("engines.comtrade.upsert")

    sync_definition = comtrade.create_sync_definitions(
        reporter_iso2s=["US"],
        commodities=[comtrade.ComtradeCommodities.NATURAL_GAS],
        start=dt.date(2021, 1, 1),
        end=dt.date(2021, 1, 31),
    )

    # Act
    comtrade.update_comtrade_data(sync_definition)

    # Assert
    mocked_upsert.assert_not_called()


def test_comtrade_engine__data_already_synced__force_sync(mocker):

    # Arrange
    create_mocked_comtrade_responses(
        mocker,
        availability_response=availability__response__multiple_countries_one_month,
        trade_responses=trade__responses__row_per_request_combination,
    )
    create_mocked_db_sync_history(
        mocker,
        history=history__us_one_month_updated_recently,
    )
    mocked_upsert = mocker.patch("engines.comtrade.upsert")

    sync_definition = comtrade.create_sync_definitions(
        reporter_iso2s=["US"],
        commodities=[comtrade.ComtradeCommodities.NATURAL_GAS],
        start=dt.date(2021, 1, 1),
        end=dt.date(2021, 2, 1),
    )

    # Act
    comtrade.update_comtrade_data(sync_definition, force=True)

    # Assert
    assert mocked_upsert.call_count == 2


def test_comtrade_engine__new_month_available__syncs_new_month(mocker):

    # Arrange
    create_mocked_comtrade_responses(
        mocker,
        availability_response=availability__response__us_two_months,
        trade_responses=trade__responses__row_per_request_combination,
    )
    create_mocked_db_sync_history(
        mocker,
        history=history__us_one_month_updated_recently,
    )
    mocked_upsert = mocker.patch("engines.comtrade.upsert")

    sync_definition = comtrade.create_sync_definitions(
        reporter_iso2s=["US"],
        commodities=[comtrade.ComtradeCommodities.NATURAL_GAS],
        start=dt.date(2021, 1, 1),
        end=dt.date(2021, 2, 28),
    )

    # Act
    comtrade.update_comtrade_data(sync_definition)

    # Assert
    upsert_records = mocked_upsert.call_args_list[0][1]
    assert upsert_records["table"] == ComtradeHsTradeRecord.__tablename__
    assert upsert_records["constraint_name"] == "comtrade_hs_record_unique"
    assert_frame_equal(
        upsert_records["df"],
        pd.DataFrame(
            [
                {
                    "reporter_iso2": "US",
                    "partner_iso2": "CA",
                    "commodity_code": "2711",
                    "flow_direction": "Imports",
                    "period": dt.datetime(2021, 2, 1),
                    "quantity": 100,
                    "quantity_unit": "kg",
                    "value_usd": 100,
                }
            ]
        ),
    )


def test_comtrade_engine__new_commodity__syncs_new_commodity(mocker):

    # Arrange
    create_mocked_comtrade_responses(
        mocker,
        availability_response=availability__response__us_two_months,
        trade_responses=trade__responses__row_per_request_combination,
    )
    create_mocked_db_sync_history(
        mocker,
        history=history__us_one_month_updated_recently,
    )
    mocked_upsert = mocker.patch("engines.comtrade.upsert")

    sync_definition = comtrade.create_sync_definitions(
        reporter_iso2s=["US"],
        commodities=[comtrade.ComtradeCommodities.NATURAL_GAS, comtrade.ComtradeCommodities.COAL],
        start=dt.date(2021, 1, 1),
        end=dt.date(2021, 1, 31),
    )

    # Act
    comtrade.update_comtrade_data(sync_definition)

    # Assert
    upsert_records = mocked_upsert.call_args_list[0][1]
    assert upsert_records["table"] == ComtradeHsTradeRecord.__tablename__
    assert upsert_records["constraint_name"] == "comtrade_hs_record_unique"
    assert_frame_equal(
        upsert_records["df"],
        pd.DataFrame(
            [
                {
                    "reporter_iso2": "US",
                    "partner_iso2": "CA",
                    "commodity_code": "2701",
                    "flow_direction": "Imports",
                    "period": dt.datetime(2021, 1, 1),
                    "quantity": 100,
                    "quantity_unit": "kg",
                    "value_usd": 100,
                }
            ]
        ),
    )


def test_comtrade_engine__new_data_with_month_gaps__doesnt_update_inbetween_months(mocker):

    # Arrange
    create_mocked_comtrade_responses(
        mocker,
        availability_response=availability__response__us_months_with_gap,
        trade_responses=trade__responses__row_per_request_combination,
    )
    create_mocked_db_sync_history(
        mocker,
        history=history__middle_month_updated,
    )

    mocked_upsert = mocker.patch("engines.comtrade.upsert")

    sync_definition = comtrade.create_sync_definitions(
        reporter_iso2s=["US"],
        commodities=[comtrade.ComtradeCommodities.NATURAL_GAS],
        start=dt.date(2021, 1, 1),
        end=dt.date(2021, 3, 31),
    )

    # Act
    comtrade.update_comtrade_data(sync_definition)

    # Assert
    upsert_records = mocked_upsert.call_args_list[0][1]
    assert upsert_records["table"] == ComtradeHsTradeRecord.__tablename__
    assert upsert_records["constraint_name"] == "comtrade_hs_record_unique"
    assert_frame_equal(
        upsert_records["df"],
        pd.DataFrame(
            [
                {
                    "reporter_iso2": "US",
                    "partner_iso2": "CA",
                    "commodity_code": "2711",
                    "flow_direction": "Imports",
                    "period": dt.datetime(2021, 1, 1),
                    "quantity": 100,
                    "quantity_unit": "kg",
                    "value_usd": 100,
                },
                {
                    "reporter_iso2": "US",
                    "partner_iso2": "CA",
                    "commodity_code": "2711",
                    "flow_direction": "Imports",
                    "period": dt.datetime(2021, 3, 1),
                    "quantity": 100,
                    "quantity_unit": "kg",
                    "value_usd": 100,
                },
            ]
        ),
    )


def test_comtrade_engine__multiple_countries__syncs_all(mocker):

    # Arrange
    create_mocked_comtrade_responses(
        mocker,
        availability_response=availability__response__multiple_countries_one_month,
        trade_responses=trade__responses__row_per_request_combination,
    )
    create_mocked_db_sync_history(
        mocker,
        history=history__no_history,
    )
    mocked_upsert = mocker.patch("engines.comtrade.upsert")

    sync_definition = comtrade.create_sync_definitions(
        reporter_iso2s=["US", "CA"],
        commodities=[comtrade.ComtradeCommodities.NATURAL_GAS],
        start=dt.date(2021, 1, 1),
        end=dt.date(2021, 1, 31),
    )

    # Act
    comtrade.update_comtrade_data(sync_definition)

    # Assert
    upsert_records = mocked_upsert.call_args_list[0][1]
    assert upsert_records["table"] == ComtradeHsTradeRecord.__tablename__
    assert upsert_records["constraint_name"] == "comtrade_hs_record_unique"
    assert_frame_equal(
        upsert_records["df"],
        pd.DataFrame(
            [
                {
                    "reporter_iso2": "CA",
                    "partner_iso2": "CA",
                    "commodity_code": "2711",
                    "flow_direction": "Imports",
                    "period": dt.datetime(2021, 1, 1),
                    "quantity": 100,
                    "quantity_unit": "kg",
                    "value_usd": 100,
                }
            ]
        ),
    )

    upsert_history = mocked_upsert.call_args_list[1][1]
    assert upsert_history["table"] == ComtradeSyncHistory.__tablename__
    assert upsert_history["constraint_name"] == "comtrade_sync_history_unique"
    assert_frame_equal(
        upsert_history["df"].drop(columns=["last_updated"]),
        pd.DataFrame(
            [
                {
                    "reporter_iso2": "CA",
                    "period": dt.datetime(2021, 1, 1),
                    "commodity_code": "2711",
                },
            ]
        ),
    )

    upsert_records = mocked_upsert.call_args_list[2][1]
    assert upsert_records["table"] == ComtradeHsTradeRecord.__tablename__
    assert upsert_records["constraint_name"] == "comtrade_hs_record_unique"
    assert_frame_equal(
        upsert_records["df"],
        pd.DataFrame(
            [
                {
                    "reporter_iso2": "US",
                    "partner_iso2": "CA",
                    "commodity_code": "2711",
                    "flow_direction": "Imports",
                    "period": dt.datetime(2021, 1, 1),
                    "quantity": 100,
                    "quantity_unit": "kg",
                    "value_usd": 100,
                }
            ]
        ),
    )

    upsert_history = mocked_upsert.call_args_list[3][1]
    assert upsert_history["table"] == ComtradeSyncHistory.__tablename__
    assert upsert_history["constraint_name"] == "comtrade_sync_history_unique"
    assert_frame_equal(
        upsert_history["df"].drop(columns=["last_updated"]),
        pd.DataFrame(
            [
                {
                    "reporter_iso2": "US",
                    "period": dt.datetime(2021, 1, 1),
                    "commodity_code": "2711",
                },
            ]
        ),
    )


def test_comtrade_engine__no_sync_request__value_error():
    with pytest.raises(ValueError) as e:
        comtrade.update_comtrade_data(pd.DataFrame())


def create_mocked_db_sync_history(mocker, *, history: pd.DataFrame):
    sync_history = mocker.patch("engines.comtrade.read_sql")
    # We use side effect to return a new DataFrame each time the function is called.
    sync_history.side_effect = history

    mocker.patch("engines.comtrade.session")


def create_mocked_comtrade_responses(
    mocker, *, availability_response: pd.DataFrame, trade_responses: pd.DataFrame
):

    mocked_ComtradeClient = mocker.patch("engines.comtrade.client")
    # We use side effect to return a new DataFrame each time the function is called.
    mocked_ComtradeClient.get_data_availability.side_effect = availability_response
    mocked_ComtradeClient.get_monthly_trades_for_periods.side_effect = trade_responses

    return mocked_ComtradeClient
