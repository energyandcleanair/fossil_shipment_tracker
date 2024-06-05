import pandas as pd
from pandas import read_sql
from engines.comtrade_client.comtrade import ComtradeClient, ComtradeCommodities

from datetime import date

from base.db import session
from base.models.comtrade import ComtradeSyncHistory, ComtradeHsTradeRecord

from base.db_utils import upsert
from base.logger import logger

client = ComtradeClient.from_env()


def _get_all_reporters():
    return client.get_all_reporters()["reporter_iso2"].to_list()


def create_sync_definitions_for_all(start: date, end: date):
    """
    Create a DataFrame of sync definitions to fetch data for all reporters, all commodities and the
    given date range.
    """
    reporters = _get_all_reporters()
    return create_sync_definitions(
        reporter_iso2s=reporters, commodities=[e for e in ComtradeCommodities], start=start, end=end
    )


def create_sync_definitions_for_all_reporters(
    *, commodities: list[ComtradeCommodities], start: date, end: date
):
    """
    Create a DataFrame of sync definitions to fetch data for all reporters, and the given
    commodities and date range.
    """
    reporters = _get_all_reporters()
    return create_sync_definitions(
        reporter_iso2s=reporters,
        commodities=commodities,
        start=start,
        end=end,
    )


def create_sync_definitions(
    *, reporter_iso2s: list[str], commodities: list[ComtradeCommodities], start: date, end: date
):
    """
    Create a DataFrame of sync definitions to fetch data for a given set of reporters, commodities
    and date range.
    """
    periods = pd.date_range(start, end, freq="M").to_period()
    return pd.DataFrame(
        [
            {"reporter_iso2": reporter, "period": period, "commodity_code": commodity.value}
            for reporter in reporter_iso2s
            for period in periods
            for commodity in commodities
        ]
    )


def update_comtrade_data(sync_definitions: pd.DataFrame, force=False):
    """
    Update the comtrade data for the given sync definitions. The sync definitions should be a
    DataFrame with the following columns:
    - reporter_iso2: ISO2 code of the reporter country
    - period: Period of the data
    - commodity_code: HS code of the commodity

    If force is True, the data will be fetched regardless of whether it has been fetched before.
    """

    if sync_definitions.empty:
        raise ValueError("No sync definitions provided")

    if not all(
        [col in sync_definitions.columns for col in ["reporter_iso2", "period", "commodity_code"]]
    ):
        raise ValueError(
            "sync_definitions must have columns: reporter_iso2, period, commodity_code"
        )

    requests = _identify_requests_to_make(sync_definitions, force=force)

    if not requests.empty:
        for request in requests:
            logger.info(f"Updating {request['reporter_iso2']}")

            last_updated = pd.Timestamp.now()

            comtrade_results = _get_data_from_comtrade_for_request(request)
            sync_history = _convert_request_to_sync_records(request, last_updated)

            logger.info(f"Upserting {comtrade_results.shape[0]} trade records")
            upsert(
                df=comtrade_results,
                table=ComtradeHsTradeRecord.__tablename__,
                constraint_name="comtrade_hs_record_unique",
            )
            logger.info(f"Upserting sync history")
            upsert(
                df=sync_history,
                table=ComtradeSyncHistory.__tablename__,
                constraint_name="comtrade_sync_history_unique",
            )
    else:
        logger.info("No new data to fetch")


def _convert_request_to_sync_records(request, last_updated):
    logger.info(f"Converting request info to sync records")
    sync_history = pd.DataFrame(
        [
            {
                "reporter_iso2": request["reporter_iso2"],
                "period": request["periods"],
                "commodity_code": request["commodities"],
                "last_updated": last_updated,
            }
        ]
    )

    sync_history = sync_history.explode("period").explode("commodity_code")

    sync_history["period"] = sync_history["period"].dt.to_timestamp()
    sync_history["commodity_code"] = sync_history["commodity_code"].apply(lambda x: x.value)
    return sync_history


def _get_data_from_comtrade_for_request(request):
    logger.info(f"Fetching data for {request['reporter_iso2']}")
    data = client.get_monthly_trades_for_periods(
        reporter=request["reporter_iso2"],
        periods=request["periods"],
        commodities=request["commodities"],
    )

    if data.empty:
        logger.warning(f"No data found for {request['reporter_iso2']}")
        return data

    # convert periods to datetime
    data["period"] = data["period"].dt.to_timestamp()
    return data


def _identify_requests_to_make(sync_definitions, force):
    if force:
        return _to_requests(sync_definitions)
    logger.info("Identifying requests to make")
    availability = _get_availability_of_data(sync_definitions)

    sync_definitions["period"] = sync_definitions["period"].dt.to_timestamp()

    availability_for_definitions = sync_definitions.merge(
        availability, how="left", on=["reporter_iso2", "period"]
    )

    to_fetch = _find_records_which_need_updating(availability_for_definitions)

    return _to_requests(to_fetch)


def _to_requests(to_fetch):
    def value_to_commodity(code):
        return next(
            (commodity for commodity in ComtradeCommodities if commodity.value == code), None
        )

    return to_fetch.groupby("reporter_iso2", group_keys=False).apply(
        lambda group: {
            "reporter_iso2": group["reporter_iso2"].iloc[0],
            "periods": group["period"]
            .apply(lambda x: x if isinstance(x, pd.Period) else pd.Period(x, freq="M"))
            .unique()
            .tolist(),
            "commodities": [
                value_to_commodity(commodity)
                for commodity in group["commodity_code"].unique().tolist()
            ],
        }
    )


def _find_records_which_need_updating(availability: pd.DataFrame):
    logger.info("Finding records which need updating")
    sync_history = read_sql(session.query(ComtradeSyncHistory).statement, session.bind)

    if sync_history.empty:
        sync_history = pd.DataFrame(
            columns=["reporter_iso2", "period", "commodity_code", "last_updated"]
        )

    # We need to make sure availability.period is in the same date type as the sync_history.period
    availability["period"] = availability["period"].dt.date

    history_and_availability = availability.merge(
        sync_history, how="left", on=["reporter_iso2", "period", "commodity_code"]
    )

    to_fetch = history_and_availability[
        (history_and_availability["last_released"] > history_and_availability["last_updated"])
        | history_and_availability["last_updated"].isna()
    ]

    return to_fetch


def _get_availability_of_data(definitions: pd.DataFrame):

    unique_periods = definitions["period"].unique()

    availability = client.get_data_availability(periods=unique_periods)

    availability["period"] = availability["period"].dt.to_timestamp()

    return availability
