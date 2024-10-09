import re
from typing import List
import requests
import sqlalchemy as sa
import pandas as pd
import numpy as np

from sqlalchemy import func, tablesample
from sqlalchemy.orm import aliased

from base.db import session
from base.models import (
    ShipmentWithSTS,
    Shipment,
    ShipmentArrivalBerth,
    ShipmentDepartureBerth,
    Departure,
    Arrival,
    ShipInspection,
)
from base.models.kpler import KplerTrade, KplerTradeComputed, KplerProduct, KplerZone
from base.logger import logger_slack, logger

import datetime as dt

from engines import fossil_tracker_api_client

from base.models import Price, Commodity

KPLER_TRADES_WITHOUT_PRICES_IGNORED_PRODUCTS = [
    {
        "name": "Bitumen/Asphalt",
        "level": "commodity",
    },
    {
        "name": "Bitumen.*",
        "level": "product",
    },
    {
        "name": "Bitumen.*",
        "level": "grade",
    },
    {
        "name": "CBFS",
        "level": "commodity",
    },
    {
        "name": "Clean Condensate",
        "level": "commodity",
    },
    {
        "name": "Clean Products",
        "level": "family",
    },
    {
        "name": "Cutter Stock",
        "level": "commodity",
    },
    {
        "name": "DPP",
        "level": "family",
    },
    {
        "name": "LCO",
        "level": "grade",
    },
    {
        "name": "Yamal Co.",
        "level": "grade",
    },
    {
        "name": "Coke",
        "level": "commodity",
    },
    {
        "name": "Met Coke",
        "level": "grade",
    },
]
KPLER_TRADES_WITHOUT_PRICES_MAX_IGNORED_PER_MONTH = 9
KPLER_TRADES_WITHOUT_PRICES_MAX_IGNORED_ADDITIONAL = 10


def test_insurers_no_unexpected_unknown():
    """ " If scraping fails and our code doesn't detect it, it adds a false 'unknown'.
    We try to detect these by detecting patterns where an insurer was known, then unknown,
    and at a later date, then found again.
    Note that this will only work after we had a successful scraping
    """

    raw_sql = """
    WITH unknown
     AS (SELECT *
         FROM   ship_insurer
         WHERE  company_raw_name = 'unknown' and is_valid),
     known
     AS (SELECT *
         FROM   ship_insurer
         WHERE  company_raw_name != 'unknown' and is_valid),
     problematic
     AS (SELECT s.commodity,
                u.updated_on - u.date_from_equasis,
                u.*,
                k.date_from_equasis,
                k.updated_on
         FROM   unknown u
                LEFT JOIN known k
                       ON u.ship_imo = k.ship_imo
                LEFT JOIN ship s
                       ON s.imo = u.ship_imo
         WHERE  ( k.date_from_equasis < u.date_from_equasis
                   OR k.date_from_equasis IS NULL )
                AND ( k.updated_on > u.updated_on )
                AND u.updated_on - u.date_from_equasis < '100 days'
            )
        SELECT *
        FROM   problematic;
    """

    result = session.execute(raw_sql)

    unexpected_unknown = result.rowcount > 0

    assert not unexpected_unknown, build_insurers_unknown_error_info(result, raw_sql)


def build_insurers_unknown_error_info(result, query):
    missing_types = [row[0] for row in result]
    count = pd.Series(missing_types).value_counts()
    count_table = count.to_string()
    return f"There are ships marked with Unknown insurers that shouldn't be:\n{count_table}\n\nUsing query:\n{query}"


def test_insurers_no_null_date_from():
    # Test that those will only one insurer have a null date_from
    raw_sql = """
        WITH count as (select ship_imo, min(date_from_equasis), count(*) from ship_insurer where is_valid group by 1)
        SELECT * from count where count = 1 and min is not null
        """

    wrong_date_from = session.execute(raw_sql)

    assert (
        wrong_date_from.rowcount == 0
    ), f"There are insurers with date_from_equasis = NULL even though these are the only ones identified: {', '.join([row[0] for row in wrong_date_from])}"


def test_overland_trade_has_values():
    # create a date range for each year from the start of 2020 to today
    start_date = dt.date(2020, 1, 1)
    end_date = dt.date.today()
    date_ranges = pd.date_range(start=start_date, end=end_date, freq="YS")

    dfs = []

    # for each year, check that we have overland trade values for each commodity and month
    for date in date_ranges:
        year_start = date.date()
        year_end = (date + pd.DateOffset(years=1)).date()

        params = {"date_from": year_start.isoformat(), "date_to": year_end.isoformat()}

        response = fossil_tracker_api_client.get_overland(**params)

        dfs = dfs + [response]

    df = pd.concat(dfs)

    df["month"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")

    assert not df.empty, "No overland trade data found"

    expected_commodities = [
        "coal_rail_road",
        "coke_rail_road",
        "crude_oil_rail_road",
        "natural_gas",
        "oil_products_pipeline",
        "oil_products_rail_road",
        "pipeline_oil",
    ]

    assert set(df["commodity"]) == set(expected_commodities), "Commodities sets do not match"

    verify_months_for_commodities(
        df,
        start_date=start_date,
        end_date=end_date,
        commodities=["natural_gas", "pipeline_oil"],
    )
    verify_months_for_commodities(
        df, start_date=start_date, end_date="2022-08-01", commodities=["coal_rail_road"]
    )


def verify_months_for_commodities(df, start_date, end_date, commodities):
    months = set(pd.date_range(start=start_date, end=end_date, freq="M").strftime("%Y-%m"))

    # Do the above loop but in a single assert statement
    assert all(
        [not (months - set(df[df["commodity"] == commodity]["month"])) for commodity in commodities]
    ), "Missing months for some commodities"


def test_counter_pricing_positive():

    data = pd.DataFrame()
    for year in range(2022, dt.date.today().year + 1):
        response = fossil_tracker_api_client.get_counter(
            date_from=f"{year}-01-01", date_to=f"{year}-12-31"
        ).sort_values(["date"], ascending=False)
        data = pd.concat([data, response])

    max_date = pd.to_datetime(data.date).max().date()
    all_positive = all(data.value_eur >= 0)

    not_positive = data[data.value_eur < 0]

    not_positive["year"] = pd.to_datetime(not_positive.date).dt.year

    not_positive_summary = (
        not_positive.groupby(by=["year", "commodity", "destination_iso2"])
        .size()
        .reset_index(name="count")
    )
    assert max_date.year == dt.date.today().year, f"Counter is incomplete, max date was: {max_date}"
    assert all_positive, "Counter pricing is not all positive:\n" + not_positive_summary.to_string()


def test_kpler_trades_without_prices():

    start_date = "2022-01-01"

    missing_rows = (
        session.query(KplerProduct.name, KplerProduct.type, func.count(KplerTrade.id))
        .select_from(KplerProduct)
        .outerjoin(KplerTrade, KplerTrade.product_id == KplerProduct.id)
        .outerjoin(
            KplerTradeComputed,
            (KplerTradeComputed.trade_id == KplerTrade.id)
            & (KplerTradeComputed.flow_id == KplerTrade.flow_id),
        )
        .outerjoin(KplerZone, (KplerZone.id == KplerTrade.departure_zone_id))
        .filter(
            KplerTradeComputed.trade_id == None,
            KplerTrade.is_valid == True,
            KplerZone.country_iso2 == "RU",
            KplerTrade.departure_date_utc >= start_date,
        )
        .group_by(KplerProduct.name, KplerProduct.type)
        .all()
    )

    without_ignored = [
        (product, level, count)
        for product, level, count in missing_rows
        if not any(
            re.match(ignored_product["name"], product) and ignored_product["level"] == level
            for ignored_product in KPLER_TRADES_WITHOUT_PRICES_IGNORED_PRODUCTS
        )
    ]

    total_rows_missing = sum([count for _, _, count in missing_rows])
    total_without_ignored = sum([count for _, _, count in without_ignored])

    total_ignored_rows = total_rows_missing - total_without_ignored
    months_between_start_and_today = (
        dt.date.today() - dt.datetime.strptime(start_date, "%Y-%m-%d").date()
    ).days / 30
    max_ignored_rows = (
        KPLER_TRADES_WITHOUT_PRICES_MAX_IGNORED_PER_MONTH * months_between_start_and_today
        + KPLER_TRADES_WITHOUT_PRICES_MAX_IGNORED_ADDITIONAL
    )

    assert not without_ignored, f"Some Kpler trades are missing computed rows: {without_ignored}"

    assert (
        total_ignored_rows <= max_ignored_rows
    ), f"Too many ignored rows {total_ignored_rows} > {max_ignored_rows}: {missing_rows}"


def check_china_russia_source():
    max_age_months = 3
    max_age_days = max_age_months * 30
    three_months_ago = dt.date.today() - dt.timedelta(days=max_age_days)

    # Get oldest date in spreadsheet
    url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQunCwQmOpGXSLWiToq6zZDLi3VFqknU2fyDrRCtURFCT2QS1oer4H9i_eCXnyZfw/pub?output=csv"
    df = pd.read_csv(url, skiprows=2)
    df = df[df["Name"].str.match(r"\d{4}-\d{2}")]
    assert len(df) > 0, "China Russia spreadsheet is empty"
    df["date"] = pd.to_datetime(df["Name"], format="%Y-%m")
    max_date = df["date"].max().date()

    assert (
        max_date > three_months_ago
    ), f"China Russia spreadsheet is more than {max_age_months} months old, last date was {max_date}"


def check_ship_inspections_report_date_filled():

    # Get the ship inspections that have a report date filled
    ship_inspections = (
        session.query(ShipInspection).filter(ShipInspection.date_of_report != None).all()
    )

    assert len(ship_inspections) > 0, "No ship inspections with a date of report filled"
