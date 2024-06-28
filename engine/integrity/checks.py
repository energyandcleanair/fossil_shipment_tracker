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
)
from base.models.kpler import KplerTrade, KplerTradeComputed, KplerProduct, KplerZone
from base.logger import logger_slack, logger

import datetime as dt

from engines import fossil_tracker_api_client

from base.models import Price, Commodity


def test_shipment_portcall_integrity():
    # check that shipments exist with some expected/hardcoded portcalls
    shipments = (
        session.query(Shipment.id)
        .join(Departure, Departure.id == Shipment.departure_id)
        .join(Arrival, Arrival.id == Shipment.arrival_id)
        .filter(
            sa.or_(
                sa.and_(Departure.portcall_id == 121521, Arrival.portcall_id == 129614),
                sa.and_(Departure.portcall_id == 121840, Arrival.portcall_id == 122068),
                sa.and_(Departure.portcall_id == 627232, Arrival.portcall_id == 643501),
                sa.and_(Departure.portcall_id == 143588, Arrival.portcall_id == 318245),
                sa.and_(Departure.portcall_id == 170033, Arrival.portcall_id == 497229),
            )
        )
        .all()
    )

    assert len(shipments) == 5


def test_shipment_table():
    # check that the shipment table respect unique departures and arrivals

    shipments = session.query(
        Shipment.id.label("shipment_id"),
        Shipment.arrival_id,
        Departure.id.label("departure_id"),
    ).join(Departure, Shipment.departure_id == Departure.id)

    arrivals, departures, shipment_ids = (
        [s.arrival_id for s in shipments if s.arrival_id is not None],
        [s.departure_id for s in shipments],
        [s.shipment_id for s in shipments.all()],
    )

    assert len(arrivals) == len(set(arrivals)) and len(departures) == len(set(departures))

    # check that no departure/arrival is references in STS shipments and non-STS shipments

    shipments_sts = session.query(
        ShipmentWithSTS.id.label("shipment_id"),
        ShipmentWithSTS.arrival_id,
        Departure.id.label("departure_id"),
    ).join(Departure, ShipmentWithSTS.departure_id == Departure.id)

    arrivals_sts, departures_sts, shipment_ids_sts = (
        [s.arrival_id for s in shipments_sts if s.arrival_id is not None],
        [s.departure_id for s in shipments_sts],
        [s.shipment_id for s in shipments_sts.all()],
    )

    assert not list(set(departures_sts) & set(departures)) and not list(
        set(arrivals_sts) & set(arrivals)
    )

    assert not (list(set(shipment_ids) & set(shipment_ids_sts)))


def test_berths():
    # make sure we respect that all shipments in departure and arrival berth have a matching shipment_id
    berths = session.query(ShipmentDepartureBerth.shipment_id).union(
        session.query(ShipmentArrivalBerth.shipment_id)
    )
    shipments = session.query(Shipment.id).union(session.query(ShipmentWithSTS.id))

    berth_shipment_ids, shipment_ids = [b.shipment_id for b in berths.all()], [
        s.id for s in shipments.all()
    ]

    assert len(set(berth_shipment_ids) & set(shipment_ids)) == len(berth_shipment_ids)


def test_portcall_relationship():
    # verify we have a 1:1 relationship with departures/arrivals and portcall
    # note - departure/arrivals can appear multiple times in the shipment with sts table, but only one portcall should
    # always be linked with departure/arrival

    non_sts_shipments = (
        session.query(
            Shipment.id,
            Departure.portcall_id.label("departure_portcall_id"),
            Arrival.portcall_id.label("arrival_portcall_id"),
        )
        .join(Departure, Departure.id == Shipment.departure_id)
        .join(Arrival, Arrival.id == Shipment.arrival_id)
    )

    departure_portcall_ids, arrival_portcall_ids = [
        d.departure_portcall_id for d in non_sts_shipments if d.departure_portcall_id is not None
    ], [a.arrival_portcall_id for a in non_sts_shipments if a.arrival_portcall_id is not None]

    assert len(departure_portcall_ids) == len(set(departure_portcall_ids)) and len(
        arrival_portcall_ids
    ) == len(set(arrival_portcall_ids))

    sts_shipments = (
        session.query(
            ShipmentWithSTS.id,
            Departure.portcall_id.label("departure_portcall_id"),
            Arrival.portcall_id.label("arrival_portcall_id"),
        )
        .join(Departure, Departure.id == ShipmentWithSTS.departure_id)
        .join(Arrival, Arrival.id == ShipmentWithSTS.arrival_id)
    )

    departure_portcall_ids_sts, arrival_portcall_ids_sts = [
        d.departure_portcall_id for d in sts_shipments if d.departure_portcall_id is not None
    ], [a.arrival_portcall_id for a in sts_shipments if a.arrival_portcall_id is not None]

    assert not len(set(departure_portcall_ids_sts) & set(departure_portcall_ids)) and not len(
        set(arrival_portcall_ids_sts) & set(arrival_portcall_ids)
    )


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

    products_missing_computed_rows = (
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
            KplerTrade.departure_date_utc >= "2022-01-01",
        )
        .group_by(KplerProduct.name, KplerProduct.type)
        .all()
    )

    assert (
        not products_missing_computed_rows
    ), f"Some Kpler trades are missing computed rows: {products_missing_computed_rows}"


def check_china_russia_source():
    max_age_months = 3
    max_age_days = max_age_months * 30
    three_months_ago = dt.date.today() - dt.timedelta(days=max_age_days)

    # Get oldest date in spreadsheet
    url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQunCwQmOpGXSLWiToq6zZDLi3VFqknU2fyDrRCtURFCT2QS1oer4H9i_eCXnyZfw/pub?output=csv"
    df = pd.read_csv(url, skiprows=1)
    df = df[df["Name"].str.match(r"\d{4}-\d{2}")]
    assert len(df) > 0, "China Russia spreadsheet is empty"
    df["date"] = pd.to_datetime(df["Name"], format="%Y-%m")
    max_date = df["date"].max().date()

    assert (
        max_date > three_months_ago
    ), f"China Russia spreadsheet is more than {max_age_months} months old, last date was {max_date}"
