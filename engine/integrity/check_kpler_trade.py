import requests
from engines.kpler_scraper.scraper_flow import KplerFlowScraper
from kpler.sdk import FlowsSplit, FlowsPeriod, FlowsMeasurementUnit
from decouple import config
import datetime as dt
from base.utils import to_datetime
import pandas as pd
import numpy as np

FST_API_URL = config("FOSSIL_SHIPMENT_TRACKER_API_URL")
FST_API_KEY = config("API_KEY")

scraper = KplerFlowScraper()

product_info = {
    "Crude/Co": {
        "platform": "liquids",
        "type": "group",
    },
    "LNG": {"platform": "lng", "type": "group"},
}


def test_kpler_trades(date_from=None, product=None, origin_iso2=None):

    start_date = to_datetime(date_from).date()
    end_date = (dt.datetime.now().replace(day=1) - dt.timedelta(days=1)).date()

    flows = get_flows_from_kpler(
        product=product, origin_iso2=origin_iso2, date_from=start_date, date_to=end_date
    )

    aggregated_trades = get_aggregated_trades_from_api(
        product=product, origin_iso2=origin_iso2, date_from=start_date, date_to=end_date
    )

    comparison = compare_flows_to_trades(flows, aggregated_trades)

    assert all(comparison["ok"]), (
        f"Incorrect values for kpler trade for {product} "
        + f"from {origin_iso2} after {date_from}:\n"
        + format_failed(comparison[~comparison.ok])
    )


def compare_flows_to_trades(flows, aggregated_trades):
    comparison = pd.merge(
        flows,
        aggregated_trades,
        how="outer",
        on=["month", "group", "to_iso2"],
        suffixes=(".expected", ".actual"),
    )

    # We don't keep track of trades with unknown destinations.
    comparison = comparison[pd.notnull(comparison.to_iso2)]

    comparison["ok"] = np.isclose(
        comparison["value_tonne.expected"], comparison["value_tonne.actual"], rtol=0.01
    )

    return comparison


def get_flows_from_kpler(product, origin_iso2, date_from, date_to):
    df = scraper.get_flows(
        product_info[product]["platform"],
        origin_iso2=origin_iso2,
        product=product,
        granularity=FlowsPeriod.Monthly,
        unit=FlowsMeasurementUnit.T,
        date_from=date_from,
        date_to=date_to,
        split=FlowsSplit.DestinationCountries,
    )

    column_selector = {
        "date": "month",
        "value": "value_tonne",
        "product": "group",
        "to_iso2": "to_iso2",
    }

    renamed_df = df.rename(columns=column_selector)[[*column_selector.values()]]
    renamed_df["month"] = renamed_df["month"].map(lambda x: x.isoformat()[:10])

    return renamed_df


def get_aggregated_trades_from_api(product, origin_iso2, date_from, date_to):

    product_type = product_info[product]["type"]

    params = {
        "api_key": FST_API_KEY,
        product_type: product,
        "origin_iso2": origin_iso2,
        "aggregate_by": f"origin_month,group,destination_iso2",
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "format": "json",
    }

    response = requests.get(FST_API_URL + "/v1/kpler_trade", params=params)

    column_selector = {
        "month": "month",
        "value_tonne": "value_tonne",
        "group": "group",
        "destination_iso2": "to_iso2",
    }

    df = pd.DataFrame(response.json()["data"]).rename(columns=column_selector)[
        [*column_selector.values()]
    ]

    df["month"] = df["month"].map(lambda x: x[:10])

    return df


def format_failed(failed):
    format_number = lambda n: f"{round(n / 1e3, 3)}kt"
    row_to_reason = lambda row: (
        f" - For {row['month']}, "
        + f"expected {format_number(row['value_tonne.expected'])} "
        + f"but got {format_number(row['value_tonne.actual'])}."
    )
    return "\n".join(failed.apply(row_to_reason, axis=1))
