from enum import Enum
import requests
from engines.kpler_scraper.scraper_flow import KplerFlowScraper
from base.kpler import FlowsSplit, FlowsPeriod, FlowsMeasurementUnit
from base.env import get_env
import datetime as dt
from base.utils import to_datetime
import pandas as pd
import numpy as np

from base.logger import logger

FST_API_URL = get_env("FOSSIL_SHIPMENT_TRACKER_API_URL")
FST_API_KEY = get_env("API_KEY")

scraper = KplerFlowScraper()


class KplerCheckerProducts(Enum):
    CRUDE = {
        "kpler_product": "Crude/Co",
        "our_type": "group",
        "our_product": "Crude/Co",
    }
    LNG = {
        "kpler_product": "lng",
        "our_type": "commodity",
        "our_product": "lng",
    }
    GASOIL_DIESEL = {
        "kpler_product": "Gasoil/Diesel",
        "our_type": "group",
        "our_product": "Gasoil/Diesel",
    }
    METALLURGICAL_COAL = {
        "kpler_product": "Metallurgical",
        "our_type": "commodity",
        "our_product": "Metallurgical",
    }
    THERMAL_COAL = {
        "kpler_product": "Thermal",
        "our_type": "commodity",
        "our_product": "Thermal",
    }

    @property
    def kpler_product(self):
        return self.value["kpler_product"]

    @property
    def our_type(self):
        return self.value["our_type"]

    @property
    def our_product(self):
        return self.value["our_product"]


def test_kpler_trades(date_from=None, date_to=None, product=None, origin_iso2=None):
    logger.debug(f"Testing kpler trades for {product} from {origin_iso2} after {date_from}")
    start_date = to_datetime(date_from).date()
    end_date = dt.datetime.now().date() - dt.timedelta(days=1)
    if date_to:
        end_date = to_datetime(date_to).date()

    flows = get_flows_from_kpler(
        product=product, origin_iso2=origin_iso2, date_from=start_date, date_to=end_date
    )

    aggregated_trades = get_aggregated_trades_from_api(
        product=product, origin_iso2=origin_iso2, date_from=start_date, date_to=end_date
    )

    comparison = compare_flows_to_trades(flows, aggregated_trades)

    expected = comparison["value_tonne.expected"].sum()
    actual = comparison["value_tonne.actual"].sum()

    sum_close = np.isclose(expected, actual, rtol=0.01)

    asserts = [
        (
            sum_close,
            f"Expected totals similar for {product} from {origin_iso2} after {date_from}:"
            + f"expected {expected} ~= actual {actual}.",
        ),
        (
            comparison["ok"].all(),
            f"Expected monthly values similar for {product} from {origin_iso2} after {date_from}:\n"
            + format_failed(comparison[~comparison.ok]),
        ),
        (
            len(comparison[comparison.ok_strict == False]) < 10,
            f"More than 10 monthly values too different for {product} from {origin_iso2} after {date_from}:\n"
            + format_failed(comparison[~comparison.ok_strict]),
        ),
    ]

    failure_reasons = [message for assert_condition, message in asserts if not assert_condition]

    if failure_reasons:
        combined_reasons = "\n".join([f" - {failure}" for failure in failure_reasons])
        raise AssertionError(f"One or more failure conditions occurred:\n{combined_reasons}")


def compare_flows_to_trades(flows, aggregated_trades):

    # Where flows.iso2 is missing a value, set it as "unknown"
    flows["to_iso2"] = flows["to_iso2"].fillna("unknown")

    flows_and_aggregated_trades = pd.merge(
        left=flows,
        right=aggregated_trades,
        how="outer",
        on=["month", "to_iso2"],
        suffixes=(".expected", ".actual"),
    )
    comparison_per_month = (
        flows_and_aggregated_trades[pd.notnull(flows_and_aggregated_trades.to_iso2)]
        .fillna({"value_tonne.expected": 0, "value_tonne.actual": 0})
        .groupby("month")
        .aggregate({"value_tonne.expected": "sum", "value_tonne.actual": "sum"})
        .reset_index()
        .sort_values("month")
    )

    average_month = (
        comparison_per_month["value_tonne.expected"].mean()
        + comparison_per_month["value_tonne.actual"].mean()
    ) / 2

    comparison_per_month["ok"] = np.isclose(
        comparison_per_month["value_tonne.expected"],
        comparison_per_month["value_tonne.actual"],
        rtol=0.05,
        atol=average_month * 0.1,
    )

    comparison_per_month["ok_strict"] = np.isclose(
        comparison_per_month["value_tonne.expected"],
        comparison_per_month["value_tonne.actual"],
        rtol=0.01,
        atol=average_month * 0.01,
    )

    return comparison_per_month


def get_flows_from_kpler(product: KplerCheckerProducts, origin_iso2, date_from, date_to):
    df = scraper.get_flows(
        origin_iso2=origin_iso2,
        product=product.kpler_product,
        granularity=FlowsPeriod.Monthly,
        unit=FlowsMeasurementUnit.T,
        date_from=date_from,
        date_to=date_to,
        split=FlowsSplit.DestinationCountries,
    )

    if df is None:
        return pd.DataFrame(
            {
                "month": pd.Series(dtype="str"),
                "to_iso2": pd.Series(dtype="str"),
                "value_tonne": pd.Series(dtype="float64"),
            }
        )

    column_selector = {
        "date": "month",
        "to_iso2": "to_iso2",
        "value": "value_tonne",
    }

    renamed_df = df.rename(columns=column_selector)[[*column_selector.values()]]
    renamed_df["month"] = renamed_df["month"].map(lambda x: x.isoformat()[:10])

    return renamed_df


def get_aggregated_trades_from_api(product, origin_iso2, date_from, date_to):
    product_type = product.our_type

    params = {
        "api_key": FST_API_KEY,
        product_type: product.our_product,
        "origin_iso2": origin_iso2,
        "aggregate_by": f"origin_month,destination_iso2",
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "exclude_within_country": "false",
        "check_complete": "false",
        "format": "json",
    }

    response = requests.get(FST_API_URL + "/v1/kpler_trade", params=params)

    if response.status_code != 200 and response.status_code != 204:
        raise Exception(
            f"Failed to get Kpler trade data for {product} from {origin_iso2} after {date_from} with status code {response.status_code}: using {response.url}"
        )

    if response.status_code == 204:
        return pd.DataFrame(
            {
                "month": pd.Series(dtype="str"),
                "to_iso2": pd.Series(dtype="str"),
                "value_tonne": pd.Series(dtype="float64"),
            }
        )

    column_selector = {
        "month": "month",
        "destination_iso2": "to_iso2",
        "value_tonne": "value_tonne",
    }

    df = pd.DataFrame(response.json()["data"]).rename(columns=column_selector)[
        [*column_selector.values()]
    ]

    df["month"] = df["month"].map(lambda x: x[:10])

    return df


def format_failed(failed):
    format_number = lambda n: f"{round(n / 1e3, 3)}kt"
    row_to_reason = lambda row: (
        f"{row['month']}: "
        + f"expected {format_number(row['value_tonne.expected'])}"
        + f" ~= actual {format_number(row['value_tonne.actual'])}"
    )
    return " | ".join(failed.apply(row_to_reason, axis=1))
