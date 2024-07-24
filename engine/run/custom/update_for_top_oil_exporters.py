from country_converter import CountryConverter
from base.utils import to_datetime
from engines import (
    commodity,
    counter,
    kpler_scraper,
    kpler_trade_computed,
)
from engines.kpler_scraper.scraper_flow import KplerFlowScraper
from engines.kpler_scraper.update import UpdateParts, UpdateStatus
from base.kpler import FlowsSplit, FlowsPeriod, FlowsMeasurementUnit

import integrity
import base
from base.logger import logger_slack, logger

import datetime as dt

from argparse import ArgumentParser

import pandas as pd


def get_crude_oil_exporters(filter_percentile=None, date_from=None, date_to=None):
    logger.info("Getting crude oil exporters")
    scraper = KplerFlowScraper()

    importer_flows: pd.DataFrame = scraper.get_flows(
        date_from=date_from,
        date_to=date_to,
        product=["Crude/Co", "DPP", "Clean Products"],
        granularity=FlowsPeriod.Annually,
        split=FlowsSplit.OriginCountries,
    )

    total_exports_by_country = (
        importer_flows.groupby("from_iso2").sum("value").sort_values("value", ascending=False)
    )

    # Calculate the cumulative percentage of total exports
    total_exports_by_country["cumulative_percentage"] = (
        total_exports_by_country["value"].cumsum() / total_exports_by_country["value"].sum()
    )

    # Get the top countries where the cumulative percentage amongst them exceeds 99%
    top_countries = total_exports_by_country[
        total_exports_by_country["cumulative_percentage"] <= filter_percentile
    ].reset_index()

    unique_countries = top_countries["from_iso2"].unique()

    without_broken_values = list(filter(lambda x: x != None and x != "not found", unique_countries))

    return sorted(without_broken_values)


def get_countries_to_update(
    filter_countries=None, filter_percentile=None, date_from=None, date_to=None
):
    logger.info("Getting countries to update")
    crude_exporters_to_oil_products = get_crude_oil_exporters(
        filter_percentile=filter_percentile, date_from=date_from, date_to=date_to
    )

    sorted_exporters = sorted(list(set(crude_exporters_to_oil_products)))

    return [
        country
        for country in sorted_exporters
        if filter_countries == None or country in filter_countries
    ]


def update(
    continue_from=None, filter_countries=None, filter_percentile=None, date_from=None, date_to=None
):
    date_from = to_datetime(date_from)
    date_to = to_datetime(date_to)

    countries_to_update = get_countries_to_update(
        filter_countries=filter_countries,
        filter_percentile=filter_percentile,
        date_from=date_from,
        date_to=date_to,
    )

    logger.info(f"Updating countries: {countries_to_update}")

    if continue_from is not None:
        countries_to_update = countries_to_update[countries_to_update.index(continue_from) :]

    result = kpler_scraper.update(
        historic_date_from=date_from,
        historic_date_to=date_to,
        origin_iso2s=countries_to_update,
        parts=[UpdateParts.REFETCH_OUTDATED_HISTORIC_ENTRIES],
    )

    if result == UpdateStatus.FAILED:
        raise RuntimeError("Unable to update Kpler trades, can't continue")

    kpler_trade_computed.update()
    counter.update()

    return


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--date-from", type=str, default=None)
    parser.add_argument("--date-to", type=str, default=dt.datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--continue-from", type=str, default=None)
    # Allow multiple countries to be specified, separated by spaces or commas
    parser.add_argument("--filter-countries", nargs="*", type=str, default=None)
    parser.add_argument("--filter-percentile", type=float, default=0.99)

    args = parser.parse_args()

    if args.date_from is None:
        print("Please specify --date-from")
        exit(1)

    logger_slack.info("=== Update for report: using %s environment ===" % (base.db.environment,))
    try:
        update(
            continue_from=args.continue_from,
            filter_countries=args.filter_countries,
            filter_percentile=args.filter_percentile,
            date_from=args.date_from,
            date_to=args.date_to,
        )
        logger_slack.info("=== Update for report complete ===")
    except BaseException as e:
        logger_slack.error("=== Update for report failed", stack_info=True, exc_info=True)
        raise e
