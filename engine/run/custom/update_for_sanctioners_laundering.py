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

EU_ISO2S = [
    "AT",
    "BE",
    "BG",
    "HR",
    "CY",
    "DK",
    "EE",
    "FI",
    "FR",
    "DE",
    "GR",
    "HU",
    "IS",
    "IE",
    "IT",
    "LV",
    "LT",
    "LU",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SK",
    "SI",
    "ES",
    "SE",
]

G7_ISO2S = ["CA", "FR", "DE", "IT", "JP", "GB", "US"]

OTHER_PRICE_CAP_MEMBERS = ["AU"]

NON_PCC_APPLYING_SANCTIONS = ["NO", "CH"]

ALL_SANCTIONING_COUNTRIES = sorted(
    list(
        set.union(
            set(EU_ISO2S),
            set(G7_ISO2S),
            set(OTHER_PRICE_CAP_MEMBERS),
            set(NON_PCC_APPLYING_SANCTIONS),
        )
    )
)


def get_oil_products_exporters(to_importers=None, date_from=None, date_to=None):
    logger.info("Getting oil products exporters")
    scraper = KplerFlowScraper()

    all_flows = pd.DataFrame()

    for importer in to_importers:
        importer_flows = scraper.get_flows(
            date_from=date_from,
            date_to=date_to,
            destination_iso2=importer,
            granularity=FlowsPeriod.Annually,
            split=FlowsSplit.OriginCountries,
        )
        all_flows = pd.concat([all_flows, importer_flows])

    unique_countries = all_flows["from_iso2"].unique()

    without_broken_values = filter(lambda x: x != None and x != "not found", unique_countries)

    return sorted(list(without_broken_values))


def get_crude_oil_exporters(to_importers=None, date_from=None, date_to=None):
    logger.info("Getting crude oil exporters")
    scraper = KplerFlowScraper()

    all_flows = pd.DataFrame()

    for importer in to_importers:
        importer_flows = scraper.get_flows(
            date_from=date_from,
            date_to=date_to,
            destination_iso2=importer,
            product="Crude/Co",
            granularity=FlowsPeriod.Annually,
            split=FlowsSplit.OriginCountries,
        )
        all_flows = pd.concat([all_flows, importer_flows])

    unique_countries = all_flows["from_iso2"].unique()

    without_broken_values = filter(lambda x: x != None and x != "not found", unique_countries)

    return sorted(list(without_broken_values))


def get_countries_to_update(filter_countries=None, date_from=None, date_to=None):
    logger.info("Getting countries to update")
    countries_to_update = ALL_SANCTIONING_COUNTRIES
    if filter_countries is not None:
        countries_to_update = filter_countries

    exporters_of_oil_products = get_oil_products_exporters(
        to_importers=sorted(countries_to_update), date_from=date_from, date_to=date_to
    )
    crude_exporters_to_oil_products = get_crude_oil_exporters(
        to_importers=exporters_of_oil_products, date_from=date_from, date_to=date_to
    )

    return sorted(
        list(
            set.union(
                set(exporters_of_oil_products),
                set(crude_exporters_to_oil_products),
            )
        )
    )


def update(continue_from=None, filter_countries=None, date_from=None, date_to=None):
    date_from = to_datetime(date_from)
    date_to = to_datetime(date_to)

    countries_to_update = get_countries_to_update(date_from=date_from, date_to=date_to)

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

    args = parser.parse_args()

    if args.date_from is None:
        print("Please specify --date-from")
        exit(1)

    logger_slack.info("=== Update for report: using %s environment ===" % (base.db.environment,))
    try:
        update(
            continue_from=args.continue_from,
            filter_countries=args.filter_countries,
            date_from=args.date_from,
            date_to=args.date_to,
        )
        logger_slack.info("=== Update for report complete ===")
    except BaseException as e:
        logger_slack.error("=== Update for report failed", stack_info=True, exc_info=True)
        raise e
