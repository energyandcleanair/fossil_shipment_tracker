from country_converter import CountryConverter
from base.utils import to_datetime
from engines import (
    portcall,
    departure,
    arrival,
    shipment,
    position,
    ship,
    counter,
    alert,
    backuper,
    kpler_scraper,
    kpler_trade_computed,
)
from engines.kpler_scraper.scraper_flow import KplerFlowScraper
from engines.kpler_scraper.update import UpdateParts, UpdateStatus
from kpler.sdk import FlowsSplit, FlowsPeriod, FlowsMeasurementUnit

import integrity
import base
from base.logger import logger_slack, logger

import datetime as dt

from argparse import ArgumentParser

import pandas as pd


def update(countries=None, continue_from=None, date_from=None, date_to=None):
    date_from = to_datetime(date_from)
    date_to = to_datetime(date_to)

    countries_to_update = countries

    logger.info(f"Updating countries: {countries_to_update}")

    if continue_from is not None:
        countries_to_update = countries_to_update[countries_to_update.index(continue_from) :]

    result = kpler_scraper.update(
        date_from=date_from,
        date_to=date_to,
        origin_iso2s=countries_to_update,
        parts=[UpdateParts.TRADES, UpdateParts.VALIDATE],
        platforms=["liquids"],
    )

    if result == UpdateStatus.FAILED:
        raise RuntimeError("Unable to update Kpler trades, can't continue")

    kpler_trade_computed.update()
    counter.update()
    counter.update(version=base.COUNTER_VERSION1)
    counter.update(version=base.COUNTER_VERSION2)

    return


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--countries", nargs="*", type=str, default=None)
    parser.add_argument("--date-from", type=str, default=None)
    parser.add_argument("--date-to", type=str, default=None)
    parser.add_argument("--continue-from", type=str, default=None)

    args = parser.parse_args()

    if args.countries is None:
        print("Please specify --countries")
        exit(1)

    if args.date_from is None:
        print("Please specify --date-from")
        exit(1)

    if args.date_to is None:
        print("Please specify --date-to")
        exit(1)

    logger_slack.info("=== Update for report: using %s environment ===" % (base.db.environment,))
    try:
        update(
            countries=args.countries,
            continue_from=args.continue_from,
            date_from=args.date_from,
            date_to=args.date_to,
        )
        logger_slack.info("=== Update for report complete ===")
    except BaseException as e:
        logger_slack.error("=== Update for report failed", stack_info=True, exc_info=True)
        raise e
