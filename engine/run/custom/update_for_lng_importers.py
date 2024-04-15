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
from engines.kpler_scraper.update import UpdateParts
from kpler.sdk import FlowsSplit, FlowsPeriod, FlowsMeasurementUnit

import integrity
import base
from base.logger import logger_slack

import datetime as dt

from argparse import ArgumentParser

import pandas as pd

COUNTRIES_TO_UPDATE = ["NL", "BE", "FR", "ES"]


def get_lng_sources(to_importers=None, date_from=None):
    scraper = KplerFlowScraper()

    all_flows = pd.DataFrame()

    for importer in to_importers:
        importer_flows = scraper.get_flows(
            date_from=date_from,
            destination_iso2=importer,
            granularity=FlowsPeriod.Annually,
            split=FlowsSplit.OriginCountries,
        )
        all_flows = pd.concat([all_flows, importer_flows])

    unqique_countries = all_flows["from_iso2"].unique()

    without_broken_values = filter(lambda x: x != None and x != "not found", unqique_countries)

    return sorted(list(without_broken_values))


def update():
    date_from = to_datetime("2021-01-01")

    lng_sources = get_lng_sources(to_importers=COUNTRIES_TO_UPDATE, date_from=date_from)

    countries_to_update = sorted(
        list(
            set.union(
                set(lng_sources),
                set(COUNTRIES_TO_UPDATE),
            )
        )
    )

    kpler_scraper.update(
        recent_date_from=date_from,
        recent_date_to=-1,
        origin_iso2s=countries_to_update,
        parts=[UpdateParts.UPDATE_RECENT_TRADES],
    )
    kpler_trade_computed.update()
    counter.update()
    return


if __name__ == "__main__":
    logger_slack.info(
        "=== Update for LNG report: using %s environment ===" % (base.db.environment,)
    )
    try:
        update()
        logger_slack.info("=== Update for LNG report complete ===")
    except BaseException as e:
        logger_slack.error("=== Update for LNG report failed", stack_info=True, exc_info=True)
        raise e
