from enum import Enum
import math
import random
import time
from typing import Optional
from tqdm import tqdm
from base.env import get_env
from base.logger import logger, logger_slack
from engines.company_scraper import Equasis

import warnings
from tqdm.contrib.logging import logging_redirect_tqdm
import logging


from engines.company_scraper.equasis import EquasisSessionPool, EquasisSessionPoolExhausted

from engines.ship_details_datasource import (
    select_ships_to_update_inspections,
    select_ships_to_update_core_details,
    update_ship_core_details,
    update_ships_inspections,
    clean_ship_details,
)

DEFAULT_UPDATE_LIMIT: int = int(get_env("EQUASIS_UPDATE_LIMIT", 1000))

global_equasis_client: Equasis | None = None


# We're using a singleton pattern here:
# - We want a single Equasis client per application as it's expensive to generate.
# - We do not want to create the client when this python file is imported because the client might
#   not be used.
def get_global_equasis_client() -> Equasis:
    global global_equasis_client
    if global_equasis_client is None:
        global_equasis_client = Equasis(session_pool=EquasisSessionPool.with_account_generator())
    return global_equasis_client


def clear_global_equasis_client():
    global global_equasis_client
    global_equasis_client = None


class ComtradeUpdateStatus(Enum):
    """
    Enum to represent the status of a Comtrade update.
    """

    SUCCESS = "success"
    EQUASIS_EXHAUSTED_FAILURE = "equasis_exhuasted_failure"
    UNEXPECTED_ERROR = "unexpected_error"


class ComtradeUpdateSteps(Enum):
    SHIP_INFO = "SHIP_INFO"
    SHIP_INSPECTIONS = "SHIP_INSPECTIONS"
    CLEAN_DATA = "CLEAN_DATA"


def update(
    force_unknown=False,
    max_updates: int = DEFAULT_UPDATE_LIMIT,
    steps: list[ComtradeUpdateSteps] = [step.value for step in ComtradeUpdateSteps],
) -> ComtradeUpdateStatus:
    """
    This function updates the company information in the database from Equasis and insurers.
    @param force_unknown: whether to force update of unknown insurers
    @param max_updates: maximum number of ships to update, defaults to environment variable
    `EQUASIS_UPDATE_LIMIT`. negative value means no limit
    """
    logger_slack.info("=== Company update ===")
    # For crude oil and oil products, force a daily refresh
    # given the importance for price caps and bans

    try:
        if ComtradeUpdateSteps.SHIP_INFO in steps:
            update_info_from_equasis(
                force_unknown=force_unknown,
                max_updates=math.floor(max_updates / 2),
            )
        if ComtradeUpdateSteps.SHIP_INSPECTIONS in steps:
            update_ships_inspections_from_equasis(
                max_updates=math.floor(max_updates / 2),
            )
        if ComtradeUpdateSteps.CLEAN_DATA in steps:
            clean_ship_details()
        logger_slack.info("=== Company update done ===")
        return ComtradeUpdateStatus.SUCCESS
    except EquasisSessionPoolExhausted as e:
        logger_slack.error("=== Company update failed: Equasis session pool exhausted ===")
        return ComtradeUpdateStatus.EQUASIS_EXHAUSTED_FAILURE
    except Exception as e:
        logger_slack.error("=== Company update failed ===", stack_info=True, exc_info=True)
        return ComtradeUpdateStatus.UNEXPECTED_ERROR


def update_ships_inspections_from_equasis(max_updates: Optional[int] = DEFAULT_UPDATE_LIMIT / 2):
    ships_to_update = select_ships_to_update_inspections(max_updates=max_updates)

    equasis = get_global_equasis_client()

    with logging_redirect_tqdm(loggers=[logging.root]), warnings.catch_warnings():
        for _, row in tqdm(ships_to_update.iterrows(), unit="ships"):
            imo = row["imo"]
            logger.info(f"Updating inspections for {imo}")

            random_wait()

            inspection_info = equasis.get_inspections(imo=imo)

            if inspection_info is not None:
                update_ships_inspections(imo, inspection_info)


def update_info_from_equasis(
    *,
    force_unknown: "bool",
    max_updates: int,
):
    """
    Collect infos from equasis about shipments that either don't have infos,
    or for infos that are potentially outdated
    :return:
    """

    top_ships = select_ships_to_update_core_details(
        force_unknown=force_unknown, max_updates=max_updates
    )

    if len(top_ships) == 0:
        logger.info(f"No ships to update")
        return

    imos_to_update = top_ships.imo.unique().tolist()

    equasis = get_global_equasis_client()

    logger.info(f"Updating {len(imos_to_update)} ships from Equasis")

    with logging_redirect_tqdm(loggers=[logging.root]), warnings.catch_warnings():
        for imo in tqdm(imos_to_update, unit="ships"):
            imo_equasis = imo.replace("NOTFOUND_", "")
            equasis_infos = equasis.get_ship_infos(imo=imo_equasis)

            random_wait()

            logger.info(
                f"Details from equasis to update in database for {imo_equasis}: {equasis_infos}"
            )

            update_ship_core_details(imo, equasis_infos)


def random_wait():
    time.sleep(random.uniform(0.25, 0.5))
