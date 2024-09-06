from datetime import date
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


from engines.company_scraper.accounts import EquasisAccountCreatorError
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


class EquasisUpdateStatus(Enum):
    """
    Enum to represent the status of a Comtrade update.
    """

    SUCCESS = "success"
    EQUASIS_EXHAUSTED_FAILURE = "equasis_exhuasted_failure"
    UNABLE_TO_CREATE_ACCOUNTS_FAILURE = "unable_to_create_accounts_failure"
    UNEXPECTED_ERROR = "unexpected_error"


class EquasisUpdateSteps(Enum):
    SHIP_INFO = "SHIP_INFO"
    SHIP_INSPECTIONS = "SHIP_INSPECTIONS"
    CLEAN_DATA = "CLEAN_DATA"


def update(
    *,
    force_unknown: bool = False,
    max_updates: int = DEFAULT_UPDATE_LIMIT,
    steps: list[EquasisUpdateSteps] = [step for step in EquasisUpdateSteps],
    filter_departing_iso2s: Optional[list[str]] = None,
    filter_minimum_departure_date: Optional[date] = None,
    refresh_accounts_between_steps: bool = True,
) -> EquasisUpdateStatus:
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
        if EquasisUpdateSteps.SHIP_INFO in steps:
            result = update_info_from_equasis(
                force_unknown=force_unknown,
                max_updates=math.floor(max_updates / 2),
                filter_departing_iso2s=filter_departing_iso2s,
                filter_minimum_departure_date=filter_minimum_departure_date,
            )

            if result.status == EquasisStepCompletionStatus.EQUASIS_EXHAUSTED_FAILURE:
                logger.warn(str(result))
                return EquasisUpdateStatus.EQUASIS_EXHAUSTED_FAILURE
            else:
                logger.info(str(result))

        if refresh_accounts_between_steps:
            clear_global_equasis_client()

        if EquasisUpdateSteps.SHIP_INSPECTIONS in steps:
            result = update_ships_inspections_from_equasis(
                max_updates=math.floor(max_updates / 2),
                filter_departing_iso2s=filter_departing_iso2s,
                filter_minimum_departure_date=filter_minimum_departure_date,
            )

            if result.status == EquasisStepCompletionStatus.EQUASIS_EXHAUSTED_FAILURE:
                logger.warn(str(result))
                return EquasisUpdateStatus.EQUASIS_EXHAUSTED_FAILURE
            else:
                logger.info(str(result))

        if EquasisUpdateSteps.CLEAN_DATA in steps:
            clean_ship_details()
        logger_slack.info("=== Company update done ===")
        return EquasisUpdateStatus.SUCCESS
    except EquasisAccountCreatorError as e:
        logger_slack.error("=== Company update failed ===", stack_info=True, exc_info=True)
        return EquasisUpdateStatus.UNABLE_TO_CREATE_ACCOUNTS_FAILURE
    except Exception as e:
        logger_slack.error("=== Company update failed ===", stack_info=True, exc_info=True)
        return EquasisUpdateStatus.UNEXPECTED_ERROR


class EquasisStepCompletionStatus(Enum):
    SUCCESS = "success"
    EQUASIS_EXHAUSTED_FAILURE = "equasis_exhuasted_failure"


class EquasisStepSyncResults:
    def __init__(
        self,
        *,
        n_checked: int,
        n_updated: int,
        max_updates: int,
        status: EquasisStepCompletionStatus,
    ):
        self.n_checked = n_checked
        self.n_updated = n_updated
        self.max_updates = max_updates
        self.status = status

    def __str__(self):
        return f"Completed sync with status {self.status}: checked {self.n_checked}, updated {self.n_updated}, max updates {self.max_updates}"


def update_ships_inspections_from_equasis(
    max_updates: Optional[int] = DEFAULT_UPDATE_LIMIT / 2,
    filter_departing_iso2s: Optional[list[str]] = None,
    filter_minimum_departure_date: Optional[date] = None,
) -> EquasisStepSyncResults:
    ships_to_update = select_ships_to_update_inspections(
        max_updates=max_updates,
        filter_departing_iso2s=filter_departing_iso2s,
        filter_minimum_departure_date=filter_minimum_departure_date,
    )

    equasis = get_global_equasis_client()

    n_checked = 0
    n_updated = 0
    try:
        with logging_redirect_tqdm(loggers=[logging.root]), warnings.catch_warnings():
            for _, row in tqdm(ships_to_update.iterrows(), unit="ships"):
                imo = row["imo"]
                logger.info(f"Updating inspections for {imo}")

                random_wait()

                inspection_info = equasis.get_inspections(imo=imo)
                n_checked += 1

                if inspection_info is not None:
                    update_ships_inspections(imo, inspection_info)
                    n_updated += 1

    except EquasisSessionPoolExhausted:
        return EquasisStepSyncResults(
            n_checked=n_checked,
            n_updated=n_updated,
            max_updates=max_updates,
            status=EquasisStepCompletionStatus.EQUASIS_EXHAUSTED_FAILURE,
        )

    return EquasisStepSyncResults(
        n_checked=n_checked,
        n_updated=n_updated,
        max_updates=max_updates,
        status=EquasisStepCompletionStatus.SUCCESS,
    )


def update_info_from_equasis(
    *,
    force_unknown: "bool",
    max_updates: int,
    filter_departing_iso2s: Optional[list[str]] = None,
    filter_minimum_departure_date: Optional[date] = None,
) -> EquasisStepSyncResults:
    """
    Collect infos from equasis about shipments that either don't have infos,
    or for infos that are potentially outdated
    :return:
    """

    top_ships = select_ships_to_update_core_details(
        force_unknown=force_unknown,
        max_updates=max_updates,
        filter_departing_iso2s=filter_departing_iso2s,
        filter_minimum_departure_date=filter_minimum_departure_date,
    )

    if len(top_ships) == 0:
        logger.info(f"No ships to update")
        return

    imos_to_update = top_ships.imo.unique().tolist()

    equasis = get_global_equasis_client()

    logger.info(f"Updating {len(imos_to_update)} ships from Equasis")

    n_checked = 0
    n_updated = 0
    try:
        with logging_redirect_tqdm(loggers=[logging.root]), warnings.catch_warnings():
            for imo in tqdm(imos_to_update, unit="ships"):
                imo_equasis = imo.replace("NOTFOUND_", "")
                equasis_infos = equasis.get_ship_infos(imo=imo_equasis)
                n_checked += 1

                random_wait()

                logger.info(
                    f"Details from equasis to update in database for {imo_equasis}: {equasis_infos}"
                )

                update_ship_core_details(imo, equasis_infos)
                n_updated += 1

    except EquasisSessionPoolExhausted:
        return EquasisStepSyncResults(
            n_checked=n_checked,
            n_updated=n_updated,
            max_updates=max_updates,
            status=EquasisStepCompletionStatus.EQUASIS_EXHAUSTED_FAILURE,
        )
    return EquasisStepSyncResults(
        n_checked=n_checked,
        n_updated=n_updated,
        max_updates=max_updates,
        status=EquasisStepCompletionStatus.SUCCESS,
    )


def random_wait():
    time.sleep(random.uniform(0.25, 0.5))
