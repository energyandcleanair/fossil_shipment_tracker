import pandas as pd
import datetime as dt
from sqlalchemy import nullslast, func
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.sql.functions import concat
import sqlalchemy as sa

import base
from base.utils import to_list
from base.db import session
from base.logger import logger, logger_slack
from base.models import (
    ShipInspection,
    KplerVessel,
)

from .selector_base import build_filter_query, COMMODITY_SETTINGS


def select_ships_to_update_inspections(*, max_updates: int):

    logger.info("Finding the ships which need inspection updates")
    ships_to_update = find_all_ships_that_need_updates()

    if max_updates > 0 and len(ships_to_update) > max_updates:
        logger_slack.warn(
            f"Too many ship inspections to update, limiting to {max_updates} ships. "
            + f"It will take {len(ships_to_update) / max_updates} iterations to update all ships. "
            + f"Prioritising most important updates."
        )
        return limit_ships_to_update(ships_to_update, max_updates)
    else:
        return ships_to_update.drop_duplicates(subset="imo", keep="first").reset_index(drop=True)


def limit_ships_to_update(ships_to_update, max_updates: int):
    commodity_settings_df = pd.DataFrame.from_dict(COMMODITY_SETTINGS, orient="index").reset_index(
        names=["commodity"]
    )

    top_ships = (
        ships_to_update.merge(commodity_settings_df, left_on="commodity", right_on="commodity")
        .sort_values(
            by=[
                "commodity_update_priority",
                "history_update_priority",
                "consecutive_failures",
                "checked_on",
            ],
            na_position="first",
            ascending=[True, False, True, True],
        )
        .drop_duplicates(subset="imo", keep="first")
        .head(max_updates)
        .reset_index(drop=True)
    )

    return top_ships


def find_all_ships_that_need_updates():
    ships_to_update = pd.DataFrame()

    for commodity, _ in COMMODITY_SETTINGS.items():
        logger.info(f"Finding ships to update for {commodity}")
        ships_for_commodity = find_ships_by_commodity_that_need_updates(
            commodities=[commodity],
        )

        ships_to_update = pd.concat([ships_to_update, ships_for_commodity])
    return ships_to_update


def find_ships_by_commodity_that_need_updates(
    commodities: "list[str]" = None,
):
    filter_query = build_filter_query()

    imo_query = (
        session.query(
            KplerVessel.imo,
            ShipInspection.updated_on.label("last_updated"),
            filter_query.c.commodity,
            filter_query.c.priority.label("history_update_priority"),
        )
        .outerjoin(ShipInspection, ShipInspection.ship_imo == KplerVessel.imo)
        .outerjoin(filter_query, filter_query.c.ship_imo == KplerVessel.imo)
        .distinct(KplerVessel.imo)
        .order_by(
            KplerVessel.imo,
            nullslast(ShipInspection.updated_on.desc()),
        )
    )

    if commodities:
        imo_query = imo_query.filter(filter_query.c.commodity.in_(to_list(commodities)))

    imo_query = imo_query.subquery()

    # We do this to keep the distribution of updates more spread out.
    three_months_ish = func.cast(concat(func.random() * 30 - 15, " DAYS"), INTERVAL)
    three_months_ago_ish = dt.datetime.now() - three_months_ish

    # We only want to update ships that haven't been updated in the last three months
    needs_update = sa.or_(
        imo_query.c.last_updated == None, imo_query.c.last_updated < three_months_ago_ish
    )

    imo_query = session.query(imo_query).filter(needs_update)

    imos_results = imo_query.all()

    results = pd.DataFrame(imos_results)

    if len(results) == 0:
        return pd.DataFrame()

    results = results[~results.imo.str.match("_v", case=False)]

    return results
