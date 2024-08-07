import pandas as pd
import datetime as dt
from sqlalchemy import nullslast, func, case
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.sql.functions import concat
import sqlalchemy as sa

import base
from base.utils import to_list
from base.db import session
from base.logger import logger, logger_slack
from base.models import (
    ShipInsurer,
    ShipOwner,
    ShipFlag,
    KplerVessel,
)

from .selector_base import build_filter_query, COMMODITY_SETTINGS


def select_ships_to_update_core_details(*, force_unknown: "bool", max_updates: int):

    logger.info("Finding the ships which need core detail updates")
    ships_to_update = find_all_ships_that_need_updates(force_unknown)

    if max_updates > 0 and len(ships_to_update) > max_updates:
        logger_slack.warn(
            f"Too many ship core details to update, limiting to {max_updates} ships. "
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
            ascending=[True, True, True],
        )
        .drop_duplicates(subset="imo", keep="first")
        .head(max_updates)
        .reset_index(drop=True)
    )

    return top_ships


def find_all_ships_that_need_updates(force_unknown):
    ships_to_update = pd.DataFrame()

    for commodity, settings in COMMODITY_SETTINGS.items():
        logger.info(f"Finding ships to update for {commodity}")
        known_update_period = settings["known"]
        unknown_update_period = settings["unknown"]

        ships_for_commodity = find_ships_by_commodity_that_need_updates(
            commodities=[commodity],
            known_update_period=known_update_period,
            unknown_update_period=unknown_update_period,
            force_unknown=force_unknown,
        )

        ships_to_update = pd.concat([ships_to_update, ships_for_commodity])
    return ships_to_update


def find_ships_by_commodity_that_need_updates(
    commodities: "list[str]" = None,
    known_update_period: int = 30,
    unknown_update_period: int = 3,
    force_unknown: bool = False,
):
    filter_query = build_filter_query()

    imo_query = (
        session.query(
            KplerVessel.imo,
            ShipInsurer.company_raw_name,
            func.coalesce(ShipInsurer.date_from_insurer, ShipInsurer.date_from_equasis).label(
                "date_from"
            ),
            ShipInsurer.checked_on,
            ShipInsurer.updated_on.label("last_updated"),
            ShipInsurer.consecutive_failures,
            ShipOwner.updated_on.label("last_updated_owner"),
            ShipFlag.updated_on.label("last_updated_flag"),
            ShipFlag.flag_iso2,
            filter_query.c.commodity,
            filter_query.c.priority,
        )
        .outerjoin(ShipInsurer, ShipInsurer.ship_imo == KplerVessel.imo)
        .outerjoin(ShipOwner, ShipOwner.ship_imo == KplerVessel.imo)
        .outerjoin(ShipFlag, ShipFlag.imo == KplerVessel.imo)
        .outerjoin(filter_query, filter_query.c.ship_imo == KplerVessel.imo)
        .filter(ShipInsurer.is_valid == True)
        .distinct(KplerVessel.imo)
        .order_by(
            KplerVessel.imo,
            nullslast(filter_query.c.priority.desc()),
            nullslast(ShipInsurer.updated_on.desc()),
            nullslast(ShipOwner.updated_on.desc()),
            nullslast(ShipFlag.updated_on.desc()),
        )
    )

    if commodities:
        imo_query = imo_query.filter(filter_query.c.commodity.in_(to_list(commodities)))

    imo_query = imo_query.subquery()

    # We use an exponential backoff to define the ignore period.
    # It will take a minimum of 256 days of checking to get to
    # the maximum of 90 days.
    backoff_base = 1.5
    backoff_limit = 90
    ignore_period = case(
        (
            func.power(backoff_base, imo_query.c.consecutive_failures) <= backoff_limit,
            func.power(backoff_base, imo_query.c.consecutive_failures),
        ),
        else_=backoff_limit,
    )
    ignore_date = func.now() - func.cast(concat(ignore_period, " DAYS"), INTERVAL)
    # This backoff period is only if we get a response from equasis
    # but that doesn't include any data
    not_in_backoff_period = sa.or_(
        imo_query.c.checked_on <= ignore_date, imo_query.c.checked_on == None
    )

    check_known_date = func.now() - func.cast(
        # Distribute the known a bit with a random to prevent a single day build up.
        concat(known_update_period - (func.random() * 6 - 3), " DAYS"),
        INTERVAL,
    )
    check_unknown_date = func.now() - func.cast(concat(unknown_update_period, " DAYS"), INTERVAL)

    not_yet_searched = sa.and_(imo_query.c.last_updated == None, imo_query.c.checked_on == None)

    unknown_and_needs_update = sa.and_(
        imo_query.c.last_updated <= check_unknown_date,
        imo_query.c.company_raw_name == base.UNKNOWN_INSURER,
    )

    known_and_needs_update = sa.and_(
        imo_query.c.last_updated <= check_known_date,
        imo_query.c.company_raw_name != base.UNKNOWN_INSURER,
    )

    expected_insurance_expiry_and_needs_update = sa.and_(
        imo_query.c.date_from <= dt.date.today() - dt.timedelta(days=11 * 30),
        imo_query.c.last_updated <= check_unknown_date,
        imo_query.c.company_raw_name != base.UNKNOWN_INSURER,
    )

    forced_unknown_update = sa.and_(
        force_unknown, imo_query.c.company_raw_name == base.UNKNOWN_INSURER
    )

    needs_update_insurance = sa.or_(
        not_yet_searched,
        unknown_and_needs_update,
        known_and_needs_update,
        expected_insurance_expiry_and_needs_update,
        forced_unknown_update,
    )

    # We do this to keep the distribution of updates more spread out.
    three_months_ish = func.cast(concat(func.random() * 30 - 15, " DAYS"), INTERVAL)

    needs_update_ship_info = sa.or_(
        imo_query.c.last_updated_owner == None,
        imo_query.c.last_updated_owner <= dt.date.today() - three_months_ish,
    )

    needs_update_ship_flag = sa.or_(
        imo_query.c.flag_iso2 == None,
        imo_query.c.last_updated_flag == None,
        imo_query.c.last_updated_flag <= dt.date.today() - three_months_ish,
    )

    needs_update = sa.or_(
        sa.and_(
            not_in_backoff_period,
            sa.or_(needs_update_insurance, needs_update_ship_info, needs_update_ship_flag),
        ),
    )

    imo_query = session.query(imo_query).filter(needs_update)

    imos_results = imo_query.all()

    results = pd.DataFrame(imos_results)

    if len(results) == 0:
        return pd.DataFrame()

    results = results[~results.imo.str.match("_v", case=False)]

    return results
