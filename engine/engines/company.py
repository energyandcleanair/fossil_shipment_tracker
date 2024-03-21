from typing import TypedDict
import requests.exceptions
from tqdm import tqdm
import pandas as pd
import datetime as dt
from sqlalchemy import func
from sqlalchemy import nullslast
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.sql.functions import concat
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from difflib import SequenceMatcher

import base
import json

from base.db_utils import execute_statement
from base.encoder import JsonEncoder
from base.utils import to_list
from base.db import session
from base.env import get_env
from base.logger import logger, logger_slack
from base.models import (
    Commodity,
    Departure,
    ShipInsurer,
    ShipOwner,
    ShipManager,
    Company,
    Country,
    KplerProduct,
    KplerTrade,
    KplerZone,
    Ship,
    Port,
)
from engines.company_scraper import Equasis, CompanyImoScraper

import warnings
from tqdm.contrib.logging import logging_redirect_tqdm
import logging


UPDATE_LIMIT: int = 500


def update(force_unknown=False):
    logger_slack.info("=== Company update ===")
    # For crude oil and oil products, force a daily refresh
    # given the importance for price caps and bans

    try:

        commodity_settings = {
            base.CRUDE_OIL: {"known": 30, "unknown": 3, "update_priority": 0},
            base.OIL_PRODUCTS: {"known": 30, "unknown": 3, "update_priority": 0},
            base.OIL_OR_CHEMICAL: {"known": 30, "unknown": 3, "update_priority": 0},
            base.LNG: {"known": 30, "unknown": 3, "update_priority": 1},
            base.COAL: {"known": 30, "unknown": 15, "update_priority": 2},
            base.BULK: {"known": 30, "unknown": 15, "update_priority": 2},
            base.LPG: {"known": 30, "unknown": 3, "update_priority": 3},
        }

        update_info_from_equasis(
            commodity_settings=commodity_settings,
            force_unknown=force_unknown,
        )

        fill_country()
        logger_slack.info("=== Company update done ===")
    except Exception as e:
        logger_slack.error("=== Company update failed ===", stack_info=True, exc_info=True)
        raise e
    return


def find_or_create_company_id(raw_name, imo=None, address=None):
    """
    The function checks whether we have a company which matches the name or exactly and has same imo, if not
    we attempt to create a record, and if there is imo conflict we double-check name similarity is close

    Parameters
    ----------
    raw_name : name of the company
    imo : optional imo of the company
    address : optional address of the company

    Returns
    -------

    """
    company_sq = session.query(
        Company.id, Company.imo, func.unnest(Company.names).label("name")
    ).subquery()
    existing_company = (
        session.query(company_sq)
        .filter(company_sq.c.name == raw_name, sa.or_(imo is None, company_sq.c.imo == imo))
        .first()
    )

    if existing_company:
        company_id = existing_company.id
    else:
        new_company = Company(
            imo=imo,
            name=raw_name,
            names=[raw_name],
            address=address,
            addresses=[address],
        )
        session.add(new_company)
        try:
            session.commit()
            company_id = new_company.id
        except sa.exc.IntegrityError:
            session.rollback()
            existing_company = session.query(Company).filter(Company.imo == imo).first()
            ratio = SequenceMatcher(None, existing_company.name, raw_name).ratio()
            if ratio > 0.9:
                company_id = existing_company.id
            else:
                logger.warning(
                    "Inconsistency: %s != %s (IMO=%s)" % (existing_company.name, raw_name, imo)
                )
                company_id = None

    return company_id


def build_filter_query():

    commodity_id_field = (
        "kpler_"
        + sa.func.replace(
            sa.func.replace(
                sa.func.lower(func.coalesce(KplerProduct.commodity_name, KplerProduct.group_name)),
                " ",
                "_",
            ),
            "/",
            "_",
        )
    ).label("commodity")

    kpler_ships = (
        session.query(
            func.unnest(KplerTrade.vessel_imos).label("ship_imo"),
            KplerTrade.departure_date_utc.label("date_utc"),
            Commodity.equivalent_id.label("commodity"),
            sa.sql.expression.literal("kpler").label("source"),
        )
        .outerjoin(KplerProduct, KplerTrade.product_id == KplerProduct.id)
        .outerjoin(Commodity, commodity_id_field == Commodity.id)
    )

    return kpler_ships.subquery()


class CommoditySettings(TypedDict):
    known: int
    unknown: int


def update_info_from_equasis(
    commodity_settings: "dict[str, CommoditySettings]" = None,
    force_unknown: "bool" = False,
):
    """
    Collect infos from equasis about shipments that either don't have infos,
    or for infos that are potentially outdated
    :return:
    """

    top_ships = get_ships_to_update(commodity_settings, force_unknown)

    if len(top_ships) == 0:
        commodities = ",".join(commodity_settings.keys())
        logger.info(f"No ships to update for {commodities}")
        return

    imos_to_update = top_ships.imo.unique().tolist()

    equasis = Equasis()

    with logging_redirect_tqdm(
        loggers=[logging.root, logger, logger_slack]
    ), warnings.catch_warnings():
        for imo in tqdm(imos_to_update, unit="ships"):
            imo_equasis = imo.replace("NOTFOUND_", "")
            equasis_infos = equasis.get_ship_infos(imo=imo_equasis)

            logger.info(
                f"Details from equasis to update in database for {imo_equasis}: {equasis_infos}"
            )

            if equasis_infos is not None:
                # Update ship record
                update_ship_record_with_raw_equasis(imo, equasis_infos)

                equasis_insurers = equasis_infos.get("insurers")
                update_ship_insurer(imo, equasis_insurers)

                # Manager
                manager_info = equasis_infos.get("manager")
                update_ship_manager(imo, manager_info)

                # Owner
                owner_info = equasis_infos.get("owner")
                update_ship_owner(imo, owner_info)
            else:
                logger.info("Failed to get response from equasis")


def get_ships_to_update(commodity_settings: "dict[str, CommoditySettings]", force_unknown: "bool"):

    logger.info("Finding the ships to update")
    ships_to_update = pd.DataFrame()

    for commodity, settings in commodity_settings.items():

        logger.info(f"Finding ships to update for {commodity}")
        known_update_period = settings["known"]
        unknown_update_period = settings["unknown"]

        ships_for_commodity = find_ships_that_need_updating(
            commodities=[commodity],
            known_update_period=known_update_period,
            unknown_update_period=unknown_update_period,
            force_unknown=force_unknown,
        )

        ships_to_update = pd.concat([ships_to_update, ships_for_commodity])

    if len(ships_to_update) > UPDATE_LIMIT:
        logger_slack.warn(
            f"Too many ships to update, limiting to {UPDATE_LIMIT} ships. "
            + f"It will take {len(ships_to_update) / UPDATE_LIMIT} iterations to update all ships. "
            + f"Prioritising by commodity type's priority, fewest consecutive failures, then oldest checked."
        )

    commodity_settings_df = pd.DataFrame.from_dict(commodity_settings, orient="index").reset_index(
        names=["commodity"]
    )

    top_ships = (
        ships_to_update.merge(commodity_settings_df, left_on="commodity", right_on="commodity")
        .sort_values(
            by=["update_priority", "consecutive_failures", "checked_on"],
            na_position="first",
            ascending=[True, True, True],
        )
        .drop_duplicates(subset="imo", keep="first")
        .head(UPDATE_LIMIT)
        .reset_index(drop=True)
    )

    return top_ships


def find_ships_that_need_updating(
    commodities: "list[str]" = None,
    known_update_period: int = 30,
    unknown_update_period: int = 3,
    force_unknown: bool = False,
):
    filter_query = build_filter_query()

    imo_query = (
        session.query(
            Ship.imo,
            ShipInsurer.company_raw_name,
            func.coalesce(ShipInsurer.date_from_insurer, ShipInsurer.date_from_equasis).label(
                "date_from"
            ),
            ShipInsurer.checked_on,
            ShipInsurer.updated_on.label("last_updated"),
            ShipInsurer.consecutive_failures,
            filter_query.c.commodity,
        )
        .outerjoin(ShipInsurer, ShipInsurer.ship_imo == Ship.imo)
        .outerjoin(filter_query, filter_query.c.ship_imo == Ship.imo)
        .distinct(filter_query.c.ship_imo)
        .order_by(filter_query.c.ship_imo, nullslast(ShipInsurer.updated_on.desc()))
    )

    if commodities:
        imo_query = imo_query.filter(filter_query.c.commodity.in_(to_list(commodities)))

    imo_query = imo_query.subquery()

    from sqlalchemy.sql import case

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

    needs_update = sa.and_(
        not_in_backoff_period,
        sa.or_(
            not_yet_searched,
            unknown_and_needs_update,
            known_and_needs_update,
            expected_insurance_expiry_and_needs_update,
            forced_unknown_update,
        ),
    )

    imo_query = session.query(imo_query).filter(needs_update)

    imos_results = imo_query.all()

    results = pd.DataFrame(imos_results)

    if len(results) == 0:
        return pd.DataFrame()

    results = results[~results.imo.str.match("_v", case=False)]

    return results


def update_ship_owner(imo, owner_info):
    if owner_info:
        owner_raw_name = owner_info.get("name")
        owner_address = owner_info.get("address")
        owner_imo = owner_info.get("imo")
        owner_date_from = owner_info.get("date_from")

        # See if exists
        owner = (
            session.query(ShipOwner)
            .filter(
                ShipOwner.company_raw_name == owner_raw_name,
                ShipOwner.ship_imo == imo,
                ShipOwner.date_from == owner_date_from,
            )
            .first()
        )
        if not owner:
            owner = ShipOwner(
                company_raw_name=owner_raw_name,
                ship_imo=imo,
                imo=owner_imo,
                date_from=owner_date_from,
                company_id=find_or_create_company_id(
                    raw_name=owner_raw_name,
                    imo=owner_imo,
                    address=owner_address,
                ),
            )
        owner.updated_on = dt.datetime.now()

        # Verify we DID find a matching company_id using find_or_create_company_id otherwise we will have an
        # integrity error
        if owner.company_id is not None:
            session.add(owner)
            session.commit()
        else:
            logger.warning(
                "Failed to find/create company_id for company {}, ship_imo {}.".format(
                    owner.company_raw_name, owner.ship_imo
                )
            )


def update_ship_manager(imo, manager_info):
    if manager_info:
        manager_raw_name = manager_info.get("name")
        manager_address = manager_info.get("address")
        manager_imo = manager_info.get("imo")
        manager_date_from = manager_info.get("date_from")

        # See if exists
        manager = (
            session.query(ShipManager)
            .filter(
                ShipManager.company_raw_name == manager_raw_name,
                ShipManager.imo == manager_imo,
                ShipManager.ship_imo == imo,
                ShipManager.date_from == manager_date_from,
            )
            .first()
        )
        if not manager:
            manager = ShipManager(
                company_raw_name=manager_raw_name,
                ship_imo=imo,
                imo=manager_imo,
                date_from=manager_date_from,
                company_id=find_or_create_company_id(
                    raw_name=manager_raw_name,
                    imo=manager_imo,
                    address=manager_address,
                ),
            )
        manager.updated_on = dt.datetime.now()
        session.add(manager)
        session.commit()


def update_ship_insurer(imo, equasis_insurers):
    if equasis_insurers:
        for equasis_insurer in equasis_insurers:
            insurer_raw_name = equasis_insurer.get("name")
            insurer_raw_date_from = (
                equasis_insurer.get("date_from")
                if equasis_insurer.get("date_from")
                else dt.datetime.now()
            )

            # If it's the first insurer, we enter an empty date_from insurer first.
            first_time_insurer = (
                session.query(ShipInsurer).filter(ShipInsurer.ship_imo == imo).count() == 0
            )
            if first_time_insurer:
                insert_first_time_insurer(imo, insurer_raw_name)

            insurer = get_matching_insurer(
                ship_imo=imo, company_raw_name=insurer_raw_name, date_from=insurer_raw_date_from
            )

            insurer_already_exists = bool(insurer)
            new_record_for_ship = not insurer_already_exists
            consecutive_unknowns = (
                insurer_already_exists
                and insurer.company_raw_name == base.UNKNOWN_INSURER
                and insurer_raw_name == base.UNKNOWN_INSURER
            )
            would_overwrite_null_date_from = (
                # If there was an insurer but it has an unknown date_from_equasis
                insurer_already_exists
                and insurer.date_from_equasis == None
            )

            if consecutive_unknowns:
                update_failed_insurer(imo, insurer)
                logger.info(f"Multiple consecutive unknown insurer {imo}, marking as checked")

            elif new_record_for_ship or would_overwrite_null_date_from:
                insert_new_insurer_record(
                    imo, company_name=insurer_raw_name, date_from=insurer_raw_date_from
                )

            else:
                update_insurer(
                    insurer=insurer,
                    imo=imo,
                    insurer_raw_name=insurer_raw_name,
                    insurer_raw_date_from=insurer_raw_date_from,
                )

    else:
        logger.info("Couldn't find insurers for %s, marking as checked" % (imo))

        insurer = get_latest_insurer(imo)
        if not insurer:
            insurer = create_unknown_insurer(imo)

        update_failed_insurer(imo, insurer)


def insert_first_time_insurer(imo, insurer_raw_name):
    # If this is the first time we collect insurer for this ship,
    # We assume it has always been this insurer
    # This is important because we only start querying a ship insurer
    # After we had a departure with it, and so the first insurer
    # would always be after the first departure otherwise
    empty_date_from_insurer = build_new_insurer(
        ship_imo=imo, company_raw_name=insurer_raw_name, company_raw_date_from=None
    )
    update_insurer(
        insurer=empty_date_from_insurer,
        imo=imo,
        insurer_raw_name=insurer_raw_name,
        insurer_raw_date_from=None,
    )


def insert_new_insurer_record(imo, company_name=None, date_from=None):
    update_insurer(
        insurer=build_new_insurer(
            ship_imo=imo,
            company_raw_name=company_name,
            company_raw_date_from=date_from,
        ),
        imo=imo,
        insurer_raw_name=company_name,
        insurer_raw_date_from=date_from,
    )


def get_matching_insurer(ship_imo=None, company_raw_name=None, date_from=None):
    # If it's not an unknown insurer, we want to find an exact match on date and name
    # so we don't update old ones.
    if company_raw_name != base.UNKNOWN_INSURER:
        return (
            session.query(ShipInsurer)
            .filter(
                ShipInsurer.ship_imo == ship_imo,
                ShipInsurer.company_raw_name == company_raw_name,
                ShipInsurer.date_from_equasis == date_from,
            )
            .first()
        )

    # If it's an unknown insurer, we only want to find a matching one if it's the latest,
    # as we don't want to mark an old unknown period as updated recently.
    else:
        latest_insurers = (
            session.query(ShipInsurer.id)
            .distinct(ShipInsurer.ship_imo)
            .order_by(ShipInsurer.ship_imo, nullslast(ShipInsurer.date_from_equasis.desc()))
            .subquery()
        )

        return (
            session.query(ShipInsurer)
            .join(latest_insurers, ShipInsurer.id == latest_insurers.c.id)
            .filter(
                ShipInsurer.ship_imo == ship_imo, ShipInsurer.company_raw_name == company_raw_name
            )
            .first()
        )


def update_ship_record_with_raw_equasis(
    ship_imo=None,
    equasis_infos=None,
):
    ship = session.query(Ship).filter(Ship.imo == ship_imo).first()
    others = dict(ship.others) if ship.others else {}
    others.update({"equasis": equasis_infos})
    # To convert datetimes to str
    others = json.loads(json.dumps(others, cls=JsonEncoder))
    ship.others = others
    session.commit()


def build_new_insurer(ship_imo=None, company_raw_name=None, company_raw_date_from=None):
    return ShipInsurer(
        company_raw_name=company_raw_name,
        imo=None,
        ship_imo=ship_imo,
        company_id=find_or_create_company_id(raw_name=company_raw_name),
        date_from_equasis=company_raw_date_from,
    )


def update_failed_insurer(imo, insurer):
    insurer.checked_on = dt.datetime.now()
    if insurer.consecutive_failures == None:
        insurer.consecutive_failures = 0

    insurer.consecutive_failures += 1
    try:
        session.add(insurer)
        session.commit()
    except IntegrityError as e:
        session.rollback()
        logger.warning(
            "Failed to update insurer checked date for ship %s" % (imo),
            stack_info=True,
            exc_info=True,
        )


def create_unknown_insurer(imo):
    unknown_insurer = build_new_insurer(ship_imo=imo, company_raw_name=base.UNKNOWN_INSURER)
    unknown_insurer.updated_on = None
    unknown_insurer.checked_on = dt.datetime.now()
    try:
        session.add(unknown_insurer)
        session.commit()
    except IntegrityError as e:
        session.rollback()
        logger.warning(
            "Failed to create unknown insurer checked date for ship %s" % (imo),
            stack_info=True,
            exc_info=True,
        )

    # reset updated on
    unknown_insurer.updated_on = None
    try:
        session.add(unknown_insurer)
        session.commit()
    except IntegrityError as e:
        session.rollback()
        logger.warning(
            "Failed to reset updated on date for unknown insurer checked date for ship %s" % (imo),
            stack_info=True,
            exc_info=True,
        )

    return unknown_insurer


def get_latest_insurer(imo):
    return (
        session.query(ShipInsurer)
        .filter(ShipInsurer.ship_imo == imo)
        .distinct(ShipInsurer.ship_imo)
        .order_by(ShipInsurer.ship_imo, nullslast(ShipInsurer.updated_on.desc()))
        .first()
    )


def update_insurer(imo=None, insurer_raw_name=None, insurer_raw_date_from=None, insurer=None):
    insurer.updated_on = dt.datetime.now()
    insurer.checked_on = dt.datetime.now()
    insurer.consecutive_failures = 0

    trying_to_update_null_date = (
        insurer_raw_date_from is not None and insurer.date_from_equasis is None
    )

    # This shouldn't happen but we want to guarantee that.
    assert not (trying_to_update_null_date)

    session.add(insurer)
    try:
        session.commit()
    except IntegrityError as e:
        session.rollback()
        logger.warning("Failed to add insurer %s for ship %s" % (insurer_raw_name, imo))


def fill_country():
    """
    This function uses regex and company name/address to attempt to fill in country_iso2 and registration_country_iso2

    Returns
    -------

    """

    def fill_using_country_ending():
        """
        We check the ending of company address using regex to see if we can determine iso2

        Returns
        -------

        """
        country_regex = session.query(
            Country.iso2,
            ("[\.| |,|_|-|/]{1}" + Country.name + "[\.]?$").label("regexp"),
        ).subquery()
        update = (
            Company.__table__.update()
            .values(country_iso2=country_regex.c.iso2)
            .where(
                sa.and_(
                    Company.country_iso2 == sa.null(),
                    Company.address.op("~*")(country_regex.c.regexp),
                )
            )
        )
        execute_statement(update)

    def fill_using_address_regexps():
        """
        Using address to estimate country_iso2
        which might be different from registration_country_iso2

        Returns
        -------

        """
        address_regexps = {
            "US": ["USA[\.]?$"],
            "SG": ["Singapore [0-9]*$"],
            "TW": ["\(Taiwan\)[\.]?"],
            "PT": ["Madeira[\.]?$"],
            "HK": ["Hong Kong, China[\.]?[\w]*[0-9]*"],
            "IM": ["Isle of Man"],
            "JE": ["Jersey"],
        }

        for key, regexps in address_regexps.items():
            condition = sa.or_(*[Company.address.op("~")(regexp) for regexp in regexps])
            update = Company.__table__.update().values(country_iso2=key).where(condition)
            execute_statement(update)

    def fill_using_name_regexps():
        """
        This is for insurers. Assuming country == registration_country

        Returns
        -------

        """
        name_regexps = {
            "BM": ["\(Bermuda\)$"],
            "GB": [
                "Britannia Steamship insurance Association Ld",
                "North of England P&I Association",
                "UK P&I Club",
                "The London P&I Club",
                "The West of  England Shipowners",
                "Standard P&I Club per Charles Taylor & Co",
            ],
            "LU": ["The Ship owners' Mutual P&I Association \(Luxembourg\)"],
            "JP": ["Japan Ship Owners' P&I Association"],
            "NO": ["Norway$", "^Hydor AS$"],
            "SE": ["\(Swedish Club\)$"],
            "US": ["American Steamship Owner P&I association$"],
            "NL": ["Noord Nederlandsche P&I Club$"],
            "RU": ["VSK Insurance Company"],
        }

        for key, regexps in name_regexps.items():
            condition = sa.and_(
                Company.address == sa.null(),
                sa.or_(*[Company.name.op("~")(regexp) for regexp in regexps]),
            )
            update = (
                Company.__table__.update()
                .values(country_iso2=key, registration_country_iso2=key)
                .where(condition)
            )
            execute_statement(update)

    def remove_care_of():
        to_remove = ["^Care of"]
        condition = sa.and_(sa.or_(*[Company.address.op("~")(regexp) for regexp in to_remove]))
        update = Company.__table__.update().values(country_iso2=sa.null()).where(condition)
        execute_statement(update)

    def fill_using_file():
        """
        Manual listing of companies registriation countries

        Returns
        -------

        """
        companies_df = pd.read_csv("assets/companies.csv", dtype={"imo": str})
        companies_df = companies_df.dropna(subset=["imo", "registration_iso2"])
        imo_country = dict(zip(companies_df.imo, companies_df.registration_iso2))
        from sqlalchemy.sql import case

        session.query(Company).filter(Company.imo.in_(imo_country)).update(
            {
                Company.registration_country_iso2: case(
                    imo_country,
                    value=Company.imo,
                )
            },
            synchronize_session="fetch",
        )
        session.commit()

        # For those without imo
        companies_df = pd.read_csv("assets/companies.csv", dtype={"imo": str})
        companies_df = companies_df[pd.isna(companies_df.imo)]
        companies_df = companies_df.dropna(subset=["name", "registration_iso2"])
        name_country = dict(zip(companies_df.name, companies_df.registration_iso2))
        from sqlalchemy.sql import case

        session.query(Company).filter(
            Company.name.in_(name_country), Company.imo == sa.null()
        ).update(
            {
                Company.registration_country_iso2: case(
                    name_country,
                    value=Company.name,
                )
            },
            synchronize_session="fetch",
        )
        session.commit()

    fill_using_country_ending()
    fill_using_address_regexps()
    fill_using_name_regexps()
    # remove_care_of()
    fill_using_file()
    fill_using_imo_website()


def fill_using_imo_website():
    """
    Query companies with missing registration ISO2 and fill it in if found

    Returns
    -------

    """
    scraper = CompanyImoScraper(base_url=base.IMO_BASE_URL, service=None)

    scraper.initialise_browser(headless=True)

    if not scraper.perform_login(get_env("IMO_USER"), get_env("IMO_PASSWORD")):
        return False

    db_countries = dict(session.query(Country.name, Country.iso2).all())

    # some countries from IMO website are not the same as standard/official names in our db, so let's add them
    additional_countries = {
        "USA": "US",
        "United States of America": "US",
        "China, People's Republic of": "CN",
        "Korea, South": "KR",
        "Korea, North": "KP",
        "Virgin Islands, British": "VI",
        "Singapore": "SG",
        "Canary Islands": "ES",
        "Kyrgyzstan": "KG",
        "Taiwan": "TW",
        "Chinese Taipei": "TW",
        "Hong Kong, China": "HK",
        "Madeira": "PT",
        "St Kitts & Nevis": "KN",
        "Antigua & Barbuda": "AG",
        "Irish Republic": "IE",
        "St Vincent & The Grenadines": "VC",
    }

    country_dict = {**db_countries, **additional_countries}

    companies = (
        session.query(Company)
        .filter(sa.and_(Company.registration_country_iso2 == sa.null(), Company.imo != sa.null()))
        .all()
    )

    for company in tqdm(companies, unit="companies"):
        # check imo website for company imo or name
        company_info = scraper.get_information(search_text=str(company.imo))

        if company_info is None or len(company_info) > 1:
            logger.warning(
                "Company not found, or more than one company with this search term ({}), skipping...".format(
                    company.imo
                )
            )
            continue

        company_info = company_info[0]
        # add reg iso2 to record and commit
        try:
            company.registration_country_iso2 = country_dict[company_info[0]]
            session.commit()
        except KeyError:
            logger.warning(
                "We did not find the ISO2 for imo {}, country {}. Considering adding manually.".format(
                    company.imo, company_info[0]
                )
            )
        except IndexError:
            logger.warning(
                "Failed to parse correct information from IMO website for {}.".format(company.imo)
            )
