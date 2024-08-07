import datetime as dt
from sqlalchemy import func
from sqlalchemy import nullslast
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from difflib import SequenceMatcher

import base
import json

from base.logger import logger
from base.encoder import JsonEncoder
from base.db import session
from base.models import (
    ShipInsurer,
    ShipOwner,
    ShipManager,
    ShipFlag,
    Company,
    Ship,
)

import country_converter as coco


def update_ship_core_details(imo, equasis_infos):

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

        flag = equasis_infos.get("current_flag")
        update_flag(imo, flag)
    else:
        logger.info("Failed to get response from equasis")


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
                session.query(ShipInsurer)
                .filter(ShipInsurer.ship_imo == imo, ShipInsurer.is_valid == True)
                .count()
                == 0
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
                ShipInsurer.is_valid == True,
            )
            .first()
        )

    # If it's an unknown insurer, we only want to find a matching one if it's the latest,
    # as we don't want to mark an old unknown period as updated recently.
    else:
        latest_insurers = (
            session.query(ShipInsurer.id)
            .filter(ShipInsurer.is_valid == True)
            .distinct(ShipInsurer.ship_imo)
            .order_by(ShipInsurer.ship_imo, nullslast(ShipInsurer.date_from_equasis.desc()))
            .subquery()
        )

        return (
            session.query(ShipInsurer)
            .join(latest_insurers, ShipInsurer.id == latest_insurers.c.id)
            .filter(
                ShipInsurer.ship_imo == ship_imo,
                ShipInsurer.company_raw_name == company_raw_name,
                ShipInsurer.is_valid == True,
            )
            .first()
        )


def update_ship_record_with_raw_equasis(
    ship_imo=None,
    equasis_infos=None,
):
    ship = session.query(Ship).filter(Ship.imo == ship_imo).first()

    if ship == None:
        ship = Ship(imo=ship_imo)
        session.add(ship)

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


def update_flag(imo, flag):

    flag_iso2 = coco.convert(names=flag, to="ISO2") if flag is not None else None
    latest_existing_flag = (
        session.query(ShipFlag)
        .filter(ShipFlag.imo == imo)
        .order_by(ShipFlag.updated_on.desc())
        .first()
    )
    update_time = dt.datetime.now()

    if not latest_existing_flag:
        # Add dummy record for history
        new_flag = ShipFlag(imo=imo, flag_iso2=flag_iso2, first_seen=None, updated_on=update_time)
        session.add(new_flag)
        session.commit()
        latest_existing_flag = new_flag

    if latest_existing_flag.flag_iso2 != flag_iso2 or latest_existing_flag.first_seen == None:
        # Add new record
        new_flag = ShipFlag(
            imo=imo, flag_iso2=flag_iso2, first_seen=update_time, updated_on=update_time
        )
        session.add(new_flag)
        session.commit()
    else:
        latest_existing_flag.updated_on = update_time
        session.commit()


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
        .filter(ShipInsurer.ship_imo == imo, ShipInsurer.is_valid == True)
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
