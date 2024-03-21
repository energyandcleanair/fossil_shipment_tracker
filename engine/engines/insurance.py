import base

from tqdm import tqdm

import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy import nullslast

import pandas as pd

import datetime as dt

from base.db import session
from base.logger import logger_slack
from engines.insurance_scraper import *
from base.models import ShipInsurer

import warnings
from tqdm.contrib.logging import logging_redirect_tqdm
import logging

known_insurers = {
    22: WestOfEnglandInsuranceScraper(),
    4: StandardClubInsuranceScraper(),
    6: GardInsuranceScraper(),
    206: AmericanSteamshipInsuranceScraper(),
    # 1: SteamshipMutualUnderwritingInsuranceScraper(),
    # 11: NorthOfEnglandPiInsuranceScraper(),
    # 19: BritanniaSteamshipInsuranceScraper(),
    # 27: SkuldInsuranceScraper(),
    # 42: LondonPiInsuranceScraper(),
    # 109: SverigesAngfartysInsuranceScraper(),
}


def update():
    logger_slack.info("=== Update insurance dates ===")
    all_insurance_to_update = get_all_insurance_to_update()

    with logging_redirect_tqdm(loggers=[logging.root]), warnings.catch_warnings():
        for insurance_to_update in tqdm(
            all_insurance_to_update.itertuples(),
            total=all_insurance_to_update.shape[0],
            unit="ships",
        ):
            update_insurance(insurance_to_update)


def update_insurance(insurance):
    id = insurance.id
    imo = insurance.ship_imo
    company_id = insurance.company_id

    scraper = known_insurers[company_id]
    date = scraper.get_insurance_start_date_for_ship(imo)

    if date != None:
        db_insurance = session.query(ShipInsurer).filter(ShipInsurer.id == id).first()

        db_insurance.date_from_insurer = date
        db_insurance.updated_on_insurer = dt.datetime.now()
        session.add(db_insurance)
        session.commit()


def get_all_insurance_to_update():
    latest_insurance_subquery = (
        session.query(
            ShipInsurer.id,
            ShipInsurer.ship_imo,
            ShipInsurer.company_id,
            ShipInsurer.date_from_equasis,
            ShipInsurer.updated_on_insurer,
        )
        .distinct(ShipInsurer.ship_imo)
        .order_by(
            ShipInsurer.ship_imo,
            nullslast(ShipInsurer.date_from_equasis.desc()),
        )
        .subquery()
    )

    query_response_ships_to_update = (
        session.query(
            latest_insurance_subquery.c.id,
            latest_insurance_subquery.c.ship_imo,
            latest_insurance_subquery.c.company_id,
            latest_insurance_subquery.c.date_from_equasis,
        )
        .where(
            sa.and_(
                latest_insurance_subquery.c.company_id.in_(known_insurers.keys()),
                # We don't want to set the date where we want to preserve "from
                # the beginning of time" entries.
                latest_insurance_subquery.c.date_from_equasis != None,
                # We don't want to update entries that are too old. Use 1 year as a
                # cut off.
                latest_insurance_subquery.c.date_from_equasis
                > dt.datetime.now() - dt.timedelta(days=365),
                sa.or_(
                    latest_insurance_subquery.c.updated_on_insurer
                    < dt.datetime.now() - dt.timedelta(days=30),
                    latest_insurance_subquery.c.updated_on_insurer == None,
                ),
            )
        )
        .all()
    )

    insurance = pd.DataFrame(query_response_ships_to_update)
    return insurance
