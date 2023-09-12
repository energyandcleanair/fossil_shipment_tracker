import base

import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy import nullslast

import pandas as pd

import datetime as dt

from base.db import session
from insurance_scraper import *
from base.models import ShipInsurer

known_insurers: dict[int, InsuranceScraper] = {
    22: WestOfEnglandInsuranceScraper(),
    # 6: GardInsuranceScraper(),
    # 4: CharlesTaylorInsuranceScraper(),
    # 1: SteamshipMutualUnderwritingInsuranceScraper(),
    # 11: NorthOfEnglandPiInsuranceScraper(),
    # 19: BritanniaSteamshipInsuranceScraper(),
    # 27: SkuldInsuranceScraper(),
    # 42: LondonPiInsuranceScraper(),
    # 109: SverigesAngfartysInsuranceScraper(),
    # 206: AmericanSteamshipInsuranceScraper(),
}

def update(imo=None):
    query_response_ships_to_update = (
        session
            .query(
                ShipInsurer.id,
                ShipInsurer.ship_imo,
                ShipInsurer.company_id,
                ShipInsurer.date_from,
            )
            .where(
                sa.and_(
                    ShipInsurer.company_id.in_(known_insurers.keys()),
                    # We don't want to set the date where we want to preserve "from
                    # the beginning of time" entries.
                    ShipInsurer.date_from != None,
                    # We don't want to update entries that are too old. Use 1 year as a
                    # cut off.
                    ShipInsurer.date_from > dt.datetime.now() - dt.timedelta(days = 365),
                    sa.or_(
                        ShipInsurer.updated_on_insurer < dt.datetime.now() - dt.timedelta(days = 30),
                        ShipInsurer.updated_on_insurer == None
                    )
                )
            )
            .distinct(
                ShipInsurer.ship_imo
            )
            .order_by(
                ShipInsurer.ship_imo,
                nullslast(ShipInsurer.date_from),
            )
            .all()
    )

    insurance = pd.DataFrame(query_response_ships_to_update)

    for insurance in insurance:
        id = insurance["id"]
        imo = insurance["ship_imo"]
        company_id = insurance["company_id"]

        scraper = known_insurers[company_id]
        date = scraper.get_imo_date(imo)

        if (date != None):
            db_insurance = (
                session
                    .query(ShipInsurer)
                    .filter(
                        ShipInsurer.id == id
                    )
                    .first()
            )

            db_insurance.date_from_insurance = date
            db_insurance.updated_on_insurer = dt.datetime.now()
            session.add(db_insurance)
            session.commit(db_insurance)
