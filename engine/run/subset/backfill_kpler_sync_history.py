import base
from base.logger import logger
from base.db import session

from base.models.kpler import KplerSyncHistory
from engines.kpler_scraper.verify import KplerTradeComparer

from sqlalchemy import func
import argparse


def update(continue_from=None):

    range_for_country = (
        session.query(
            KplerSyncHistory.country_iso2,
            func.min(KplerSyncHistory.date).label("min_date"),
            func.max(KplerSyncHistory.date).label("max_date"),
        )
        .group_by(KplerSyncHistory.country_iso2)
        .order_by(KplerSyncHistory.country_iso2)
        .all()
    )

    verifier = KplerTradeComparer()

    if continue_from:
        range_for_country = [
            (country, min_date, max_date)
            for country, min_date, max_date in range_for_country
            if country >= continue_from
        ]

    for country, min_date, max_date in range_for_country:
        logger.info(f"Verifying {country} from {min_date} to {max_date}")
        verifier.compare_sync_against_flows(
            origin_iso2s=[country],
            date_from=min_date,
            date_to=max_date,
        )

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--continue-from", help="Specify the country to continue from")
    args = parser.parse_args()

    print("=== Using %s environment ===" % (base.db.environment,))
    update(continue_from=args.continue_from)
