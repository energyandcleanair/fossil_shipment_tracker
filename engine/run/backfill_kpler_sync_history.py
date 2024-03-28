import base
from base.logger import logger
from base.db import session

from base.models.kpler import KplerSyncHistory
from engines.kpler_scraper.verify import KplerTradeVerifier

from sqlalchemy import func
import argparse


def update(continue_from=None):

    range_for_country_platform = (
        session.query(
            KplerSyncHistory.country_iso2,
            KplerSyncHistory.platform,
            func.min(KplerSyncHistory.date).label("min_date"),
            func.max(KplerSyncHistory.date).label("max_date"),
        )
        .group_by(KplerSyncHistory.country_iso2, KplerSyncHistory.platform)
        .order_by(KplerSyncHistory.country_iso2, KplerSyncHistory.platform)
        .all()
    )

    verifier = KplerTradeVerifier()

    if continue_from:
        range_for_country_platform = [
            (country, platform, min_date, max_date)
            for country, platform, min_date, max_date in range_for_country_platform
            if country >= continue_from
        ]

    for country, platform, min_date, max_date in range_for_country_platform:
        logger.info(f"Verifying {country} {platform} from {min_date} to {max_date}")
        verifier.verify_sync_against_flows(
            origin_iso2s=[country],
            platforms=[platform],
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
