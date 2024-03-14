import base
from base.logger import logger
from base.db import session

from base.models.kpler import KplerSyncHistory
from engines.kpler_scraper.verify import verify_sync_against_flows

from sqlalchemy import func


def update():

    range_for_country_platform = (
        session.query(
            KplerSyncHistory.country_iso2,
            KplerSyncHistory.platform,
            func.min(KplerSyncHistory.date).label("min_date"),
            func.max(KplerSyncHistory.date).label("max_date"),
        )
        .group_by(KplerSyncHistory.country_iso2, KplerSyncHistory.platform)
        .all()
    )

    for country, platform, min_date, max_date in range_for_country_platform:
        logger.info(f"Verifying {country} {platform} from {min_date} to {max_date}")
        verify_sync_against_flows(
            origin_iso2s=[country],
            platforms=[platform],
            date_from=min_date,
            date_to=max_date,
        )

    return


if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
