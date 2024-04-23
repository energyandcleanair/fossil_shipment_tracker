from base.models.kpler import KplerVessel

from base.db import session
from base.logger import logger

import datetime as dt


def main():

    vessels_without_build_dates = (
        session.query(KplerVessel).filter(KplerVessel.build_date == None).all()
    )

    for vessel in vessels_without_build_dates:
        build_year = vessel.others["buildYear"]
        build_month = vessel.others["buildMonth"]
        if build_year and build_month:
            vessel.build_date = dt.date(build_year, build_month, 1)
        elif build_year and not build_month:
            vessel.build_date = dt.date(build_year, 1, 1)
        else:
            logger.info(f"Vessel {vessel.imo} has no build year")

    session.commit()


if __name__ == "__main__":
    main()
