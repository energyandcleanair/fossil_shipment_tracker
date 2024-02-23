from base.db_utils import upsert
from engines import (
    kpler_scraper,
)

import base
from base.db import engine
from base.logger import logger

from sqlalchemy import text

import pandas as pd


def update():

    statement = """
      select origin_zone.country_iso2,
             platform,
             updated_on,
             date_trunc('day', min(departure_date_utc)) min_date_utc,
             date_trunc('day', max(departure_date_utc)) max_date_utc
        from kpler_trade join kpler_zone origin_zone on origin_zone.id = kpler_trade.departure_zone_id
       where is_valid
       group by origin_zone.country_iso2, platform, updated_on
       order by origin_zone.country_iso2, platform, updated_on;
    """
    # Returns a DataFrame with the results of the query, columns:
    # country_iso2, platform, updated_on, min_date_utc, max_date_utc
    result = pd.read_sql(statement, engine)

    # Get the minimum and maximum dates for each country and platform.
    date_range_per_country_platform = (
        result.groupby(by=["country_iso2", "platform"])
        .agg(min_date_utc=("min_date_utc", "min"), max_date_utc=("max_date_utc", "max"))
        .reset_index()
    )

    for index, row in date_range_per_country_platform.iterrows():
        country = row["country_iso2"]
        platform = row["platform"]
        min_date_utc = row["min_date_utc"]
        max_date_utc = row["max_date_utc"]

        logger.info(
            f"Updating sync history for {country} on {platform} from {min_date_utc} to {max_date_utc}"
        )

        results_for_country_platform = result[
            (result["country_iso2"] == country) & (result["platform"] == platform)
        ]

        # Make a date range from min_date_utc to max_date_utc
        date_range = [
            date for date in pd.date_range(start=min_date_utc, end=max_date_utc, freq="D")
        ]

        # Make a DataFrame with the columns:
        # - country_iso2: the country_iso2
        # - platform: the platform
        # - date: the date in the date_range
        # - last_updated: the maximum updated_on where the date falls between min_date_utc and max_date_utc (or None if there is no updated_on for that date)
        # - is_valid: False
        # - last_checked: None
        records = map(
            lambda date: {
                "country_iso2": country,
                "platform": platform,
                "date": date,
                "last_updated": results_for_country_platform[
                    (results_for_country_platform["min_date_utc"] <= date)
                    & (results_for_country_platform["max_date_utc"] >= date)
                ]["updated_on"].max(),
                "is_valid": False,
                "last_checked": None,
            },
            date_range,
        )

        records_to_update = pd.DataFrame.from_records(records).dropna(subset=["last_updated"])

        logger.info(f"Upserting {len(records_to_update)} records")

        upsert(
            records_to_update,
            table="kpler_sync_history",
            constraint_name="kpler_sync_history_unique",
        )

    return


if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
