import datetime as dt
import logging
import warnings
from base.utils import to_datetime
from base.logger import logger, logger_slack
from base.models.kpler import KplerSyncHistory
from .scraper_trade import KplerTradeScraper
from .upload import *

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from base.db import session


def update_trades(
    date_from=None,
    date_to=None,
    platforms=None,
    origin_iso2s=["RU"],
):
    scraper = KplerTradeScraper()
    date_from = to_datetime(date_from).date() if date_from is not None else dt.date(2020, 1, 1)
    date_to = to_datetime(date_to).date() if date_to is not None else dt.date.today()
    periods = pd.period_range(start=date_from, end=date_to, freq="M").astype(str)

    _platforms = scraper.platforms if platforms is None else platforms

    update_time = dt.datetime.now()

    with logging_redirect_tqdm(loggers=[logging.root]), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for platform in tqdm(_platforms, unit="platform", leave=False):
            for from_iso2 in tqdm(origin_iso2s, unit="from-iso", leave=False):
                for period in tqdm(periods, unit="month", leave=False):
                    logger.info(
                        f"Updating trades for {platform} for country {from_iso2} in {period}"
                    )
                    # To prevent memory issues, we do it one country at a time
                    trades, vessels, products, installations = scraper.get_trades(
                        platform=platform, from_iso2=from_iso2, month=period
                    )

                    if not isinstance(trades, pd.DataFrame):
                        trades = pd.DataFrame(trades)

                    departure_date = pd.to_datetime(trades.departure_date_utc).dt.date

                    trades = trades[(departure_date >= date_from) & (departure_date <= date_to)]
                    logger.info(
                        f"Uploading {len(vessels)} vessels for {platform}, {from_iso2}, {period}"
                    )
                    upload_vessels(vessels)
                    logger.info(
                        f"Uploading {len(trades)} trades for {platform}, {from_iso2}, {period}"
                    )
                    upload_trades(trades, update_time=update_time)
                    logger.info(
                        f"Uploading {len(installations)} installations for {platform}, {from_iso2}, {period}"
                    )
                    upload_installations(installations)

                    logger.info(
                        f"Marking scraper history complete for {platform}, {from_iso2}, {period}"
                    )
                    mark_scraper_history(
                        platform, from_iso2, period, update_time, date_from, date_to
                    )

                    del trades, vessels, products, installations

                logger.info(f"Finished updating trades for {platform} for country {from_iso2}")


def mark_scraper_history(platform, from_iso2, period, update_time, date_from, date_to):
    days = [day for day in get_days_in_month(period) if day >= date_from and day <= date_to]

    records = [
        {
            "date": day,
            "platform": platform,
            "country_iso2": from_iso2,
            "last_updated": update_time,
            "is_valid": False,
            "last_checked": None,
        }
        for day in days
    ]

    df = pd.DataFrame.from_records(records)

    upsert(
        df,
        table=KplerSyncHistory.__tablename__,
        constraint_name="kpler_sync_history_unique",
        show_progress=False,
    )


def get_days_in_month(period_as_str):
    period = pd.Period(period_as_str, freq="M")
    days = pd.date_range(start=period.start_time.date(), end=period.end_time.date()).to_list()
    return days
