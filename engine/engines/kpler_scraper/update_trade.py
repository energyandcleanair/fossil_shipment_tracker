import datetime as dt
import logging
import warnings
from base.utils import to_datetime

from engines.kpler_scraper.checks_data_source import mark_updated
from .scraper_trade import KplerTradeScraper
from .upload import *

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from base.db import session


def update_trades(date_from=None, date_to=None, origin_iso2s=["RU"], update_time=dt.datetime.now()):
    scraper = KplerTradeScraper()
    date_from = to_datetime(date_from).date() if date_from is not None else dt.date(2020, 1, 1)
    date_to = to_datetime(date_to).date() if date_to is not None else dt.date.today()
    periods = pd.period_range(start=date_from, end=date_to, freq="M").astype(str)

    with logging_redirect_tqdm(loggers=[logging.root]), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for from_iso2 in tqdm(origin_iso2s, unit="from-iso", leave=False):
            for period in tqdm(periods, unit="month", leave=False):
                logger.info(f"Updating trades for country {from_iso2} in {period}")
                # To prevent memory issues, we do it one country at a time
                trades, vessels, products, installations = scraper.get_trades(
                    from_iso2=from_iso2, month=period
                )

                if not isinstance(trades, pd.DataFrame):
                    trades = pd.DataFrame(trades)

                logger.info(f"Uploading {len(vessels)} vessels for {from_iso2}, {period}")
                upload_vessels(vessels)
                logger.info(f"Uploading {len(trades)} trades for {from_iso2}, {period}")
                upload_trades(trades, update_time=update_time)
                logger.info(
                    f"Uploading {len(installations)} installations for {from_iso2}, {period}"
                )
                upload_installations(installations)

                logger.info(f"Marking scraper history complete for {from_iso2}, {period}")
                mark_updated(from_iso2, period, update_time)

                del trades, vessels, products, installations

            logger.info(f"Finished updating trades for country {from_iso2}")
