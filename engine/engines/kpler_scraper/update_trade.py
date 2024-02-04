import datetime as dt
import logging
import warnings
from base.utils import to_datetime
from base.logger import logger, logger_slack
from .scraper_trade import KplerTradeScraper
from .upload import *

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


def update_trades(
    date_from=None,
    date_to=None,
    platforms=None,
    origin_iso2s=["RU"],
):
    scraper = KplerTradeScraper()
    date_from = to_datetime(date_from) if date_from is not None else dt.date(2020, 1, 1)
    date_to = to_datetime(date_to) if date_to is not None else dt.date.today()
    periods = pd.period_range(start=date_from, end=date_to, freq="M").astype(str)

    _platforms = scraper.platforms if platforms is None else platforms

    update_time = dt.datetime.now()

    with logging_redirect_tqdm(
        loggers=[logging.root, logger, logger_slack]
    ), warnings.catch_warnings():
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

                    upload_vessels(vessels)
                    upload_trades(trades, update_time=update_time)
                    upload_installations(installations)

                    del trades, vessels, products, installations

                logger.info(f"Finished updating trades for {platform} for country {from_iso2}")
