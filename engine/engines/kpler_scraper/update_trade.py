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
    ignore_if_copy_failed=False,
):
    scraper = KplerTradeScraper()
    date_from = to_datetime(date_from) if date_from is not None else dt.date(2020, 1, 1)
    date_to = to_datetime(date_to) if date_to is not None else dt.date.today()

    _platforms = scraper.platforms if platforms is None else platforms

    with logging_redirect_tqdm(
        loggers=[logging.root, logger, logger_slack]
    ), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for platform in tqdm(_platforms, unit="platform"):
            for from_iso2 in tqdm(origin_iso2s, position=1, unit="from-iso"):
                # To prevent memory issues, we do it one country at a time
                trades, vessels, zones, products, installations = scraper.get_trades(
                    platform=platform, from_iso2=from_iso2, date_from=date_from
                )

                # upload_products(products, ignore_if_copy_failed=ignore_if_copy_failed)
                upload_zones(zones, ignore_if_copy_failed=ignore_if_copy_failed)
                upload_vessels(vessels, ignore_if_copy_failed=ignore_if_copy_failed)
                upload_trades(trades, ignore_if_copy_failed=ignore_if_copy_failed)
                upload_installations(installations, ignore_if_copy_failed=ignore_if_copy_failed)
