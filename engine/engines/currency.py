from itertools import product
import pandas as pd
import datetime as dt
import geopandas as gpd
from sqlalchemy import func
import sqlalchemy as sa
from tqdm import tqdm

from base.logger import logger
from base.utils import to_datetime
from base.logger import logger_slack
from base.db import session
from base.db_utils import upsert
from base.models import Country, Currency
from base.models import DB_TABLE_CURRENCY
import base

from currency_converter import CurrencyConverter, ECB_URL

from urllib.request import urlretrieve


def update(date_from=dt.date(2022, 1, 1), date_to=None, force=False):
    """
    Fill from countryconvert data
    :return:
    """

    date_from = to_datetime(choose_date_from(date_from, force))
    date_to = to_datetime(date_to) or dt.date.today() - dt.timedelta(days=1)

    logger_slack.info("=== Currency update ===")

    try:

        converter = build_converter()

        currencies = get_currency_names(converter, date_to)
        dates = pd.date_range(start=date_from, end=date_to)

        exchanges = get_exchange_rates(converter, currencies, dates)

        upsert(exchanges, DB_TABLE_CURRENCY, "unique_currency")
        session.commit()

    except Exception as e:
        logger_slack.error(
            f"Currency update failed",
            stack_info=True,
            exc_info=True,
        )


def get_exchange_rates(converter, currencies, dates):
    exchanges = pd.DataFrame(list(product(dates, currencies)), columns=["date", "currency"])

    exchanges["per_eur"] = exchanges.progress_apply(
        lambda row: converter.convert(
            amount=1, currency="EUR", new_currency=row.currency, date=row.date
        ),
        axis=1,
    )
    exchanges["estimated"] = False
    return exchanges


def get_currency_names(converter, date_to):
    currencies = [
        currency
        for (currency, bounds) in converter.bounds.items()
        if bounds.last_date > date_to - dt.timedelta(days=7)
    ]

    return currencies


def choose_date_from(date_from, force):
    last_date = (
        session.query(func.max(Currency.date)).filter(sa.not_(Currency.estimated)).first()[0]
    )
    if last_date and not force:
        date_from = max(date_from, last_date)
    return date_from


def build_converter():
    filename = "cache/ecb.zip"
    urlretrieve(ECB_URL, filename)

    converter = CurrencyConverter(
        filename, fallback_on_missing_rate=True, fallback_on_wrong_date=True
    )

    return converter
