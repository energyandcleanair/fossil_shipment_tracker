import pandas as pd
import datetime as dt
import geopandas as gpd
from sqlalchemy import func
from tqdm import tqdm

from base.logger import logger
from base.utils import to_datetime
from base.db import session
from base.db_utils import upsert
from base.models import Country, CurrencyExchange
from base.models import DB_TABLE_CURRENCYEXCHANGE
import base


def update(date_from=dt.date(2022,1,1), force=False):
    """
    Fill from countryconvert data
    :return:
    """

    from forex_python.converter import CurrencyRates
    c = CurrencyRates()

    last_date = session.query(func.max(CurrencyExchange.date)).first()[0]
    if last_date and not force:
        date_from = max(date_from, last_date)

    tqdm.pandas()

    dates = pd.date_range(start=to_datetime(date_from), end=dt.date.today())
    exchanges = pd.DataFrame(data={'date':dates})
    exchanges['eur'] = exchanges.progress_apply(lambda row: c.get_rates('EUR', row.date),
                                                axis=1)

    fields = {
        'usd_per_eur': 'USD',
        'gbp_per_eur': 'GBP',
        'jpy_per_eur': 'JPY',
        'krw_per_eur': 'KRW',
        'php_per_eur': 'PHP'
    }

    for f in fields:
        exchanges[f] = exchanges.apply(lambda row: row.eur.get(fields[f]), axis=1)

    cols = set(CurrencyExchange.__dict__.keys()) & set(exchanges.columns)

    upsert(exchanges[cols], DB_TABLE_CURRENCYEXCHANGE, 'unique_currencyexchange')
    session.commit()
