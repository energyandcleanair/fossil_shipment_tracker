import pandas as pd
import datetime as dt
import geopandas as gpd
from sqlalchemy import func
from tqdm import tqdm

from base.logger import logger
from base.utils import to_datetime
from base.db import session
from base.db_utils import upsert
from base.models import Country, Currency
from base.models import DB_TABLE_CURRENCY
import base


def update(date_from=dt.date(2022, 1, 1),
           date_to=None,
           force=False):
    """
    Fill from countryconvert data
    :return:
    """

    from forex_python.converter import CurrencyRates
    c = CurrencyRates()

    last_date = session.query(func.max(Currency.date)).first()[0]
    if last_date and not force:
        date_from = max(date_from, last_date)

    tqdm.pandas()

    date_to = to_datetime(date_to) or dt.date.today()

    dates = pd.date_range(start=to_datetime(date_from), end=date_to)
    exchanges = pd.DataFrame(data={'date': dates})
    exchanges['per_eur'] = exchanges.progress_apply(lambda row: c.get_rates('EUR', row.date),
                                                axis=1)

    exchanges = pd.concat([exchanges.drop(['per_eur'], axis=1),
                           pd.json_normalize(exchanges['per_eur'])],
                           axis=1)

    exchanges['EUR'] = 1

    # Fill missing values
    exchanges.fillna(method='ffill', inplace=True)
    exchanges.fillna(method='bfill', inplace=True)

    exchanges = pd.melt(exchanges, id_vars='date', var_name='currency', value_name='per_eur')
    upsert(exchanges, DB_TABLE_CURRENCY, 'unique_currency')
    session.commit()
