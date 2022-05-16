import pandas as pd
import geopandas as gpd

from base.logger import logger
from base.db import session
from base.db_utils import upsert
from base.models import Country
from base.models import DB_TABLE_COUNTRY
import base


def fill():
    """
    Fill from countryconvert data
    :return:
    """

    import country_converter as coco
    cc = coco.CountryConverter()

    countries = cc.data
    countries.rename(columns={'ISO2':'iso2','ISO3':'iso3','name_official':'name_official','name_short':'name'}, inplace=True)

    def to_region(row):
        if row.iso2 in ["US", "TR", "KR", "CN", "IN"]:
            return row["name"]
        if row.EU28 == "EU28":
            return "EU28"
        return "Others"

    countries["region"] = countries.apply(to_region, axis=1)

    countries = countries[['iso2', 'iso3', 'name', 'name_official', 'region']]

    # Adding for_orders
    countries.loc[len(countries)] = [base.FOR_ORDERS, base.FOR_ORDERS, 'For orders', 'For orders', 'For orders']

    # Adding LNG
    countries.loc[len(countries)] = [base.LNG, base.LNG, 'LNG', 'LNG', 'LNG']

    upsert(countries, DB_TABLE_COUNTRY, 'unique_country')
    session.commit()
