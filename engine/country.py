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
    countries.rename(columns={'ISO2':'iso2','ISO3':'iso3',
                              'name_official':'name_official','name_short':'name'}, inplace=True)

    def to_region(row):
        if row.iso2 in ["US", "TR", "KR", "CN", "IN", "GB"]:
            return row["name"]
        if row.EU == "EU":
            return "EU"
        return "Others"

    def to_regions(row):
        regions = []
        if row.iso2 in ["US", "TR", "KR", "CN", "IN", "GB"]:
            regions.append(row["name"])
        if row.EU28 == "EU28":
            regions.append("EU28")
        if row.EU == "EU":
            regions.append("EU")
        if not regions:
            regions.append("Others")
        regions.append('Global')
        return regions

    countries["region"] = countries.apply(to_region, axis=1)
    countries["regions"] = countries.apply(to_regions, axis=1)

    countries = countries[['iso2', 'iso3', 'name', 'name_official', 'region', 'regions']]

    # Adding for_orders
    countries.loc[len(countries)] = [base.FOR_ORDERS, base.FOR_ORDERS, 'For orders', 'For orders', 'For orders', 'For orders']

    # Adding LNG
    countries.loc[len(countries)] = [base.LNG, base.LNG, 'LNG', 'LNG', 'LNG', 'LNG']

    upsert(countries, DB_TABLE_COUNTRY, 'unique_country')
    session.commit()
