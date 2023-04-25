import pandas as pd
import numpy as np

from . import COUNTRY_GLOBAL


def intersect(list1, list2):
    return list(set(list1) & set(list2))


def roll_average_kpler(kpler, rolling_days):
    daterange = pd.date_range(min(kpler.date), max(kpler.date)).rename("date")
    data = kpler.copy()
    data["date"] = pd.to_datetime(data["date"])
    data = (
        data.groupby(
            intersect(
                [
                    "origin_iso2",
                    "origin_country",
                    "origin_region",
                    "origin_type",
                    "origin_name",
                    "destination_iso2",
                    "destination_country",
                    "destination_region",
                    "destination_type",
                    "destination_name",
                    "product",
                    "product_group",
                    "product_family",
                    "pricing_scenario",
                    "commodity",
                    "commodity_equivalent_name",
                ],
                data.columns,
            ),
            dropna=False,
        )
        .apply(
            lambda x: x.set_index("date")
            .resample("D")
            .sum(numeric_only=True)
            .reindex(daterange)
            .fillna(0)
            .rolling(rolling_days, min_periods=rolling_days)
            .mean()
        )
        .reset_index()
        .replace({np.nan: None})
    )
    return data


def to_list(d, convert_tuple=False):
    if d is None:
        return []
    if convert_tuple and isinstance(d, tuple):
        return list(d)
    if not isinstance(d, list):
        return [d]
    else:
        return d


def to_options(d):
    return [{"label": v["label"], "value": k} for k, v in d.items()]


def get_from_countries():
    from_countries = pd.read_csv("kpler/assets/from_countries.csv")
    options = (
        from_countries[["iso2", "country"]]
        .rename(columns={"iso2": "value", "country": "label"})
        .to_dict("records")
    )
    return options


def get_to_countries():
    to_countries = pd.read_csv("kpler/assets/to_countries.csv", keep_default_na=False)
    options = (
        to_countries[["iso2", "country"]]
        .rename(columns={"iso2": "value", "country": "label"})
        .to_dict("records")
    )
    return [{"label": "Global", "value": COUNTRY_GLOBAL}] + options
