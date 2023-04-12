import pandas as pd
import numpy as np

from . import COUNTRY_GLOBAL


def intersect(list1, list2):
    return list(set(list1) & set(list2))


def roll_average_insurance(insurance, rolling_days):
    daterange = pd.date_range(min(insurance.date), max(insurance.date)).rename("date")
    data = insurance.copy()
    data["date"] = pd.to_datetime(data["date"])
    data = (
        data.groupby(
            intersect(
                [
                    "commodity_origin_iso2",
                    "commodity_origin_country",
                    "commodity_origin_region",
                    "commodity_destination_iso2",
                    "commodity_destination_country",
                    "commodity_destination_region",
                    "insurer_owner_region",
                    "pricing_scenario",
                    "commodity_group_name",
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
    from_countries = pd.read_csv("insurance/assets/from_countries.csv")
    options = (
        from_countries[["iso2", "country"]]
        .rename(columns={"iso2": "value", "country": "label"})
        .to_dict("records")
    )
    return options


def get_to_countries():
    to_countries = pd.read_csv("insurance/assets/to_countries.csv", keep_default_na=False)
    options = (
        to_countries[["iso2", "country"]]
        .rename(columns={"iso2": "value", "country": "label"})
        .to_dict("records")
    )
    return [{"label": "Global", "value": COUNTRY_GLOBAL}] + options


def add_insurer_owner_region(df):
    def recode_eug7(ship_owner_region, ship_owner_iso2, ship_insurer_region, ship_insurer_iso2):
        g7 = ["CA", "FR", "DE", "IT", "JP", "GB", "US"]
        res = np.where(
            (ship_owner_region == "EU")
            | ship_owner_iso2.isin(g7)
            | (ship_insurer_region == "EU")
            | ship_insurer_iso2.isin(g7),
            "Owned and / or insured in EU & G7",
            np.where(
                ship_insurer_iso2 == "NO",
                "Insured in Norway",
                np.where(pd.isna(ship_owner_iso2), "Unknown", "Others"),
            ),
        )
        return res

    df["insurer_owner_region"] = recode_eug7(
        df.ship_owner_region,
        df.ship_owner_iso2,
        df.ship_insurer_region,
        df.ship_insurer_iso2,
    )

    return df
