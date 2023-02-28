import pandas as pd
import numpy as np

palette = {
    "China": "#990000",
    "EU": "#8cc9D0",
    "India": "#f6b26b",
    "United States": "#35416C",
    "Turkey": "#27a59c",
    "For orders": "#FFF2CC",
    "Others": "#cacaca",
    "United Kingdom": "#741b47",
    "Unknown": "#333333",
    "Russia": "#660000",
    "United Arab Emirates": "#741b47",
    "South Korea": "#351c75",
    "Coal": "#351c75",
    "LNG": "#f6b26b",
    "Pipeline gas": "#f6b26b80",
    "Gas": "#f6b26b",
    "Crude oil": "#741b47",
    "Oil": "#741b47",
    "Oil products and chemicals": "#741b4760",
}


def intersect(list1, list2):
    return list(set(list1) & set(list2))


def roll_average_counter(counter, rolling_days):
    daterange = pd.date_range(min(counter.date), max(counter.date)).rename("date")
    counter = counter.copy()
    counter = (
        counter.groupby(
            intersect(
                [
                    "commodity",
                    "commodity_name",
                    "commodity_group",
                    "commodity_group_name",
                    "destination_iso2",
                    "destination_country",
                    "destination_region",
                    "currency",
                    "pricing_scenario",
                    "pricing_scenario_name",
                ],
                counter.columns,
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
    return counter


def roll_average_voyage(voyages, rolling_days, value):
    daterange = pd.date_range(min(voyages.date), max(voyages.date)).rename("date")
    voyages = voyages.copy()
    voyages["date"] = pd.to_datetime(voyages["date"])
    groupby_cols = list(set(voyages.columns) - set(["date", value]))

    voyages = (
        voyages.groupby(groupby_cols, dropna=False)
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
    return voyages
