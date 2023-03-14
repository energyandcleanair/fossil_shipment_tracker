import pandas as pd
import numpy as np


def intersect(list1, list2):
    return list(set(list1) & set(list2))


def roll_average_kpler(kpler, rolling_days):
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
