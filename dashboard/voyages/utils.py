import pandas as pd
import numpy as np


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
