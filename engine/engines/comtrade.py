from comtradeapicall import (
    getFinalDataAvailability,
    getFinalData,
    getReference,
    convertCountryIso3ToCode,
)

from base.env import get_env
from base.utils import to_datetime

import datetime as dt

import pandas as pd


from country_converter import CountryConverter


class ComtradeClient:
    """
    Client for the Comtrade API for monthly HS reported data. It normalises the data and returns it as a DataFrame.
    """

    def __init__(self, api_key):
        self.api_key = api_key
        self.cc = CountryConverter()

    def get_data_availability(self, start, end):

        months = pd.date_range(start, end, freq="M").strftime("%Y%m").tolist()
        period_argument = ",".join(months)

        data_availability = getFinalDataAvailability(
            subscription_key=self.api_key,
            typeCode="C",
            freqCode="M",
            clCode="HS",
            period=period_argument,
            reporterCode=None,
        )

        data_availability["period"] = pd.to_datetime(
            data_availability["period"], format="%Y%m"
        ).dt.to_period("M")

        # Parse last available as iso8601
        data_availability["lastReleased"] = pd.to_datetime(
            data_availability["lastReleased"], format="%Y-%m-%d"
        ).dt.date

        # Check that data_availability only has one dataset for each period and reporterISO
        assert data_availability.groupby(["period", "reporterISO"]).size().max() == 1

        # Select the reporter, period, and lastReleased
        data_availability = data_availability[["reporterISO", "period", "lastReleased"]]

        # Add missing values for the reporter and period combinations that are not in the data_availability
        all_reporters = data_availability["reporterISO"].unique()
        all_periods = data_availability["period"].unique()
        all_combinations = pd.MultiIndex.from_product(
            [all_reporters, all_periods], names=["reporterISO", "period"]
        )
        all_combinations = pd.DataFrame(all_combinations.to_frame().reset_index(drop=True))
        data_availability = (
            data_availability.set_index(["reporterISO", "period"])
            .reindex(all_combinations)
            .reset_index()
        )

        data_availability["lastReleased"] = data_availability["lastReleased"].where(
            data_availability["lastReleased"].notna(), None
        )

        data_availability["reporterISO"] = data_availability["reporterISO"].apply(
            lambda x: self.cc.convert(names=x, src="ISO3", to="ISO2")
        )

        # Filter rows with "not found" from data_availability
        data_availability = data_availability[
            data_availability["reporterISO"] != "not found"
        ].reindex()

        return data_availability
