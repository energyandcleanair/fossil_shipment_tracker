from enum import Enum
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


class ComtradeCommodities(Enum):
    COAL = "2701"
    COKE_AND_SEMI_COKE_OF_COAL = "2704"
    COAL_GAS = "2705"
    TAR_FROM_COAL = "2706"

    CRUDE_PETROLEUM_OILS = "2709"
    OIL_PRODUCTS = "2710"

    NATURAL_GAS = "2711"
    LIQUEFIED_NATURAL_GAS = "271111"
    GASEOUS_NATURAL_GAS = "271121"


class TypeCodes(Enum):
    COMMODITIES = "C"


class FrequencyCodes(Enum):
    MONTHLY = "M"


class ClassificationCodes(Enum):
    HARMONISED_SYSTEM = "HS"


class FlowCodes(Enum):
    IMPORTS = "M"
    EXPORTS = "X"


total_customs_procedure_code = "C00"
all_modes_of_supply_code = "0"  # All other codes for mode of supply are not supported


class ComtradeClient:
    """
    Client for the Comtrade API for monthly HS reported data. It normalises the data and returns it as a DataFrame.
    """

    def __init__(self, api_key):
        self.api_key = api_key
        self.cc = CountryConverter()

    def get_data_availability(self, *, start, end):
        """
        Get the HS data availability for the given period for all countries. It returns a DataFrame
        with the following columns:
        - reporter_iso: ISO2 code of the reporter country
        - period: Period of the data
        - last_released: Date when the data was last released
        """

        months = pd.date_range(start, end, freq="M").strftime("%Y%m").tolist()
        period_argument = ",".join(months)

        data_availability = getFinalDataAvailability(
            subscription_key=self.api_key,
            typeCode=TypeCodes.COMMODITIES.value,
            freqCode=FrequencyCodes.MONTHLY.value,
            clCode=ClassificationCodes.HARMONISED_SYSTEM.value,
            period=period_argument,
            reporterCode=None,
        )

        return self._clean_availability(data_availability)

    def _clean_availability(self, data_availability):
        data_availability["period"] = pd.to_datetime(
            data_availability["period"], format="%Y%m"
        ).dt.to_period("M")

        # Parse last available as iso8601
        data_availability["lastReleased"] = pd.to_datetime(
            data_availability["lastReleased"], format="%Y-%m-%d"
        ).dt.date

        # Check that data_availability only has one dataset for each period and reporterISO
        assert data_availability.groupby(["period", "reporterISO"]).size().max() == 1

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

        data_availability["last_released"] = data_availability["lastReleased"].where(
            data_availability["lastReleased"].notna(), None
        )

        data_availability["reporter_iso"] = data_availability["reporterISO"].apply(
            lambda x: self.cc.convert(names=x, src="ISO3", to="ISO2")
        )

        # Filter rows with "not found" from data_availability
        data_availability = data_availability[
            data_availability["reporter_iso"] != "not found"
        ].reindex()

        # Select the reporter, period, and lastReleased
        data_availability = data_availability[["reporter_iso", "period", "last_released"]]

        return data_availability

    def get_monthly_trades_for_periods(
        self, *, reporter: str, periods: list[pd.Period], commodities: list[ComtradeCommodities]
    ):
        """
        Get monthly imports for a list of periods and commodities for a single reporter. Will make
        multiple requests if there are more than 12 periods.
        The data is returned as a DataFrame with the following columns:
        - reporter_iso: ISO2 code of the reporter country
        - partner_iso: ISO2 code of the partner country
        - commodity_code: HS code of the commodity
        - flow_direction: "Imports" or "Exports"
        - period: Period of the data
        - quantity: Quantity of the commodity
        - quantity_unit: Unit of the quantity
        - value_usd: Value of the trade in USD
        """
        # Split periods into groups of 12
        periods_groups = [periods[i : i + 12] for i in range(0, len(periods), 12)]

        period_group_results = []
        for periods_group in periods_groups:
            period_group_results.append(
                self._get_monthly_imports_for_period_subset(
                    reporter=reporter, periods=periods_group, commodities=commodities
                )
            )

        joined_results = pd.concat(period_group_results)

        cleaned_results = self._clean_imports(joined_results)

        return cleaned_results

    def _get_monthly_imports_for_period_subset(
        self, *, reporter: str, periods: list[pd.Period], commodities: list[ComtradeCommodities]
    ):
        """
        Get monthly imports for a subset of periods and commodities for a single reporter.
        A maximum of 12 periods can be requested at a time.
        """
        if not all(isinstance(commodity, ComtradeCommodities) for commodity in commodities):
            raise ValueError("commodities must be a list of ComtradeCommodities")

        if len(periods) > 12:
            raise ValueError("periods must be less than or equal to 12")

        reporter_iso3 = self.cc.convert(names=reporter, src="ISO2", to="ISO3")
        reporter_code = convertCountryIso3ToCode(reporter_iso3)
        periods_as_str = ",".join([period.strftime("%Y%m") for period in periods])
        commodities_as_str = ",".join([commodity.value for commodity in commodities])
        flow_codes_as_str = ",".join([codes.value for codes in FlowCodes])

        data = getFinalData(
            subscription_key=self.api_key,
            typeCode=TypeCodes.COMMODITIES.value,
            freqCode=FrequencyCodes.MONTHLY.value,
            clCode=ClassificationCodes.HARMONISED_SYSTEM.value,
            flowCode=flow_codes_as_str,
            period=periods_as_str,
            reporterCode=reporter_code,
            cmdCode=commodities_as_str,
            partnerCode=None,
            partner2Code=None,
            customsCode=total_customs_procedure_code,
            motCode=all_modes_of_supply_code,
            includeDesc=True,
        )

        return data

    def _clean_imports(self, data: pd.DataFrame):

        converted_columns = [
            "reporter_iso",
            "partner_iso",
            "commodity_code",
            "flow_direction",
            "period",
            "quantity",
            "quantity_unit",
            "value_usd",
        ]

        if data.empty:
            return pd.DataFrame(columns=converted_columns)

        data["period"] = pd.to_datetime(data["period"], format="%Y%m").dt.to_period("M")

        data["reporter_iso"] = data["reporterISO"].apply(
            lambda x: self.cc.convert(names=x, src="ISO3", to="ISO2")
        )

        data["partner_iso"] = data["partnerISO"].apply(
            lambda x: self.cc.convert(names=x, src="ISO3", to="ISO2")
        )

        data["commodity_code"] = data["cmdCode"].astype(str)

        data["quantity"] = data["qty"]
        data["quantity_unit"] = data["qtyUnitAbbr"]

        data["value_usd"] = data["primaryValue"]

        data = data[(data["partner_iso"] != "not found") & (data["partner_iso"] != "W00")].reindex()

        data["flow_direction"] = data["flowDesc"]
        data = data[converted_columns]

        return data
