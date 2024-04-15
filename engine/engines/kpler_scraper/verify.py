import numpy as np
from tqdm import tqdm
from base.db import session
from base.logger import logger

from kpler.sdk import FlowsSplit, FlowsPeriod, FlowsMeasurementUnit
from base.models.kpler import KplerProduct, KplerSyncHistory, KplerTrade, KplerZone
from base.utils import to_datetime
from base.models import DB_TABLE_KPLER_SYNC_COMPARISON_DETAILS

from engines.kpler_scraper.scraper_flow import KplerFlowScraper

from sqlalchemy import func
from sqlalchemy.orm import aliased

import pandas as pd
import datetime as dt

TOLERANCE_FOR_VALUE_ERROR_FOR_DAY_COUNTRY = 0.05
TOLERANCE_FOR_VALUE_ERROR_FOR_DAY_PRODUCT = 0.05
TOLERANCE_FOR_VALUE_ERROR_FOR_WHOLE_DAY = 0.05
JOINT_PROBLEM_COUNT_THRESHOLD = 3

RECENT_PERIOD_FOR_ERROR_CHECKING = 90
TOLERANCE_FOR_VALUE_ERROR_IN_RECENT_PERIOD_SUM = 0.05


class KplerTradeVerifier:
    def __init__(self):
        self.scraper = KplerFlowScraper()

    def verify_sync_against_flows(self, origin_iso2s=None, date_from=None, date_to=None):
        date_from = to_datetime(date_from) if date_from is not None else dt.date(2020, 1, 1)
        date_to = to_datetime(date_to) if date_to is not None else dt.date.today()
        if isinstance(date_from, dt.datetime):
            date_from = date_from.date()
        if isinstance(date_to, dt.datetime):
            date_to = date_to.date()

        checked_time = dt.datetime.now()

        comparisons = pd.DataFrame()

        for country in origin_iso2s:
            logger.info(f"Verifying {country} from {date_from} to {date_to}")
            comparison, comparison_details = self.compare_to_live_flows(
                origin_iso2=country, date_from=date_from, date_to=date_to
            )

            comparison["country"] = country

            self.update_sync_history_with_status(
                origin_iso2=country,
                date_from=date_from,
                date_to=date_to,
                comparison=comparison,
                checked_time=checked_time,
            )

            self.update_sync_comparison_details(
                origin_iso2=country,
                comparison_details=comparison_details,
                checked_time=checked_time,
            )

            comparisons = pd.concat([comparisons, comparison])

        failed_comparisons = comparisons[~comparisons["ok"]]
        if (comparisons["ok"] == False).any():
            logger.warn(f"Some comparisons failed:\n{failed_comparisons.to_string()}")

        return

    def compare_to_live_flows(self, origin_iso2=None, date_from=None, date_to=None):

        comparison_per_dest = self.compare_to_live_flows_for_dest(origin_iso2, date_from, date_to)
        comparison_per_product = self.compare_to_live_flows_for_product(
            origin_iso2, date_from, date_to
        )

        dest_comparison = self._aggregate_to_check_period(comparison_per_dest)
        product_comparison = self._aggregate_to_check_period(comparison_per_product)

        return self.summarise_comparisons(
            dest_comparison, product_comparison
        ), self.combine_comparison_details(comparison_per_dest, comparison_per_product)

    def combine_comparison_details(self, comparison_per_dest, comparison_per_product):

        comparison_per_dest = comparison_per_dest.rename(columns={"country_iso2": "factor"})
        comparison_per_product = comparison_per_product.rename(columns={"product": "factor"})

        comparison_per_dest["factor_type"] = "dest"
        comparison_per_product["factor_type"] = "product"

        return pd.concat([comparison_per_dest, comparison_per_product]).rename(
            columns={
                "value_tonne.expected": "value_tonne_expected",
                "value_tonne.actual": "value_tonne_actual",
            }
        )

    def summarise_comparisons(self, dest_comparison, product_comparison):
        comparison = pd.merge(
            dest_comparison,
            product_comparison,
            on="departure_day",
            how="outer",
            suffixes=(".dest", ".product"),
        )

        comparison["problems"] = comparison["problems.dest"] + comparison["problems.product"]
        comparison["value_tonne.expected"] = (
            comparison["value_tonne.expected.dest"] + comparison["value_tonne.expected.product"]
        ) / 2
        comparison["value_tonne.actual"] = (
            comparison["value_tonne.actual.dest"] + comparison["value_tonne.actual.product"]
        ) / 2

        # Allows for each day to have a small number of problems as long as the total closely matches.
        comparison["ok"] = (comparison["problems"] < JOINT_PROBLEM_COUNT_THRESHOLD) | np.isclose(
            comparison["value_tonne.expected"],
            comparison["value_tonne.actual"],
            rtol=TOLERANCE_FOR_VALUE_ERROR_FOR_WHOLE_DAY,
        )

        return comparison

    def update_sync_history_with_status(
        self,
        origin_iso2=None,
        date_from=None,
        date_to=None,
        comparison=None,
        checked_time=None,
    ):
        query_for_history_entries = session.query(KplerSyncHistory).filter(
            KplerSyncHistory.country_iso2 == origin_iso2,
            KplerSyncHistory.date >= date_from,
            KplerSyncHistory.date <= date_to,
        )

        history_entries = query_for_history_entries.all()

        # Comparison to dict of date to ok
        comparison = comparison.set_index("departure_day").to_dict(orient="index")

        for entry in tqdm(history_entries, unit=f"history-entry", leave=False):
            is_valid = comparison[entry.date]["ok"] if entry.date in comparison else True
            entry.last_checked = checked_time
            entry.is_valid = is_valid

        session.commit()

    def update_sync_comparison_details(
        self,
        origin_iso2=None,
        comparison_details=None,
        checked_time=None,
    ):
        comparison_details["origin"] = origin_iso2
        comparison_details["checked_time"] = checked_time

        comparison_details.to_sql(
            DB_TABLE_KPLER_SYNC_COMPARISON_DETAILS,
            session.bind,
            if_exists="append",
            index=False,
        )

        session.commit()

    def compare_to_live_flows_for_dest(self, origin_iso2, date_from, date_to):

        logger.info(f"Comparing {origin_iso2} from {date_from} to {date_to} by destination")

        flows_from_kpler = self.scraper.get_flows(
            origin_iso2=origin_iso2,
            granularity=FlowsPeriod.Daily,
            unit=FlowsMeasurementUnit.T,
            date_from=date_from - dt.timedelta(days=RECENT_PERIOD_FOR_ERROR_CHECKING),
            date_to=date_to,
            split=FlowsSplit.DestinationCountries,
        )[["date", "to_iso2", "value"]]

        departure_day = func.date_trunc("day", KplerTrade.departure_date_utc).label("departure_day")

        destination_zone = aliased(KplerZone)
        origin_zone = aliased(KplerZone)

        grouped_trades_query = (
            session.query(
                departure_day,
                destination_zone.country_iso2,
                func.sum(KplerTrade.value_tonne).label("value_tonne"),
            )
            .outerjoin(
                origin_zone,
                KplerTrade.departure_zone_id == origin_zone.id,
            )
            .outerjoin(
                destination_zone,
                KplerTrade.arrival_zone_id == destination_zone.id,
            )
            .filter(
                origin_zone.country_iso2 == origin_iso2,
                departure_day >= date_from,
                departure_day <= date_to,
                KplerTrade.is_valid == True,
            )
            .group_by(departure_day, destination_zone.country_iso2)
        )

        actual = pd.read_sql(grouped_trades_query.statement, session.bind)
        actual.country_iso2 = actual.country_iso2.fillna("unknown")

        expected = flows_from_kpler.rename(
            columns={"date": "departure_day", "to_iso2": "country_iso2", "value": "value_tonne"}
        )
        expected.country_iso2 = expected.country_iso2.fillna("unknown")

        expected["recent_sum"] = (
            expected.groupby("country_iso2", dropna=False)
            .rolling(
                window=dt.timedelta(days=RECENT_PERIOD_FOR_ERROR_CHECKING),
                min_periods=1,
                on="departure_day",
            )
            .sum()["value_tonne"]
            .reset_index(0, drop=True)
        )

        # Using pandas, merge actual and expected
        comparison = pd.merge(
            expected,
            actual,
            on=["departure_day", "country_iso2"],
            how="outer",
            suffixes=(".expected", ".actual"),
        )

        departure_date = pd.to_datetime(comparison["departure_day"]).dt.date
        comparison = comparison[(departure_date >= date_from) & (departure_date <= date_to)]

        not_close = ~np.isclose(
            comparison["value_tonne.expected"],
            comparison["value_tonne.actual"],
            rtol=TOLERANCE_FOR_VALUE_ERROR_FOR_DAY_COUNTRY,
        )

        error_is_significant_in_recent = (
            np.abs(comparison["value_tonne.expected"] - comparison["value_tonne.actual"])
            > comparison["recent_sum"] * TOLERANCE_FOR_VALUE_ERROR_IN_RECENT_PERIOD_SUM
        )

        comparison["problems"] = not_close & error_is_significant_in_recent

        return comparison

    def compare_to_live_flows_for_product(self, origin_iso2, date_from, date_to):

        logger.info(f"Comparing {origin_iso2} from {date_from} to {date_to} by product")

        flows_from_kpler = self.scraper.get_flows(
            origin_iso2=origin_iso2,
            granularity=FlowsPeriod.Daily,
            unit=FlowsMeasurementUnit.T,
            date_from=date_from,
            date_to=date_to,
            split=FlowsSplit.Products,
        )[["date", "group", "family", "value"]]

        flows_from_kpler["product"] = flows_from_kpler.group.combine_first(flows_from_kpler.family)

        flows_from_kpler = flows_from_kpler[["date", "product", "value"]]

        departure_day = func.date_trunc("day", KplerTrade.departure_date_utc).label("departure_day")
        product = func.coalesce(KplerProduct.group_name, KplerProduct.family_name).label("product")

        origin_zone = aliased(KplerZone)

        grouped_trades_query = (
            session.query(
                departure_day,
                product,
                func.sum(KplerTrade.value_tonne).label("value_tonne"),
            )
            .outerjoin(
                origin_zone,
                KplerTrade.departure_zone_id == origin_zone.id,
            )
            .outerjoin(
                KplerProduct,
                KplerTrade.product_id == KplerProduct.id,
            )
            .filter(
                origin_zone.country_iso2 == origin_iso2,
                departure_day >= date_from,
                departure_day <= date_to,
                KplerTrade.is_valid == True,
            )
            .group_by(departure_day, product)
        )

        actual = pd.read_sql(grouped_trades_query.statement, session.bind)

        expected = (
            flows_from_kpler[pd.notnull(flows_from_kpler["product"])]
            .rename(columns={"date": "departure_day", "value": "value_tonne"})
            .groupby(["departure_day", "product"])
            .aggregate({"value_tonne": "sum"})
            .reset_index()
        )

        expected["recent_sum"] = (
            expected.groupby("product", dropna=False)
            .rolling(
                window=dt.timedelta(days=RECENT_PERIOD_FOR_ERROR_CHECKING),
                min_periods=1,
                on="departure_day",
            )
            .sum()["value_tonne"]
            .reset_index(0, drop=True)
        )

        # Using pandas, merge actual and expected
        comparison = pd.merge(
            expected,
            actual,
            on=["departure_day", "product"],
            how="outer",
            suffixes=(".expected", ".actual"),
        )

        departure_date = pd.to_datetime(comparison["departure_day"]).dt.date
        comparison = comparison[(departure_date >= date_from) & (departure_date <= date_to)]

        not_close = ~np.isclose(
            comparison["value_tonne.expected"],
            comparison["value_tonne.actual"],
            rtol=TOLERANCE_FOR_VALUE_ERROR_FOR_DAY_PRODUCT,
        )

        error_is_significant_in_recent = (
            np.abs(comparison["value_tonne.expected"] - comparison["value_tonne.actual"])
            > comparison["recent_sum"] * TOLERANCE_FOR_VALUE_ERROR_IN_RECENT_PERIOD_SUM
        )

        comparison["problems"] = not_close & error_is_significant_in_recent

        return comparison

    def _aggregate_to_check_period(self, comparison):
        return (
            comparison.groupby("departure_day")
            .aggregate(
                {"problems": "sum", "value_tonne.expected": "sum", "value_tonne.actual": "sum"}
            )
            .reset_index()
        )
