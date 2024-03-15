import numpy as np
from tqdm import tqdm
from base.db import session
from base.logger import logger

from kpler.sdk import FlowsSplit, FlowsPeriod, FlowsMeasurementUnit
from base.models.kpler import KplerProduct, KplerSyncHistory, KplerTrade, KplerZone
from base.utils import to_datetime

from engines.kpler_scraper.scraper_flow import KplerFlowScraper


from sqlalchemy import func
from sqlalchemy.orm import aliased

import pandas as pd
import datetime as dt

TOLERANCE_FOR_VALUE_ERROR_FOR_DAY_COUNTRY = 0.05
TOLERANCE_FOR_VALUE_ERROR_FOR_DAY_PRODUCT = 0.05
TOLERANCE_FOR_VALUE_ERROR_FOR_WHOLE_DAY = 0.01
SPLIT_PROBLEM_COUNT_THRESHOLD = 3


class KplerTradeVerifier:
    def __init__(self):
        self.scraper = KplerFlowScraper()

    def verify_sync_against_flows(
        self, origin_iso2s=None, platforms=None, date_from=None, date_to=None
    ):
        date_from = to_datetime(date_from) if date_from is not None else dt.date(2020, 1, 1)
        date_to = to_datetime(date_to) if date_to is not None else dt.date.today()
        if isinstance(date_from, dt.datetime):
            date_from = date_from.date()
        if isinstance(date_to, dt.datetime):
            date_to = date_to.date()

        checked_time = dt.datetime.now()

        comparisons = pd.DataFrame()

        for country in origin_iso2s:
            for platform in platforms:
                logger.info(f"Verifying {country} {platform} from {date_from} to {date_to}")
                comparison = self.compare_to_live_flows(
                    origin_iso2=country, platform=platform, date_from=date_from, date_to=date_to
                )

                comparison["country"] = country
                comparison["platform"] = platform

                self.update_sync_history_with_status(
                    origin_iso2=country,
                    platform=platform,
                    date_from=date_from,
                    date_to=date_to,
                    comparison=comparison,
                    checked_time=checked_time,
                )

                comparisons = pd.concat([comparisons, comparison])

        failed_comparisons = comparisons[~comparisons["ok"]]
        if (comparisons["ok"] == False).any():
            logger.warn(f"Some comparisons failed:\n{failed_comparisons.to_string()}")

        return

    def compare_to_live_flows(self, origin_iso2=None, platform=None, date_from=None, date_to=None):

        dest_comparison = self.compare_to_live_flows_for_dest(
            origin_iso2, platform, date_from, date_to
        )
        product_comparison = self.compare_to_live_flows_for_product(
            origin_iso2, platform, date_from, date_to
        )

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
        comparison["ok"] = (comparison["problems"] < SPLIT_PROBLEM_COUNT_THRESHOLD) & np.isclose(
            comparison["value_tonne.expected"],
            comparison["value_tonne.actual"],
            rtol=TOLERANCE_FOR_VALUE_ERROR_FOR_WHOLE_DAY,
        )

        return comparison

    def update_sync_history_with_status(
        self,
        origin_iso2=None,
        platform=None,
        date_from=None,
        date_to=None,
        comparison=None,
        checked_time=None,
    ):
        query_for_history_entries = session.query(KplerSyncHistory).filter(
            KplerSyncHistory.country_iso2 == origin_iso2,
            KplerSyncHistory.platform == platform,
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

    def compare_to_live_flows_for_dest(self, origin_iso2, platform, date_from, date_to):

        logger.info(
            f"Comparing {origin_iso2} {platform} from {date_from} to {date_to} by destination"
        )

        flows_from_kpler = self.scraper.get_flows(
            platform,
            origin_iso2=origin_iso2,
            granularity=FlowsPeriod.Daily,
            unit=FlowsMeasurementUnit.T,
            date_from=date_from,
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
                KplerTrade.platform == platform,
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

        # Using pandas, merge actual and expected
        comparison = pd.merge(
            expected,
            actual,
            on=["departure_day", "country_iso2"],
            how="outer",
            suffixes=(".expected", ".actual"),
        )

        comparison["problems"] = ~np.isclose(
            comparison["value_tonne.expected"],
            comparison["value_tonne.actual"],
            rtol=TOLERANCE_FOR_VALUE_ERROR_FOR_DAY_COUNTRY,
        )

        # Group comparison by departure_day counting the number of "not ok"
        comparison_per_day = (
            comparison.groupby("departure_day")
            .aggregate(
                {"problems": "sum", "value_tonne.expected": "sum", "value_tonne.actual": "sum"}
            )
            .reset_index()
        )

        return comparison_per_day

    def compare_to_live_flows_for_product(self, origin_iso2, platform, date_from, date_to):

        logger.info(f"Comparing {origin_iso2} {platform} from {date_from} to {date_to} by product")

        flows_from_kpler = self.scraper.get_flows(
            platform,
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
                KplerTrade.platform == platform,
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

        # Using pandas, merge actual and expected
        comparison = pd.merge(
            expected,
            actual,
            on=["departure_day", "product"],
            how="outer",
            suffixes=(".expected", ".actual"),
        )

        comparison["problems"] = ~np.isclose(
            comparison["value_tonne.expected"],
            comparison["value_tonne.actual"],
            rtol=TOLERANCE_FOR_VALUE_ERROR_FOR_DAY_PRODUCT,
        )

        # Group comparison by departure_day counting the number of "not ok"
        comparison_per_day = (
            comparison.groupby("departure_day")
            .aggregate(
                {"problems": "sum", "value_tonne.expected": "sum", "value_tonne.actual": "sum"}
            )
            .reset_index()
        )

        return comparison_per_day
