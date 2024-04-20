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

TOLERANCE_FOR_VALUE_ERROR = 0.05
ABSOLUTE_TOLERANCE_FOR_VALUE_ERROR = 100


class KplerTradeComparer:
    def __init__(self):
        self.scraper = KplerFlowScraper()

    def compare_to_live_flows(self, origin_iso2=None, date_from=None, date_to=None):

        comparison_per_dest = self.compare_to_live_flows_for_dest(origin_iso2, date_from, date_to)
        comparison_per_product = self.compare_to_live_flows_for_product(
            origin_iso2, date_from, date_to
        )

        self.update_sync_comparison_details(
            origin_iso2=origin_iso2,
            comparison_details=self.combine_comparison_details(
                comparison_per_dest, comparison_per_product
            ),
            checked_time=dt.datetime.now(),
        )

        dest_comparison = self._aggregate_to_check_period(comparison_per_dest)
        product_comparison = self._aggregate_to_check_period(comparison_per_product)

        return self.summarise_comparisons(dest_comparison, product_comparison)

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
        comparison["ok"] = comparison["problems"] == 0

        return comparison

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

        departure_date = pd.to_datetime(comparison["departure_day"]).dt.date
        comparison = comparison[(departure_date >= date_from) & (departure_date <= date_to)]

        not_close = ~np.isclose(
            comparison["value_tonne.expected"],
            comparison["value_tonne.actual"],
            rtol=TOLERANCE_FOR_VALUE_ERROR,
            atol=ABSOLUTE_TOLERANCE_FOR_VALUE_ERROR,
        )

        comparison["problems"] = not_close

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
            rtol=TOLERANCE_FOR_VALUE_ERROR,
            atol=ABSOLUTE_TOLERANCE_FOR_VALUE_ERROR,
        )

        comparison["problems"] = not_close

        return comparison

    def _aggregate_to_check_period(self, comparison):
        return (
            comparison.groupby("departure_day")
            .aggregate(
                {"problems": "sum", "value_tonne.expected": "sum", "value_tonne.actual": "sum"}
            )
            .reset_index()
        )
