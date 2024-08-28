from http import HTTPStatus
from itertools import product
from flask import Response
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import func, case, cast, nullslast, any_, true, String, Integer, column
from sqlalchemy.orm import aliased
from sqlalchemy.dialects.postgresql import aggregate_order_by, array, ARRAY, array_agg

from flask_restx import inputs, reqparse

import datetime as dt

import base
from base import UNKNOWN_INSURER
from base.models.kpler import KplerSyncHistory
from .security import key_required
from . import routes_api
from .template import TemplateResource
from base import PRICING_DEFAULT
from base import UNKNOWN_INSURER
from base.logger import logger
from base.db import session
from base.models import (
    KplerProduct,
    Country,
    Currency,
    Commodity,
    KplerTrade,
    KplerTradeComputed,
    KplerTradeComputedShips,
    KplerInstallation,
    KplerZone,
)
from base.utils import to_datetime, to_list, intersect, df_to_json


def string_array(values):
    return cast(array(values), ARRAY(String))


def integer_array(values):
    return cast(array(values), ARRAY(Integer))


# Using a static value for this using this query. The value of 99 percentile is probably unlikely to
# change.
#
# with journey_lengths as (
# 	select (arrival_date_utc - departure_date_utc) as days
# 	  from kpler_trade
# 	 where arrival_date_utc is not null and departure_date_utc is not null
# 	 order by days desc
# )
# select percentile_cont(0.99) within group (order by days)
#  from journey_lengths;
JOURNEY_LENGTH_99_PERCENTILE_DAYS = 90


@routes_api.route("/v1/kpler_trade", strict_slashes=False)
class KplerTradeResource(TemplateResource):
    parser: reqparse.RequestParser = TemplateResource.parser.copy()

    parser.replace_argument(
        "aggregate_by",
        type=str,
        action="split",
        default=None,
        help="which variables to aggregate by. Could be any of origin_country,origin,destination_country,destination,product,date,date,country,year",
    )

    parser.add_argument(
        "trade_ids", type=int, action="split", default=None, help="The trade IDs to find."
    )

    parser.add_argument(
        "api_key", help="Key to use the endpoint", required=True, type=str, default=None
    )

    parser.add_argument(
        "origin_iso2", help="Origin iso2", required=False, action="split", default=None
    )

    parser.add_argument(
        "origin_area",
        help="Origin area, possible options: Arctic, Baltic, Black sea. Caspian Sea, Pacific",
        action="split",
        default=None,
    )

    parser.add_argument(
        "commodity_origin_iso2",
        help="Commodity origin iso2",
        required=False,
        action="split",
        default=None,
    )

    parser.add_argument(
        "origin_port_name", help="Origin port name(s)", required=False, action="split", default=None
    )

    parser.add_argument(
        "origin_region", help="Origin region", required=False, action="split", default=None
    )

    parser.add_argument(
        "destination_port_name",
        help="Destination port name(s)",
        required=False,
        action="split",
        default=None,
    )

    parser.add_argument(
        "destination_iso2",
        help="Destination iso2",
        required=False,
        action="split",
        default=None,
    )
    parser.add_argument(
        "destination_iso2_not",
        action="split",
        help="countries(s) of destination to exclude e.g. RU",
        required=False,
        default=None,
    )

    parser.add_argument(
        "destination_region",
        help="Destination region",
        required=False,
        action="split",
        default=None,
    )

    parser.add_argument(
        "date_from",
        type=str,
        help="Filters where ships left origin on or after date (format 2020-01-01). Overwritten if destination_date_from or origin_date_from is provided.",
        default="2020-01-01",
        required=False,
    )

    parser.add_argument(
        "date_to",
        type=str,
        help="Filters where ships left origin on or before date (format 2020-01-01). Overwritten if destination_date_to or origin_date_to is provided. Defaults to yesterday.",
        default="-1",
        required=False,
    )

    parser.add_argument(
        "destination_date_from",
        type=str,
        help="Filters where ships arrived at destination on or after date (format 2020-01-01)",
        default=None,
        required=False,
    )

    parser.add_argument(
        "destination_date_to",
        type=str,
        help="Filters where ships arrived at destination on or before date (format 2020-01-01)",
        default=None,
        required=False,
    )

    parser.add_argument(
        "origin_date_from",
        type=str,
        help="Filters where ships left origin on or after date (format 2020-01-01)",
        default=None,
        required=False,
    )

    parser.add_argument(
        "origin_date_to",
        type=str,
        help="Filters where ships left origin on or before date (format 2020-01-01)",
        default=None,
        required=False,
    )

    parser.add_argument(
        "exclude_within_country",
        type=inputs.boolean,
        help="exclude trades within the same country",
        default=True,
    )

    parser.add_argument(
        "pricing_scenario",
        help="Pricing scenario (standard or pricecap)",
        action="split",
        default=[PRICING_DEFAULT],
        required=False,
    )

    parser.add_argument(
        "currency",
        action="split",
        help="currency(ies) of returned results e.g. EUR,USD,GBP",
        required=False,
        default=["EUR", "USD"],
    )

    parser.add_argument(
        "grade",
        help="Kpler grade (typically a grade)",
        required=False,
        action="split",
        default=None,
    )
    parser.add_argument(
        "commodity",
        help="Kpler commodity (e.g. Crude)",
        required=False,
        action="split",
        default=None,
    )
    parser.add_argument(
        "group",
        help="Kpler product group (e.g. Crude/Co)",
        required=False,
        action="split",
        default=None,
    )
    parser.add_argument(
        "family", help="Kpler product family", required=False, action="split", default=None
    )
    parser.add_argument(
        "commodity_equivalent",
        help="Commodity using CREA's nomenclature: either crude_oil, oil_products, lng, or coal",
        required=False,
        action="split",
        default=None,
    )

    parser.add_argument(
        "buyer",
        type=str,
        help="name of the/a buyer",
        default=None,
        required=False,
    )

    parser.add_argument(
        "seller",
        type=str,
        help="name of the/a seller",
        default=None,
        required=False,
    )

    parser.add_argument(
        "origin_installation_ids",
        type=int,
        action="split",
        default=None,
        help="filters where origin_installation_id is any of the provided",
    )

    parser.add_argument(
        "destination_installation_ids",
        type=int,
        action="split",
        default=None,
        help="filters where destination_installation_id is any of the provided",
    )

    parser.add_argument(
        "origin_zone_ids",
        type=int,
        action="split",
        default=None,
        help="filters where origin_zone_id is any of the provided",
    )

    parser.add_argument(
        "destination_zone_ids",
        type=int,
        action="split",
        default=None,
        help="filters where destination_zone_id is any of the provided",
    )

    parser.add_argument(
        "map_unconfirmed_region_eu_to_unknown",
        type=inputs.boolean,
        help="Maps destination region to unknown if the destination of the EU is not likely.",
        required=False,
        default=False,
    )

    parser.add_argument(
        "only_sts",
        type=inputs.boolean,
        help="Filters where trade involves STS at origin, during transit, or at destination",
        required=False,
        default=False,
    )

    parser.add_argument(
        "sts_region",
        type=str,
        help="Filters where trade involves STS in region at origin, during transit, or at destination",
        required=False,
    )

    parser.add_argument(
        "sts_iso2",
        type=str,
        help="Filters where trade involves STS in country at origin, during transit, or at destination",
        required=False,
    )

    parser.add_argument(
        "completeness_checked_age_threshold",
        type=int,
        help="The number of days since the last check for completeness for the record.",
        required=False,
        default=35,
    )

    parser.add_argument(
        "completeness_check_error_percentage_days_threshold",
        type=float,
        help="The percentage of records that must be complete for the check to pass.",
        required=False,
        default=0.01,
    )

    parser.add_argument(
        "vessel_imos",
        type=str,
        help="Filters trades where the vessel IMO is any of the provided",
        required=False,
        action="split",
    )

    parser.add_argument(
        "nest_ships",
        type=inputs.boolean,
        help="Ships are nested by default. Unnest ships means that there's one row per ship."
        + "Sum aggregates will double count trades where there are multiple ships per trade.",
        required=False,
        default=True,
    )

    must_group_by = ["currency", "pricing_scenario"]
    date_cols = ["date", "origin_date", "destination_date"]
    value_cols = [
        "value_tonne",
        "value_m3",
        "value_gas_m3",
        "value_eur",
        "value_currency",
        "avg_vessel_age",
        "trade_count",
        "n_inspections_2y",
        "deficiencies_per_inspection_2y",
        "detentions_per_inspection_2y",
        "n_detentions_2y",
        "avg_n_inspections_2y",
        "avg_deficiencies_per_inspection_2y",
        "avg_detentions_per_inspection_2y",
        "avg_n_detentions_2y",
    ]

    pivot_dependencies = {
        "grade": ["commodity", "group", "family", "commodity_equivalent"],
        "commodity": ["group", "family", "commodity_equivalent"],
        "group": ["family", "commodity_equivalent"],
        "family": ["commodity_equivalent"],
        "origin_country": ["origin_iso2", "origin_region"],
        "commodity_origin_country": ["commodity_origin_iso2", "commodity_origin_region"],
        "origin_iso2": ["origin_country", "origin_region"],
        "commodity_origin_iso2": ["commodity_origin_country", "commodity_origin_region"],
        "destination_country": ["destination_iso2", "destination_region"],
        "commodity_destination_country": [
            "commodity_destination_iso2",
            "commodity_destination_region",
        ],
        "destination_iso2": ["destination_country", "destination_region"],
        "commodity_destination_iso2": [
            "commodity_destination_country",
            "commodity_destination_region",
        ],
    }
    filename = "kpler_trade"

    def get_aggregate_cols_dict(self, subquery, params):

        base_aggregates = {
            "flow_origin_port": [
                subquery.c.origin_port_name.label("origin_name"),
                subquery.c.origin_port_name,
                subquery.c.origin_zone_name,
                subquery.c.origin_iso2,
                subquery.c.origin_country,
                subquery.c.origin_region,
                subquery.c.origin_area,
                subquery.c.commodity_origin_iso2,
                subquery.c.commodity_origin_country,
                subquery.c.commodity_origin_region,
            ],
            "flow_origin_country": [
                subquery.c.origin_country.label("origin_name"),
                subquery.c.origin_iso2,
                subquery.c.origin_country,
                subquery.c.origin_region,
                subquery.c.origin_area,
                subquery.c.commodity_origin_iso2,
                subquery.c.commodity_origin_country,
                subquery.c.commodity_origin_region,
            ],
            "origin_country": [
                subquery.c.origin_iso2,
                subquery.c.origin_country,
                subquery.c.origin_region,
            ],
            "origin_iso2": [
                subquery.c.origin_iso2,
                subquery.c.origin_country,
                subquery.c.origin_region,
            ],
            "origin_area": [
                subquery.c.origin_iso2,
                subquery.c.origin_country,
                subquery.c.origin_region,
                subquery.c.origin_area,
            ],
            "commodity_origin_country": [
                subquery.c.commodity_origin_iso2,
                subquery.c.commodity_origin_country,
                subquery.c.commodity_origin_region,
            ],
            "commodity_origin_iso2": [
                subquery.c.commodity_origin_iso2,
                subquery.c.commodity_origin_country,
                subquery.c.commodity_origin_region,
            ],
            "origin": [
                subquery.c.origin_installation_name,
                subquery.c.origin_port_name,
                subquery.c.origin_zone_name,
                subquery.c.origin_area,
                subquery.c.origin_iso2,
                subquery.c.origin_country,
                subquery.c.origin_region,
            ],
            "flow_destination_port": [
                subquery.c.destination_port_name.label("destination_name"),
                subquery.c.destination_port_name,
                subquery.c.destination_zone_name,
                subquery.c.destination_iso2,
                subquery.c.destination_country,
                subquery.c.destination_region,
                subquery.c.commodity_destination_iso2,
                subquery.c.commodity_destination_country,
                subquery.c.commodity_destination_region,
            ],
            "flow_destination_country": [
                subquery.c.destination_country.label("destination_name"),
                subquery.c.destination_iso2,
                subquery.c.destination_country,
                subquery.c.destination_region,
                subquery.c.commodity_destination_iso2,
                subquery.c.commodity_destination_country,
                subquery.c.commodity_destination_region,
            ],
            "destination_port": [
                subquery.c.destination_port_name,
                subquery.c.destination_zone_name,
                subquery.c.destination_iso2,
                subquery.c.destination_country,
                subquery.c.destination_region,
            ],
            "destination_country": [
                subquery.c.destination_iso2,
                subquery.c.destination_country,
                subquery.c.destination_region,
            ],
            "destination_iso2": [
                subquery.c.destination_iso2,
                subquery.c.destination_country,
                subquery.c.destination_region,
            ],
            "destination_region": [
                subquery.c.destination_region,
            ],
            "commodity_destination_country": [
                subquery.c.commodity_destination_iso2,
                subquery.c.commodity_destination_country,
                subquery.c.commodity_destination_region,
            ],
            "commodity_destination_iso2": [
                subquery.c.commodity_destination_iso2,
                subquery.c.commodity_destination_country,
                subquery.c.commodity_destination_region,
            ],
            "destination": [
                subquery.c.destination_installation_name,
                subquery.c.destination_port_name,
                subquery.c.destination_zone_name,
                subquery.c.destination_country,
                subquery.c.destination_iso2,
                subquery.c.destination_region,
            ],
            "grade": [
                subquery.c.grade,
                subquery.c.commodity,
                subquery.c.group,
                subquery.c.family,
                subquery.c.commodity_equivalent,
                subquery.c.commodity_equivalent_name,
                subquery.c.commodity_equivalent_group,
                subquery.c.commodity_equivalent_group_name,
            ],
            "commodity": [
                subquery.c.commodity,
                subquery.c.group,
                subquery.c.family,
                subquery.c.commodity_equivalent,
                subquery.c.commodity_equivalent_name,
                subquery.c.commodity_equivalent_group,
                subquery.c.commodity_equivalent_group_name,
            ],
            "group": [
                subquery.c.group,
                subquery.c.commodity_equivalent_group,
                subquery.c.commodity_equivalent_group_name,
            ],
            "family": [
                subquery.c.family,
                subquery.c.commodity_equivalent_group,
                subquery.c.commodity_equivalent_group_name,
            ],
            "commodity_equivalent": [
                subquery.c.commodity_equivalent,
                subquery.c.commodity_equivalent_name,
                subquery.c.commodity_equivalent_group,
                subquery.c.commodity_equivalent_group_name,
            ],
            "commodity_equivalent_name": [
                subquery.c.commodity_equivalent,
                subquery.c.commodity_equivalent_name,
                subquery.c.commodity_equivalent_group,
                subquery.c.commodity_equivalent_group_name,
            ],
            "commodity_equivalent_group": [
                subquery.c.commodity_equivalent_group,
                subquery.c.commodity_equivalent_group_name,
            ],
            "commodity_equivalent_group_name": [
                subquery.c.commodity_equivalent_group,
                subquery.c.commodity_equivalent_group_name,
            ],
            "currency": [subquery.c.currency],
            "date": [func.date_trunc("day", subquery.c.origin_date_utc).label("date")],
            "origin_date": [
                func.date_trunc("day", subquery.c.origin_date_utc).label("origin_date")
            ],
            "origin_month": [func.date_trunc("month", subquery.c.origin_date_utc).label("month")],
            "origin_year": [func.extract("year", subquery.c.origin_date_utc).label("year")],
            "destination_date": [
                func.date_trunc("day", subquery.c.destination_date_utc).label("destination_date")
            ],
            "destination_month": [
                func.date_trunc("month", subquery.c.destination_date_utc).label("month")
            ],
            "destination_year": [
                func.extract("year", subquery.c.destination_date_utc).label("year")
            ],
            "pricing_scenario": [subquery.c.pricing_scenario],
            "ownership_sanction_coverage": [subquery.c.ownership_sanction_coverage],
            "flag_sanction_coverage": [subquery.c.flag_sanction_coverage],
            "status": [subquery.c.status],
            "is_sts": [subquery.c.is_sts],
        }

        nest_ships = params.get("nest_ships")

        ktc_aggregates = (
            {
                "largest_vessel_type": [subquery.c.largest_vessel_type],
            }
            if nest_ships
            else {
                "vessel_imo": [subquery.c.vessel_imo],
                "ship_insurer": [
                    subquery.c.ship_insurer_name,
                    subquery.c.ship_insurer_iso2,
                    subquery.c.ship_insurer_region,
                ],
                "ship_insurer_iso2": [
                    subquery.c.ship_insurer_iso2,
                    subquery.c.ship_insurer_region,
                ],
                "ship_owner": [
                    subquery.c.ship_owner_name,
                    subquery.c.ship_owner_iso2,
                    subquery.c.ship_owner_region,
                ],
                "ship_owner_iso2": [
                    subquery.c.ship_owner_iso2,
                    subquery.c.ship_owner_region,
                ],
                "crea_designation": [subquery.c.crea_designation],
            }
        )

        return base_aggregates | ktc_aggregates

    def get_agg_value_cols(self, subquery, params):

        base_values = [
            func.sum(subquery.c.value_tonne).label("value_tonne"),
            func.sum(subquery.c.value_m3).label("value_m3"),
            func.sum(subquery.c.value_gas_m3).label("value_gas_m3"),
            func.sum(subquery.c.value_eur).label("value_eur"),
            func.sum(subquery.c.value_currency).label("value_currency"),
            func.count(func.distinct(subquery.c.trade_id)).label("trade_count"),
        ]

        nest_ships = params.get("nest_ships")

        ktc_values = (
            [
                func.avg(subquery.c.avg_vessel_age).label("avg_vessel_age"),
                func.avg(subquery.c.avg_n_inspections_2y).label("avg_n_inspections_2y"),
                func.avg(subquery.c.avg_deficiencies_per_inspection_2y).label(
                    "avg_deficiencies_per_inspection_2y"
                ),
                func.avg(subquery.c.avg_detentions_per_inspection_2y).label(
                    "avg_detentions_per_inspection_2y"
                ),
                func.avg(subquery.c.avg_n_detentions_2y).label("avg_n_detentions_2y"),
            ]
            if nest_ships
            else [
                func.avg(subquery.c.vessel_age).label("avg_vessel_age"),
                func.avg(subquery.c.n_inspections_2y).label("avg_n_inspections_2y"),
                func.avg(subquery.c.deficiencies_per_inspection_2y).label(
                    "avg_deficiencies_per_inspection_2y"
                ),
                func.avg(subquery.c.detentions_per_inspection_2y).label(
                    "avg_detentions_per_inspection_2y"
                ),
                func.avg(subquery.c.n_detentions_2y).label("n_detentions_2y"),
            ]
        )

        return base_values + ktc_values

    @routes_api.expect(parser)
    @key_required
    def get(self):
        params = KplerTradeResource.parser.parse_args(strict=True)

        if params.get("origin_date_from") or params.get("destination_date_from"):
            params["date_from"] = None
        if params.get("origin_date_to") or params.get("destination_date_to"):
            params["date_to"] = None

        if params.get("date_from") is not None and params.get("date_from") == params.get("date_to"):
            return Response(
                status=HTTPStatus.BAD_REQUEST,
                response="date_from and date_to cannot be the same date",
            )
        if params.get("origin_date_from") is not None and params.get(
            "origin_date_from"
        ) == params.get("origin_date_to"):
            return Response(
                status=HTTPStatus.BAD_REQUEST,
                response="origin_date_from and origin_date_to cannot be the same date",
            )
        if params.get("destination_date_from") is not None and params.get(
            "destination_date_from"
        ) == params.get("destination_date_to"):
            return Response(
                status=HTTPStatus.BAD_REQUEST,
                response="destination_date_from and destination_date_to cannot be the same date",
            )

        return self.get_from_params(params)

    def check_complete(self, query, params):

        ### Setup the parameters for the sync check
        all_countries = self._get_origin_countries(params)

        min_date, max_date = self._get_sync_date_range(params)

        max_age = params.get("completeness_checked_age_threshold")
        earliest_allowed_date = (dt.datetime.now() - dt.timedelta(days=max_age)).date()
        percentage_threshold = params.get("completeness_check_error_percentage_days_threshold")

        n_countries = len(all_countries)
        n_days = (max_date - min_date).days
        total_entries_count = n_countries * n_days
        max_errors_count = total_entries_count * percentage_threshold

        ### Fetch the data for the sync check
        sync_history_with_missing = self._get_sync_history_with_missing(
            all_countries, min_date, max_date
        )

        ### Check the data for the sync check
        # Filter out the entries that are not valid based on the commented out query above
        failing_entries = sync_history_with_missing[
            # Confirms the data has been synced and checked.
            pd.isnull(sync_history_with_missing["last_checked"])
            # Confirms the data has been checked within the threshold.
            | (sync_history_with_missing["last_checked"] < pd.to_datetime(earliest_allowed_date))
            # Confirms the data is valid.
            | (sync_history_with_missing["is_valid"] == False)
        ].reset_index(drop=True)

        if len(failing_entries) > max_errors_count:
            return False, self._to_readable_failures(failing_entries).to_csv(index=False)
        else:
            return True, None

    def _to_readable_failures(self, failing_entries):
        grouped_entries = failing_entries.groupby(["country_iso2"])
        result = []
        for group, entries in grouped_entries:
            country_iso2 = group
            dates = entries["date"].tolist()
            sequential_dates = []
            start_date = dates[0]
            end_date = dates[0]
            for i in range(1, len(dates)):
                if (dates[i] - dates[i - 1]).days > 1:
                    sequential_dates.append((country_iso2, start_date, end_date))
                    start_date = dates[i]
                end_date = dates[i]
            sequential_dates.append((country_iso2, start_date, end_date))
            result.extend(sequential_dates)

        result = pd.DataFrame(result, columns=["origin_iso2", "date_from", "date_to"])
        return result

    def _get_sync_history_with_missing(self, all_countries, min_date, max_date):
        all_dates = pd.date_range(min_date, max_date)

        # Create a pandas dataframe with all the possible dates and countries
        all_possible_entries = pd.DataFrame(
            list(product(all_countries, all_dates)), columns=["country_iso2", "date"]
        )

        actual_sync_history_query = session.query(
            KplerSyncHistory.country_iso2,
            KplerSyncHistory.date,
            KplerSyncHistory.last_checked,
            KplerSyncHistory.is_valid,
        ).filter(
            sa.and_(
                # Country to check for date range
                KplerSyncHistory.country_iso2.in_(all_countries),
                KplerSyncHistory.date >= min_date,
                KplerSyncHistory.date <= max_date,
            )
        )

        actual_sync_history = pd.read_sql(actual_sync_history_query.statement, session.bind)

        # Merge the actual sync history with the possible entries to get the missing entries
        sync_history_with_missing = pd.merge(
            all_possible_entries,
            actual_sync_history,
            how="left",
            on=["country_iso2", "date"],
        )

        return sync_history_with_missing

    def _get_sync_date_range(self, params) -> tuple[dt.datetime, dt.datetime]:

        date_from: dt.datetime | None = to_datetime(params.get("date_from"))
        date_to: dt.datetime | None = to_datetime(params.get("date_to"))
        origin_date_from: dt.datetime | None = to_datetime(params.get("origin_date_from"))
        origin_date_to: dt.datetime | None = to_datetime(params.get("origin_date_to"))
        destination_date_from: dt.datetime | None = to_datetime(params.get("destination_date_from"))
        destination_date_to: dt.datetime | None = to_datetime(params.get("destination_date_to"))

        # We need to get the sync date for the date_from assuming the longest length journey. We
        # use the 99th percentile so it's not 10 years (which is the longest journey tracked in the
        # DB).
        origin_date_for_destination_date_from = (
            None
            if destination_date_from is None
            else (destination_date_from - dt.timedelta(days=JOURNEY_LENGTH_99_PERCENTILE_DAYS))
        )
        # We need to get the sync date for the date_to assuming the shortest length journey. We can
        # assume the 1st percentile is 0 days as it's close to this.
        origin_date_for_destination_date_to = destination_date_to

        date_froms = [
            date
            for date in [
                date_from,
                origin_date_from,
                origin_date_for_destination_date_from,
            ]
            if date is not None
        ]

        date_tos = [
            date
            for date in [
                date_to,
                origin_date_to,
                origin_date_for_destination_date_to,
            ]
            if date is not None
        ]

        min_date = min(date_froms)
        max_date = max(date_tos)

        return min_date, max_date

    def _get_origin_countries(self, params) -> list[str]:

        # Don't add to this without updating the rest of this function to handle the new params.
        checked_params = [
            "origin_iso2",
            "commodity_origin_iso2",
            "origin_region",
            "origin_port_name",
            "origin_area",
            "origin_installation_ids",
            "origin_zone_ids",
        ]

        origin_related_params = {
            key: value
            for key, value in params.items()
            if key.startswith("origin") and not key.startswith("origin_date")
        }

        # This is used to check that we are not missing any origin related params in the sync
        # checking. You should not delete this.
        assert all((param in checked_params) for param in origin_related_params)

        # We need to keep the list of params used up to date in checked_params.
        origin_iso2 = params.get("origin_iso2")
        origin_area = params.get("origin_area")
        commodity_origin_iso2 = params.get("commodity_origin_iso2")
        origin_port_name = params.get("origin_port_name")
        origin_region = params.get("origin_region")
        origin_installation_ids = params.get("origin_installation_ids")
        origin_zone_ids = params.get("origin_zone_ids")

        query = (
            session.query(Country.iso2)
            .outerjoin(KplerZone, Country.iso2 == KplerZone.country_iso2)
            .outerjoin(KplerInstallation, KplerZone.port_id == KplerInstallation.port_id)
            .distinct()
        )

        if origin_iso2:
            query = query.filter(Country.iso2.in_(to_list(origin_iso2)))

        if commodity_origin_iso2:
            query = query.filter(Country.iso2.in_(to_list(commodity_origin_iso2)))

        if origin_region:
            query = query.filter(Country.region.in_(to_list(origin_region)))

        if origin_port_name:
            query = query.filter(KplerZone.port_name.in_(to_list(origin_port_name)))

        if origin_area:
            query = query.filter(KplerZone.area.in_(to_list(origin_area)))

        if origin_installation_ids:
            query = query.filter(KplerInstallation.id.in_(to_list(origin_installation_ids)))

        if origin_zone_ids:
            query = query.filter(KplerZone.id.in_(to_list(origin_zone_ids)))

        return [item[0] for item in query.all()]

    def initial_query(self, params=None):
        origin_zone = aliased(KplerZone)
        destination_zone = aliased(KplerZone)
        CommodityEquivalent = aliased(Commodity)
        OriginCountry = aliased(Country)
        DestinationCountry = aliased(Country)
        CommodityOriginCountry = aliased(Country)
        CommodityDestinationCountry = aliased(Country)
        OriginInstallation = aliased(KplerInstallation)
        DestinationInstallation = aliased(KplerInstallation)

        price_date = func.date_trunc("day", KplerTrade.departure_date_utc)

        map_unconfirmed_region_eu_to_unknown = params.get("map_unconfirmed_region_eu_to_unknown")

        commodity_origin_iso2_field = case(
            [
                (KplerProduct.grade_name.in_(["CPC Kazakhstan", "KEBCO"]), "KZ"),
            ],
            else_=origin_zone.country_iso2,
        ).label("commodity_origin_iso2")

        destination_region_field = DestinationCountry.region.label("destination_region")
        commodity_destination_region_field = CommodityDestinationCountry.region.label(
            "commodity_destination_region"
        )
        if map_unconfirmed_region_eu_to_unknown:

            def map_destination_field(cls):
                return case(
                    (
                        sa.and_(
                            sa.or_(
                                KplerTrade.status == "ongoing",
                                KplerTrade.departure_date_utc > "2022-12-05",
                            ),
                            CommodityEquivalent.name == "Crude oil",
                            cls.region == "EU",
                            cls.iso2 != "BG",
                        ),
                        "Unknown",
                    ),
                    else_=cls.region,
                ).label("destination_region")

            destination_region_field = map_destination_field(DestinationCountry)
            commodity_destination_region_field = map_destination_field(CommodityDestinationCountry)

        is_sts_field = sa.or_(
            sa.and_(KplerTrade.arrival_sts != None, KplerTrade.arrival_sts == True),
            sa.and_(KplerTrade.departure_sts != None, KplerTrade.departure_sts == True),
            KplerTrade.step_zone_ids != None,
        ).label("is_sts")

        null_as_unknown = lambda field: func.coalesce(field, base.UNKNOWN)

        nest_ships = params.get("nest_ships")

        kpler_trade_computed_table = KplerTradeComputed if nest_ships else KplerTradeComputedShips

        kpler_trade_computed_columns = (
            [
                column
                for column in KplerTradeComputed.__table__.columns
                if column
                not in [
                    KplerTradeComputed.trade_id,
                    KplerTradeComputed.product_id,
                    KplerTradeComputed.flow_id,
                ]
            ]
            if nest_ships
            else [
                column
                for column in KplerTradeComputedShips.__table__.columns
                if column
                not in [
                    KplerTradeComputedShips.trade_id,
                    KplerTradeComputedShips.product_id,
                    KplerTradeComputedShips.flow_id,
                ]
            ]
        )

        value_eur_field = (KplerTrade.value_tonne * kpler_trade_computed_table.eur_per_tonne).label(
            "value_eur"
        )

        columns = [
            # Renaming everything in terms of "origin" and "destination"
            KplerTrade.id.label("trade_id"),
            KplerTrade.flow_id,
            KplerTrade.status,
            KplerTrade.departure_date_utc.label("origin_date_utc"),
            KplerTrade.departure_installation_id.label("origin_installation_id"),
            KplerTrade.departure_installation_name.label("origin_installation_name"),
            OriginInstallation.type.label("origin_installation_type"),
            KplerTrade.departure_zone_id.label("origin_zone_id"),
            origin_zone.name.label("origin_zone_name"),
            origin_zone.name.label("origin_zone_type"),
            origin_zone.port_id.label("origin_port_id"),
            origin_zone.port_name.label("origin_port_name"),
            origin_zone.country_name.label("origin_country"),
            origin_zone.country_iso2.label("origin_iso2"),
            origin_zone.area.label("origin_area"),
            OriginCountry.region.label("origin_region"),
            commodity_origin_iso2_field,
            CommodityOriginCountry.name.label("commodity_origin_country"),
            CommodityOriginCountry.region.label("commodity_origin_region"),
            KplerTrade.arrival_date_utc.label("destination_date_utc"),
            KplerTrade.arrival_installation_id.label("destination_installation_id"),
            KplerTrade.arrival_installation_name.label("destination_installation_name"),
            DestinationInstallation.type.label("destination_installation_type"),
            destination_zone.id.label("destination_zone_id"),
            null_as_unknown(destination_zone.name).label("destination_zone_name"),
            null_as_unknown(destination_zone.type).label("destination_zone_type"),
            destination_zone.port_id.label("destination_port_id"),
            null_as_unknown(destination_zone.port_name).label("destination_port_name"),
            null_as_unknown(destination_zone.country_name).label("destination_country"),
            null_as_unknown(destination_zone.country_iso2).label("destination_iso2"),
            destination_region_field,
            null_as_unknown(destination_zone.country_name).label("commodity_destination_country"),
            null_as_unknown(destination_zone.country_iso2).label("commodity_destination_iso2"),
            commodity_destination_region_field,
            KplerTrade.departure_sts.label("origin_sts"),
            KplerTrade.arrival_sts.label("destination_sts"),
            KplerProduct.grade_name.label("grade"),
            KplerProduct.commodity_name.label("commodity"),
            KplerProduct.group_name.label("group"),
            KplerProduct.family_name.label("family"),
            Commodity.equivalent_id.label("commodity_equivalent"),  # For filtering
            CommodityEquivalent.name.label("commodity_equivalent_name"),
            CommodityEquivalent.group.label("commodity_equivalent_group"),
            CommodityEquivalent.group_name.label("commodity_equivalent_group_name"),
            KplerTrade.value_tonne,
            KplerTrade.value_m3,
            KplerTrade.value_gas_m3,
            value_eur_field,
            Currency.currency,
            (value_eur_field * Currency.per_eur).label("value_currency"),
            KplerTrade.vessel_imos,
            KplerTrade.buyer_names,
            KplerTrade.seller_names,
            is_sts_field,
        ] + kpler_trade_computed_columns

        query = (
            session.query(*columns)
            .outerjoin(KplerProduct, KplerTrade.product_id == KplerProduct.id)
            .join(origin_zone, KplerTrade.departure_zone_id == origin_zone.id)
            .outerjoin(destination_zone, KplerTrade.arrival_zone_id == destination_zone.id)
            .outerjoin(
                OriginCountry,
                OriginCountry.iso2 == origin_zone.country_iso2,
            )
            .outerjoin(
                DestinationCountry,
                DestinationCountry.iso2 == destination_zone.country_iso2,
            )
            .outerjoin(
                CommodityOriginCountry,
                CommodityOriginCountry.iso2 == commodity_origin_iso2_field,
            )
            .outerjoin(
                CommodityDestinationCountry,
                CommodityDestinationCountry.iso2 == destination_zone.country_iso2,
            )
            .join(
                kpler_trade_computed_table,
                sa.and_(
                    kpler_trade_computed_table.trade_id == KplerTrade.id,
                    kpler_trade_computed_table.flow_id == KplerTrade.flow_id,
                    kpler_trade_computed_table.product_id == KplerTrade.product_id,
                    kpler_trade_computed_table.eur_per_tonne != None,
                ),
            )
            .join(Commodity, kpler_trade_computed_table.kpler_product_commodity_id == Commodity.id)
            .join(CommodityEquivalent, Commodity.equivalent_id == CommodityEquivalent.id)
            .outerjoin(Currency, Currency.date == price_date)
            .outerjoin(
                OriginInstallation, OriginInstallation.id == KplerTrade.departure_installation_id
            )
            .outerjoin(
                DestinationInstallation,
                DestinationInstallation.id == KplerTrade.arrival_installation_id,
            )
            .order_by(
                KplerTrade.id,
                KplerTrade.flow_id,
                kpler_trade_computed_table.pricing_scenario,
                Currency.currency,
            )
        )

        # Only keep valid trades
        query = query.filter(KplerTrade.is_valid == True).filter(
            origin_zone.country_iso2 != "not found"
        )

        return query

    def filter(self, query, params=None):
        origin_iso2 = params.get("origin_iso2")
        origin_region = params.get("origin_region")
        origin_port_name = params.get("origin_port_name")
        origin_area = params.get("origin_area")
        commodity_origin_iso2 = params.get("commodity_origin_iso2")
        destination_iso2 = params.get("destination_iso2")
        destination_iso2_not = params.get("destination_iso2_not")
        commodity_destination_iso2 = params.get("commodity_destination_iso2")
        destination_port_name = params.get("destination_port_name")
        destination_region = params.get("destination_region")
        exclude_within_country = params.get("exclude_within_country")
        only_sts = params.get("only_sts")

        trade_ids = params.get("trade_ids")

        grade = params.get("grade")
        commodity = params.get("commodity")
        group = params.get("group")
        family = params.get("family")
        commodity_equivalent = params.get("commodity_equivalent")

        date_from = params.get("date_from")
        date_to = params.get("date_to")
        destination_date_from = params.get("destination_date_from")
        destination_date_to = params.get("destination_date_to")
        origin_date_from = params.get("origin_date_from")
        origin_date_to = params.get("origin_date_to")
        pricing_scenario = params.get("pricing_scenario")
        currency = params.get("currency")

        buyer = params.get("buyer")
        seller = params.get("seller")

        origin_installation_ids = params.get("origin_installation_ids")
        destination_installation_ids = params.get("destination_installation_ids")

        origin_zone_ids = params.get("origin_zone_ids")
        destination_zone_ids = params.get("destination_zone_ids")

        sts_region = params.get("sts_region")
        sts_iso2 = params.get("sts_iso2")

        vessel_imos = params.get("vessel_imos")

        nest_ships = params.get("nest_ships")

        kpler_trade_computed_table = KplerTradeComputed if nest_ships else KplerTradeComputedShips

        if trade_ids:
            query = query.filter(KplerTrade.id.in_(to_list(trade_ids)))

        if grade:
            query = query.filter(KplerProduct.grade_name.in_(to_list(grade)))

        if date_from:
            query = query.filter(KplerTrade.departure_date_utc >= str(to_datetime(date_from)))

        if date_to:
            query = query.filter(
                func.date_trunc("day", KplerTrade.departure_date_utc) <= to_datetime(date_to)
            )

        if origin_date_from:
            query = query.filter(
                KplerTrade.departure_date_utc >= str(to_datetime(origin_date_from))
            )

        if origin_date_to:
            query = query.filter(
                func.date_trunc("day", KplerTrade.departure_date_utc) <= to_datetime(origin_date_to)
            )

        if destination_date_from:
            query = query.filter(
                KplerTrade.arrival_date_utc >= str(to_datetime(destination_date_from))
            )

        if destination_date_to:
            query = query.filter(
                func.date_trunc("day", KplerTrade.arrival_date_utc)
                <= to_datetime(destination_date_to)
            )

        if pricing_scenario:
            query = query.filter(
                kpler_trade_computed_table.pricing_scenario.in_(to_list(pricing_scenario))
            )

        if currency is not None:
            query = query.filter(Currency.currency.in_(to_list(currency)))

        if buyer:
            query = query.filter(KplerTrade.buyer_names.overlap(to_list(buyer)))

        if seller:
            query = query.filter(KplerTrade.seller_names.overlap(to_list(seller)))

        if origin_installation_ids:
            query = query.filter(
                KplerTrade.departure_installation_id.in_(to_list(origin_installation_ids))
            )

        if destination_installation_ids:
            query = query.filter(
                KplerTrade.arrival_installation_id.in_(to_list(destination_installation_ids))
            )

        if origin_zone_ids:
            query = query.filter(KplerTrade.departure_zone_id.in_(to_list(origin_zone_ids)))

        if destination_zone_ids:
            query = query.filter(KplerTrade.arrival_zone_id.in_(to_list(destination_zone_ids)))

        subquery = query.subquery()
        query = session.query(subquery)

        if origin_iso2:
            query = query.filter(subquery.c.origin_iso2.in_(to_list(origin_iso2)))

        if origin_port_name:
            query = query.filter(subquery.c.origin_port_name.in_(to_list(origin_port_name)))

        if origin_region:
            query = query.filter(subquery.c.origin_region.in_(to_list(origin_region)))

        if origin_area:
            query = query.filter(subquery.c.origin_area.in_(to_list(origin_area)))

        if destination_port_name:
            query = query.filter(
                subquery.c.destination_port_name.in_(to_list(destination_port_name))
            )

        if destination_iso2:
            query = query.filter(subquery.c.destination_iso2.in_(to_list(destination_iso2)))
        if destination_iso2_not:
            query = query.filter(subquery.c.destination_iso2.notin_(to_list(destination_iso2_not)))

        if commodity:
            query = query.filter(subquery.c.commodity.in_(to_list(commodity)))

        if group:
            query = query.filter(subquery.c.group.in_(to_list(group)))

        if family:
            query = query.filter(subquery.c.family.in_(to_list(family)))

        if commodity_equivalent:
            query = query.filter(subquery.c.commodity_equivalent.in_(to_list(commodity_equivalent)))

        if destination_region:
            query = query.filter(subquery.c.destination_region.in_(to_list(destination_region)))

        if commodity_origin_iso2:
            query = query.filter(
                subquery.c.commodity_origin_iso2.in_(to_list(commodity_origin_iso2))
            )

        if commodity_destination_iso2:
            query = query.filter(
                subquery.c.commodity_destination_iso2.in_(to_list(commodity_destination_iso2))
            )

        if exclude_within_country:
            query = query.filter(
                sa.or_(
                    subquery.c.origin_iso2 != subquery.c.destination_iso2,
                    subquery.c.destination_iso2 == None,
                )
            )

        if only_sts:
            query = query.filter(subquery.c.is_sts == True)

        if sts_region:
            query = query.filter(
                (
                    (subquery.c.destination_region == sts_region)
                    & (subquery.c.destination_sts == True)
                )
                | ((subquery.c.origin_region == sts_region) & (subquery.c.origin_sts == True))
                | subquery.c.step_zone_regions.any(sts_region)
            )
        if sts_iso2:
            query = query.filter(
                ((subquery.c.destination_iso2 == sts_iso2) & (subquery.c.destination_sts == True))
                | ((subquery.c.origin_iso2 == sts_iso2) & (subquery.c.origin_sts == True))
                | subquery.c.step_zone_iso2s.any(sts_iso2)
            )

        if vessel_imos:
            query = query.filter(subquery.c.vessel_imos.overlap(to_list(vessel_imos)))

        return query
