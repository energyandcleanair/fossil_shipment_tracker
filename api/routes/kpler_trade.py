from http import HTTPStatus
from flask import Response
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import (
    func,
    case,
    cast,
    nullslast,
    any_,
    true,
    String,
    Integer,
)
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
    KplerInstallation,
    KplerZone,
)
from base.utils import to_datetime, to_list, intersect, df_to_json


def string_array(values):
    return cast(array(values), ARRAY(String))


def integer_array(values):
    return cast(array(values), ARRAY(Integer))


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

    must_group_by = ["currency", "pricing_scenario"]
    date_cols = ["date", "origin_date", "destination_date"]
    value_cols = [
        "value_tonne",
        "value_m3",
        "value_gas_m3",
        "value_eur",
        "value_currency",
        "avg_vessel_age",
    ]

    pivot_dependencies = {
        "grade": ["commodity", "group", "family", "commodity_equivalent"],
        "commodity": ["group", "family", "commodity_equivalent"],
        "group": ["family", "commodity_equivalent"],
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

    def get_aggregate_cols_dict(self, subquery):
        return {
            "flow_origin_port": [
                subquery.c.origin_port_name.label("origin_name"),
                subquery.c.origin_port_name,
                subquery.c.origin_zone_name,
                subquery.c.origin_iso2,
                subquery.c.origin_country,
                subquery.c.origin_region,
                subquery.c.commodity_origin_iso2,
                subquery.c.commodity_origin_country,
                subquery.c.commodity_origin_region,
            ],
            "flow_origin_country": [
                subquery.c.origin_country.label("origin_name"),
                subquery.c.origin_iso2,
                subquery.c.origin_country,
                subquery.c.origin_region,
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
            "ship_insurer_country": [
                subquery.c.ship_insurer_iso2s,
                subquery.c.ship_insurer_regions,
            ],
            "ship_owner_country": [
                subquery.c.ship_owner_iso2s,
                subquery.c.ship_owner_regions,
            ],
            "ownership_sanction_coverage": [subquery.c.ownership_sanction_coverage],
            "status": [subquery.c.status],
            "is_sts": [subquery.c.is_sts],
        }

    def get_agg_value_cols(self, subquery):
        return [
            func.sum(subquery.c.value_tonne).label("value_tonne"),
            func.sum(subquery.c.value_m3).label("value_m3"),
            func.sum(subquery.c.value_gas_m3).label("value_gas_m3"),
            func.sum(subquery.c.value_eur).label("value_eur"),
            func.sum(subquery.c.value_currency).label("value_currency"),
            func.avg(subquery.c.avg_vessel_age).label("avg_vessel_age"),
            # func.sum(subquery.c.value_energy).label("value_energy"),
            # func.sum(subquery.c.value_gas_m3).label("value_gas_m3")
        ]

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

        subquery = query.subquery()

        max_age = params.get("completeness_checked_age_threshold")
        earliest_allowed_date = (dt.datetime.now() - dt.timedelta(days=max_age)).date()

        failing_entries_query = (
            session.query(
                KplerSyncHistory.country_iso2,
                KplerSyncHistory.date,
            )
            .select_from(subquery)
            .outerjoin(
                KplerSyncHistory,
                sa.and_(
                    KplerSyncHistory.country_iso2 == subquery.c.origin_iso2,
                    KplerSyncHistory.date == func.date_trunc("day", subquery.c.origin_date_utc),
                ),
            )
            .filter(
                # Negate valid check
                sa.not_(
                    # Would be valid if it meets these requirements.
                    sa.and_(
                        # Check that we've updated this data.
                        KplerSyncHistory.id != None,
                        # Check that the data has been checked for completeness.
                        KplerSyncHistory.last_checked != None,
                        # Check that the data has been checked for completeness within the threshold
                        KplerSyncHistory.last_checked > earliest_allowed_date,
                        # Check that the data is complete.
                        KplerSyncHistory.is_valid,
                    )
                )
            )
            .group_by(
                KplerSyncHistory.country_iso2,
                KplerSyncHistory.date,
            )
        )

        failing_entries = pd.read_sql(failing_entries_query.statement, session.bind)

        # Convert list to country_iso2, date_from, date_to for each set of sequential dates
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

        if len(result) > 0:
            return False, result.to_csv(index=False)
        else:
            return True, None

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

        value_eur_field = (KplerTrade.value_tonne * KplerTradeComputed.eur_per_tonne).label(
            "value_eur"
        )

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

        query = (
            session.query(
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
                null_as_unknown(destination_zone.country_name).label(
                    "commodity_destination_country"
                ),
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
                KplerTradeComputed.pricing_scenario,
                KplerTrade.value_tonne,
                KplerTrade.value_m3,
                KplerTrade.value_gas_m3,
                value_eur_field,
                Currency.currency,
                (value_eur_field * Currency.per_eur).label("value_currency"),
                KplerTradeComputed.pricing_commodity,
                KplerTrade.vessel_imos,
                KplerTrade.buyer_names,
                KplerTrade.seller_names,
                KplerTradeComputed.vessel_ages,
                KplerTradeComputed.avg_vessel_age,
                KplerTradeComputed.ship_insurer_names,
                KplerTradeComputed.ship_insurer_iso2s,
                KplerTradeComputed.ship_insurer_regions,
                KplerTradeComputed.ship_owner_names,
                KplerTradeComputed.ship_owner_iso2s,
                KplerTradeComputed.ship_owner_regions,
                KplerTradeComputed.ownership_sanction_coverage,
                KplerTradeComputed.step_zone_names,
                KplerTradeComputed.step_zone_iso2s,
                KplerTradeComputed.step_zone_regions,
                KplerTradeComputed.step_zone_ids,
                is_sts_field,
            )
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
                KplerTradeComputed,
                sa.and_(
                    KplerTradeComputed.trade_id == KplerTrade.id,
                    KplerTradeComputed.flow_id == KplerTrade.flow_id,
                    KplerTradeComputed.product_id == KplerTrade.product_id,
                    KplerTradeComputed.eur_per_tonne != None,
                ),
            )
            .join(Commodity, KplerTradeComputed.kpler_product_commodity_id == Commodity.id)
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
                KplerTradeComputed.pricing_scenario,
                Currency.currency,
            )
        )

        # Only keep valid trades
        query = query.filter(KplerTrade.is_valid == True)

        return query

    def filter(self, query, params=None):
        origin_iso2 = params.get("origin_iso2")
        origin_region = params.get("origin_region")
        origin_port_name = params.get("origin_port_name")
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
            query = query.filter(KplerTradeComputed.pricing_scenario.in_(to_list(pricing_scenario)))

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

        return query
