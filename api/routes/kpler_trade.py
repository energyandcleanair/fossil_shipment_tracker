from sqlalchemy import func
import sqlalchemy as sa
from sqlalchemy.orm import aliased
from sqlalchemy import case


import base
from .security import key_required
from . import routes_api
from .template import TemplateResource
from base import PRICING_DEFAULT
from base.logger import logger
from base.db import session
from base.models import (
    KplerFlow,
    KplerProduct,
    Country,
    Price,
    Currency,
    Commodity,
    KplerTrade,
    KplerZone,
)
from base.utils import to_datetime, to_list, intersect, df_to_json


@routes_api.route("/v1/kpler_trade", strict_slashes=False)
class KplerFlowResource(TemplateResource):
    parser = TemplateResource.parser.copy()

    parser.replace_argument(
        "aggregate_by",
        type=str,
        action="split",
        default=None,
        help="which variables to aggregate by. Could be any of origin_country,origin,destination_country,destination,product,date,date,country,year",
    )

    parser.add_argument(
        "api_key", help="Key to use the endpoint", required=True, type=str, default=None
    )

    parser.add_argument(
        "origin_iso2", help="Origin iso2", required=False, action="split", default=None
    )

    parser.add_argument(
        "origin_port_name", help="Origin port name", required=False, action="split", default=None
    )

    parser.add_argument(
        "destination_iso2",
        help="Destination iso2",
        required=False,
        action="split",
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
        help="start date (format 2020-01-01)",
        default="2020-01-01",
        required=False,
    )

    parser.add_argument(
        "exclude_within_country",
        type=bool,
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

    parser.add_argument("date_to", type=str, help="End date", default=None, required=False)

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
        "platform",
        type=str,
        help="platform",
        default=None,
        required=False,
    )

    must_group_by = ["currency", "pricing_scenario"]
    date_cols = ["date"]
    value_cols = ["value_tonne", "value_m3", "value_eur", "value_currency"]

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
            "origin_country": [
                subquery.c.origin_iso2,
                subquery.c.origin_country,
                # subquery.c.origin_region,
            ],
            "origin_iso2": [
                subquery.c.origin_iso2,
                subquery.c.origin_country,
                # subquery.c.origin_region,
            ],
            # "commodity_origin_country": [
            #     subquery.c.commodity_origin_iso2,
            #     subquery.c.commodity_origin_country,
            #     subquery.c.commodity_origin_region,
            # ],
            # "commodity_origin_iso2": [
            #     subquery.c.commodity_origin_iso2,
            #     subquery.c.commodity_origin_country,
            #     subquery.c.commodity_origin_region,
            # ],
            # "origin": [
            #     subquery.c.origin_name,
            #     subquery.c.origin_iso2,
            #     subquery.c.origin_country,
            #     subquery.c.origin_region,
            # ],
            "destination_country": [
                subquery.c.destination_iso2,
                subquery.c.destination_country,
                # subquery.c.destination_region,
            ],
            "destination_iso2": [
                subquery.c.destination_iso2,
                subquery.c.destination_country,
                # subquery.c.destination_region,
            ],
            "commodity_destination_country": [
                subquery.c.commodity_destination_iso2,
                subquery.c.commodity_destination_country,
                # subquery.c.commodity_destination_region,
            ],
            "commodity_destination_iso2": [
                subquery.c.commodity_destination_iso2,
                subquery.c.commodity_destination_country,
                # subquery.c.commodity_destination_region,
            ],
            "destination": [
                subquery.c.destination_zone_name,
                subquery.c.destination_country,
                subquery.c.destination_iso2,
            ],
            "grade": [
                subquery.c.grade,
                subquery.c.commodity,
                subquery.c.group,
                subquery.c.family,
                subquery.c.commodity_equivalent,
                subquery.c.commodity_equivalent_group,
            ],
            "commodity": [
                subquery.c.commodity,
                subquery.c.group,
                subquery.c.family,
                subquery.c.commodity_equivalent,
                subquery.c.commodity_equivalent_group,
            ],
            "group": [
                subquery.c.group,
                subquery.c.commodity_equivalent,
                subquery.c.commodity_equivalent_group,
            ],
            "commodity_equivalent": [
                subquery.c.commodity_equivalent,
                subquery.c.commodity_equivalent_group,
            ],
            "currency": [subquery.c.currency],
            "origin_date": [func.date_trunc("day", subquery.c.origin_date_utc).label("date")],
            "origin_month": [func.date_trunc("month", subquery.c.origin_date_utc).label("month")],
            "origin_year": [func.extract("year", subquery.c.origin_date_utc).label("year")],
            "pricing_scenario": [subquery.c.pricing_scenario],
        }

    def get_agg_value_cols(self, subquery):
        return [
            func.sum(subquery.c.value_tonne).label("value_tonne"),
            func.sum(subquery.c.value_m3).label("value_m3"),
            func.sum(subquery.c.value_eur).label("value_eur"),
            func.sum(subquery.c.value_currency).label("value_currency"),
            # func.sum(subquery.c.value_energy).label("value_energy"),
            # func.sum(subquery.c.value_gas_m3).label("value_gas_m3")
        ]

    @routes_api.expect(parser)
    @key_required
    def get(self):
        params = KplerFlowResource.parser.parse_args(strict=True)
        return self.get_from_params(params)

    def initial_query(self, params=None):

        origin_zone = aliased(KplerZone)
        destination_zone = aliased(KplerZone)
        CommodityEquivalent = aliased(Commodity)
        price_date = func.date_trunc("day", KplerTrade.departure_date_utc)
        value_eur_field = (KplerTrade.value_tonne * Price.eur_per_tonne).label("value_eur")

        # Commodity used for pricing
        commodity_id_field = (
            "kpler_"
            + sa.func.replace(
                sa.func.replace(
                    sa.func.lower(
                        func.coalesce(KplerProduct.commodity_name, KplerProduct.group_name)
                    ),
                    " ",
                    "_",
                ),
                "/",
                "_",
            )
        ).label("commodity")

        pricing_commodity_id_field = case(
            [
                (
                    sa.and_(
                        KplerProduct.group_name == "Crude/Co",
                        origin_zone.country_iso2 == "RU",
                        KplerProduct.grade_name.notin_(["CPC Kazakhstan", "KEBCO"]),
                        origin_zone.port_name.op("~*")("^Nakhodka|^De Kast|^Prigorod"),
                    ),
                    "crude_oil_espo",
                ),
                (
                    sa.and_(
                        KplerProduct.group_name == "Crude/Co",
                        origin_zone.country_iso2 == "RU",
                        KplerProduct.grade_name.notin_(["CPC Kazakhstan", "KEBCO"]),
                    ),
                    "crude_oil_urals",
                ),
            ],
            else_=Commodity.pricing_commodity,
        ).label("pricing_commodity")

        query = (
            session.query(
                # Renaming everything in terms of "origin" and "destination"
                KplerTrade.id.label("trade_id"),
                KplerTrade.flow_id,
                KplerTrade.status,
                KplerTrade.departure_date_utc.label("origin_date_utc"),
                KplerTrade.departure_zone_id.label("origin_zone_id"),
                origin_zone.name.label("origin_zone_name"),
                origin_zone.name.label("origin_zone_type"),
                origin_zone.port_id.label("origin_port_id"),
                origin_zone.port_name.label("origin_port_name"),
                origin_zone.country_name.label("origin_country"),
                origin_zone.country_iso2.label("origin_iso2"),
                KplerTrade.arrival_date_utc.label("destination_date_utc"),
                destination_zone.name.label("destination_zone_name"),
                destination_zone.name.label("destination_zone_type"),
                destination_zone.port_id.label("destination_port_id"),
                destination_zone.port_name.label("destination_port_name"),
                destination_zone.country_name.label("destination_country"),
                destination_zone.country_iso2.label("destination_iso2"),
                destination_zone.country_name.label("commodity_destination_country"),
                destination_zone.country_iso2.label("commodity_destination_iso2"),
                KplerProduct.grade_name.label("grade"),
                KplerProduct.commodity_name.label("commodity"),
                KplerProduct.group_name.label("group"),
                KplerProduct.family_name.label("family"),
                Commodity.equivalent_id.label("commodity_equivalent"),  # For filtering
                CommodityEquivalent.name.label("commodity_equivalent_name"),
                CommodityEquivalent.group.label("commodity_equivalent_group"),
                Price.scenario.label("pricing_scenario"),
                KplerTrade.value_tonne,
                KplerTrade.value_m3,
                value_eur_field,
                Currency.currency,
                (value_eur_field * Currency.per_eur).label("value_currency"),
                pricing_commodity_id_field,
                KplerTrade.vessel_imos,
                KplerTrade.buyer_names,
                KplerTrade.seller_names,
            )
            .outerjoin(KplerProduct, KplerTrade.product_id == KplerProduct.id)
            .join(origin_zone, KplerTrade.departure_zone_id == origin_zone.id)
            .join(destination_zone, KplerTrade.arrival_zone_id == destination_zone.id)
            .join(Commodity, commodity_id_field == Commodity.id)
            .join(CommodityEquivalent, Commodity.equivalent_id == CommodityEquivalent.id)
            .join(
                Price,
                sa.and_(
                    Price.date == price_date,
                    sa.or_(
                        destination_zone.country_iso2 == sa.any_(Price.destination_iso2s),
                        Price.destination_iso2s == base.PRICE_NULLARRAY_CHAR,
                    ),
                    Price.departure_port_ids == base.PRICE_NULLARRAY_INT,
                    Price.ship_owner_iso2s == base.PRICE_NULLARRAY_CHAR,
                    Price.ship_insurer_iso2s == base.PRICE_NULLARRAY_CHAR,
                    Price.commodity == pricing_commodity_id_field,
                ),
            )
            .outerjoin(Currency, Currency.date == price_date)
            .order_by(
                KplerTrade.id,
                KplerTrade.flow_id,
                Price.scenario,
                Currency.currency,
                Price.destination_iso2s,
            )
            .distinct(
                KplerTrade.id,
                KplerTrade.flow_id,
                Price.scenario,
                Currency.currency,
            )
        )

        # Only keep valid trades
        query = query.filter(KplerTrade.is_valid == True)

        return query

    def filter(self, query, params=None):

        origin_iso2 = params.get("origin_iso2")
        origin_port_name = params.get("origin_port_name")
        commodity_origin_iso2 = params.get("commodity_origin_iso2")
        destination_iso2 = params.get("destination_iso2")
        commodity_destination_iso2 = params.get("commodity_destination_iso2")
        destination_region = params.get("destination_region")
        exclude_within_country = params.get("exclude_within_country")

        grade = params.get("grade")
        commodity = params.get("commodity")
        group = params.get("group")
        family = params.get("family")
        commodity_equivalent = params.get("commodity_equivalent")

        date_from = params.get("date_from")
        date_to = params.get("date_to")
        platform = params.get("platform")
        pricing_scenario = params.get("pricing_scenario")
        currency = params.get("currency")

        if grade:
            query = query.filter(KplerTrade.grade_name.in_(to_list(grade)))

        if platform:
            query = query.filter(KplerTrade.platform.in_(to_list(platform)))

        if date_from:
            query = query.filter(KplerTrade.departure_date_utc >= str(to_datetime(date_from)))

        if date_to:
            query = query.filter(
                func.date_trunc("day", KplerTrade.departure_date_utc) <= to_datetime(date_to)
            )

        if pricing_scenario:
            query = query.filter(Price.scenario.in_(to_list(pricing_scenario)))

        if currency is not None:
            query = query.filter(Currency.currency.in_(to_list(currency)))

        subquery = query.subquery()
        query = session.query(subquery)

        if origin_iso2:
            query = query.filter(subquery.c.origin_iso2.in_(to_list(origin_iso2)))

        if origin_port_name:
            query = query.filter(subquery.c.origin_port_name.in_(to_list(origin_port_name)))

        if destination_iso2:
            query = query.filter(subquery.c.destination_iso2.in_(to_list(destination_iso2)))

        if commodity:
            query = query.filter(subquery.c.commodity.in_(to_list(commodity)))

        if group:
            query = query.filter(subquery.c.group.in_(to_list(group)))

        if family:
            query = query.filter(subquery.c.family_name.in_(to_list(family)))

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

        return query
