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
from base.models import KplerFlow, KplerProduct, Country, Price, Currency, Commodity
from base.utils import to_datetime, to_list, intersect, df_to_json

KPLER_TOTAL = "Total"


@routes_api.route(
    "/v1/kpler_flow",
    strict_slashes=False,
    doc={
        "deprecated": True,
        "description": "This route is deprecated, use /v1/kpler_trade with aggregate_by=origin_date,flow_origin_country,flow_destination_country,grade,pricing_scenario instead.",
    },
)
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
        "origin_type",
        help="Split origin by either country or port",
        required=False,
        action="split",
        default=["country"],
    )

    parser.add_argument(
        "destination_type",
        help="Split destination by either country or port",
        required=False,
        action="split",
        default=["country"],
    )

    parser.add_argument(
        "date_from",
        type=str,
        help="start date (format 2020-01-01)",
        default="2020-01-01",
        required=False,
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

    must_group_by = ["origin_type", "destination_type", "currency", "pricing_scenario"]
    date_cols = ["date"]
    value_cols = ["value_tonne", "value_eur", "value_currency"]
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
    filename = "kpler_flow"

    def get_aggregate_cols_dict(self, subquery):
        return {
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
                subquery.c.origin_name,
                subquery.c.origin_iso2,
                subquery.c.origin_country,
                subquery.c.origin_region,
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
            "destination": [subquery.c.destination_name],
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
            "origin_type": [subquery.c.origin_type],
            "destination_type": [subquery.c.destination_type],
            "currency": [subquery.c.currency],
            "date": [subquery.c.date],
            # date_trunc month
            "month": [func.date_trunc("month", subquery.c.date).label("month")],
            "year": [func.extract("year", subquery.c.date).label("year")],
            "pricing_scenario": [subquery.c.pricing_scenario],
        }

    def get_agg_value_cols(self, subquery):
        return [
            func.sum(subquery.c.value_tonne).label("value_tonne"),
            func.sum(subquery.c.value_eur).label("value_eur"),
            func.sum(subquery.c.value_currency).label("value_currency"),
        ]

    @routes_api.expect(parser)
    @key_required
    def get(self):
        params = KplerFlowResource.parser.parse_args(strict=True)
        return self.get_from_params(params)

    def initial_query(self, params=None):

        FromCountry = aliased(Country)
        CommodityOriginCountry = aliased(Country)

        ToCountry = aliased(Country)
        CommodityDestinationCountry = aliased(Country)
        CommodityEquivalent = aliased(Commodity)

        commodity_origin_iso2_field = case(
            [
                (KplerFlow.grade.in_(["CPC Kazakhstan", "KEBCO"]), "KZ"),
            ],
            else_=KplerFlow.from_iso2,
        ).label("commodity_origin_iso2")

        commodity_destination_iso2_field = KplerFlow.to_iso2.label("commodity_destination_iso2")

        value_tonne_field = case([(KplerFlow.unit == "t", KplerFlow.value)], else_=sa.null()).label(
            "value_tonne"
        )

        value_eur_field = (value_tonne_field * Price.eur_per_tonne).label("value_eur")

        # Commodity used for pricing
        commodity_id_field = (
            "kpler_"
            + sa.func.replace(
                sa.func.replace(
                    sa.func.lower(func.coalesce(KplerFlow.commodity, KplerFlow.group)), " ", "_"
                ),
                "/",
                "_",
            )
        ).label("commodity")

        pricing_commodity_id_field = case(
            [
                (
                    sa.and_(
                        KplerFlow.group == "Crude/Co",
                        KplerFlow.from_iso2 == "RU",
                        KplerFlow.grade.notin_(["CPC Kazakhstan", "KEBCO"]),
                        KplerFlow.from_zone_name.op("~*")("^Nakhodka|^De Kast|^Prigorod"),
                    ),
                    "crude_oil_espo",
                ),
                (
                    sa.and_(
                        KplerFlow.group == "Crude/Co",
                        KplerFlow.from_iso2 == "RU",
                        KplerFlow.grade.notin_(["CPC Kazakhstan", "KEBCO"]),
                    ),
                    "crude_oil_urals",
                ),
            ],
            else_=Commodity.pricing_commodity,
        ).label("pricing_commodity")

        query = (
            session.query(
                KplerFlow.from_iso2.label("origin_iso2"),
                FromCountry.name.label("origin_country"),
                FromCountry.region.label("origin_region"),
                KplerFlow.from_split.label("origin_type"),
                KplerFlow.from_zone_name.label("origin_name"),
                KplerFlow.to_iso2.label("destination_iso2"),
                ToCountry.name.label("destination_country"),
                ToCountry.region.label("destination_region"),
                # Commodity origin and destination
                commodity_origin_iso2_field,
                CommodityOriginCountry.name.label("commodity_origin_country"),
                CommodityOriginCountry.region.label("commodity_origin_region"),
                commodity_destination_iso2_field,
                CommodityDestinationCountry.name.label("commodity_destination_country"),
                CommodityDestinationCountry.region.label("commodity_destination_region"),
                KplerFlow.to_split.label("destination_type"),
                KplerFlow.to_zone_name.label("destination_name"),
                KplerFlow.date,
                # KplerFlow.product.label("product"),
                KplerFlow.grade.label("grade"),
                KplerFlow.commodity.label("commodity"),
                KplerFlow.group.label("group"),
                KplerFlow.family.label("family"),
                Price.scenario.label("pricing_scenario"),
                value_tonne_field,
                value_eur_field,
                Currency.currency,
                (value_eur_field * Currency.per_eur).label("value_currency"),
                # Commodity.name.label("commodity"),
                # Commodity.group.label("commodity_group"),
                Commodity.equivalent_id.label("commodity_equivalent"),  # For filtering
                CommodityEquivalent.name.label("commodity_equivalent_name"),
                CommodityEquivalent.group.label("commodity_equivalent_group"),
                pricing_commodity_id_field,
            )
            .outerjoin(
                FromCountry,
                FromCountry.iso2 == KplerFlow.from_iso2,
            )
            .outerjoin(
                ToCountry,
                ToCountry.iso2 == KplerFlow.to_iso2,
            )
            .outerjoin(
                CommodityOriginCountry,
                CommodityOriginCountry.iso2 == commodity_origin_iso2_field,
            )
            .outerjoin(
                CommodityDestinationCountry,
                CommodityDestinationCountry.iso2 == commodity_destination_iso2_field,
            )
            # join to avoid double counting (we collected both by product and by group)
            # but this isn't perfect, as sometimes, kpler is using group when not knowing product
            # .outerjoin(
            #     KplerProduct,
            #     sa.and_(
            #         sa.or_(
            #             KplerProduct.name == KplerFlow.product,
            #             # TODO CHECK this is not creating double counting
            #             # or removing rows because of no pricing for the group
            #             KplerProduct.group == KplerFlow.product,
            #         ),
            #         KplerProduct.platform == KplerFlow.platform,
            #     ),
            # )
            .join(Commodity, commodity_id_field == Commodity.id)
            .join(CommodityEquivalent, Commodity.equivalent_id == CommodityEquivalent.id)
            .join(
                Price,
                sa.and_(
                    Price.date == KplerFlow.date,
                    sa.or_(
                        KplerFlow.to_iso2 == sa.any_(Price.destination_iso2s),
                        Price.destination_iso2s == base.PRICE_NULLARRAY_CHAR,
                    ),
                    Price.departure_port_ids == base.PRICE_NULLARRAY_INT,
                    Price.ship_owner_iso2s == base.PRICE_NULLARRAY_CHAR,
                    Price.ship_owner_iso2s == base.PRICE_NULLARRAY_CHAR,
                    Price.commodity == pricing_commodity_id_field,
                ),
            )
            .outerjoin(Currency, Currency.date == KplerFlow.date)
            .order_by(
                KplerFlow.id,
                Price.scenario,
                Currency.currency,
                Price.destination_iso2s,
            )
            .distinct(
                KplerFlow.id,
                Price.scenario,
                Currency.currency,
            )
        )

        # Only keep valid flows
        query = query.filter(KplerFlow.is_valid == True)

        return query

    def filter(self, query, params=None):

        origin_iso2 = params.get("origin_iso2")
        commodity_origin_iso2 = params.get("commodity_origin_iso2")
        destination_iso2 = params.get("destination_iso2")
        commodity_destination_iso2 = params.get("commodity_destination_iso2")
        destination_region = params.get("destination_region")
        origin_type = params.get("origin_type")
        destination_type = params.get("destination_type")

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

        if origin_iso2:
            query = query.filter(KplerFlow.from_iso2.in_(to_list(origin_iso2)))

        if destination_iso2:
            query = query.filter(KplerFlow.to_iso2.in_(to_list(destination_iso2)))

        if grade:
            query = query.filter(KplerFlow.grade.in_(to_list(grade)))

        if commodity:
            query = query.filter(KplerFlow.commodity.in_(to_list(commodity)))

        if group:
            query = query.filter(KplerFlow.group.in_(to_list(group)))

        if family:
            query = query.filter(KplerFlow.family.in_(to_list(family)))

        if origin_type:
            query = query.filter(KplerFlow.from_split.in_(to_list(origin_type)))

        if destination_type:
            query = query.filter(KplerFlow.to_split.in_(to_list(destination_type)))

        if platform:
            query = query.filter(KplerFlow.platform.in_(to_list(platform)))

        if date_from:
            query = query.filter(KplerFlow.date >= str(to_datetime(date_from)))

        if date_to:
            query = query.filter(KplerFlow.date <= str(to_datetime(date_to)))

        if pricing_scenario:
            query = query.filter(Price.scenario.in_(to_list(pricing_scenario)))

        if currency is not None:
            query = query.filter(Currency.currency.in_(to_list(currency)))

        subquery = query.subquery()
        query = session.query(subquery)

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

        return query
