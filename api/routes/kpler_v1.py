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


@routes_api.route("/v1/kpler_flow", strict_slashes=False)
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

    parser.add_argument("product", help="Product", required=False, action="split", default=None)
    parser.add_argument(
        "commodity",
        help="Commodity using CREA's nomenclature: either crude_oil, oil_products, or lng",
        required=False,
        action="split",
        default=None,
    )

    parser.add_argument(
        "from_split",
        type=str,
        help="How to split departures: Can be any of country,port,installation",
        default=False,
        required=False,
    )

    parser.add_argument(
        "to_split",
        type=str,
        help="How to split arrivals: Can be any of country,port,installation",
        default=False,
        required=False,
    )

    parser.add_argument(
        "platform",
        type=str,
        help="platform",
        default=None,
        required=False,
    )

    must_group_by = ["origin_type", "destination_type", "currency"]
    date_cols = ["date"]
    value_cols = ["value_tonne", "value_eur", "value_currency"]
    pivot_dependencies = {
        "product": ["product_group", "product_family", "commodity"],
        "origin_country": ["origin_iso2", "origin_region"],
        "origin_iso2": ["origin_country", "origin_region"],
        "destination_country": ["destination_iso2", "destination_region"],
        "destination_iso2": ["destination_country", "destination_region"],
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
            "destination": [subquery.c.destination_name],
            "product": [
                subquery.c.product,
                subquery.c.product_group,
                subquery.c.product_family,
                subquery.c.commodity,
                subquery.c.commodity_equivalent,
            ],
            "origin_type": [subquery.c.origin_type],
            "destination_type": [subquery.c.destination_type],
            "currency": [subquery.c.currency],
            "date": [subquery.c.date],
            "month": [func.month(subquery.c.date).label("month")],
            "year": [func.extract("year", subquery.c.date).label("year")],
            "commodity": [subquery.c.commodity],
            "commodity_equivalent": [subquery.c.commodity_equivalent],
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
        params = KplerFlowResource.parser.parse_args()
        return self.get_from_params(params)

    def initial_query(self, params=None):

        FromCountry = aliased(Country)
        ToCountry = aliased(Country)
        CommodityEquivalent = aliased(Commodity)

        value_tonne_field = case([(KplerFlow.unit == "t", KplerFlow.value)], else_=sa.null()).label(
            "value_tonne"
        )

        value_eur_field = (value_tonne_field * Price.eur_per_tonne).label("value_eur")
        commodity_id_field = sa.case(
            [
                (KplerFlow.product == "lng", "lng"),
                (KplerFlow.product.in_(["Coal", "Thermal", "Metallurgical"]), "coal"),
            ],
            else_="kpler_"
            + sa.func.replace(
                sa.func.replace(sa.func.lower(KplerFlow.product), " ", "_"), "/", "_"
            ),
        ).label("commodity")

        commodity_equivalent_id_field = case(
            [
                (
                    sa.and_(KplerProduct.family.in_(["Dirty"]), KplerFlow.product != "Condensate"),
                    "crude_oil",
                ),
                (
                    sa.and_(
                        sa.or_(
                            KplerProduct.group.in_(["Fuel Oils"]),
                            KplerProduct.family.in_(["Light Ends", "Middle Distillates"]),
                        ),
                        KplerFlow.product != "Clean Condensate",
                    ),
                    "oil_products",
                ),
                (KplerProduct.name.in_(["lng"]), "lng"),
                (KplerProduct.name.in_(["Coal", "Thermal", "Metallurgical"]), "coal"),
            ],
            else_="others",
        ).label("commodity_equivalent")

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
                KplerFlow.to_split.label("destination_type"),
                KplerFlow.to_zone_name.label("destination_name"),
                KplerFlow.date,
                KplerFlow.product.label("product"),
                KplerProduct.group.label("product_group"),
                KplerProduct.family.label("product_family"),
                Price.scenario.label("pricing_scenario"),
                value_tonne_field,
                value_eur_field,
                Currency.currency,
                (value_eur_field * Currency.per_eur).label("value_currency"),
                Commodity.name.label("commodity"),
                commodity_equivalent_id_field,  # For filtering
                CommodityEquivalent.name.label("commodity_equivalent_name"),
            )
            .outerjoin(
                FromCountry,
                FromCountry.iso2 == KplerFlow.from_iso2,
            )
            .outerjoin(
                ToCountry,
                ToCountry.iso2 == KplerFlow.to_iso2,
            )
            # join to avoid double counting (we collected both by product and by group)
            # but this isn't perfect, as sometimes, kpler is using group when not knowing product
            .outerjoin(
                KplerProduct,
                sa.and_(
                    sa.or_(
                        KplerProduct.name == KplerFlow.product,
                        KplerProduct.group == KplerFlow.product,
                    ),
                    KplerProduct.platform == KplerFlow.platform,
                ),
            )
            .join(Commodity, commodity_id_field == Commodity.id)
            .join(CommodityEquivalent, commodity_equivalent_id_field == CommodityEquivalent.id)
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
                    Price.commodity == Commodity.pricing_commodity,
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
        destination_iso2 = params.get("destination_iso2")
        destination_region = params.get("destination_region")
        origin_type = params.get("origin_type")
        destination_type = params.get("destination_type")
        product = params.get("product")
        commodity = params.get("commodity")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        platform = params.get("platform")
        pricing_scenario = params.get("pricing_scenario")
        currency = params.get("currency")

        if origin_iso2:
            query = query.filter(KplerFlow.from_iso2.in_(to_list(origin_iso2)))

        if destination_iso2:
            query = query.filter(KplerFlow.to_iso2.in_(to_list(destination_iso2)))

        if product:
            query = query.filter(KplerFlow.product.in_(to_list(product)))

        if origin_type:
            query = query.filter(KplerFlow.from_split.in_(to_list(origin_type)))

        if destination_type:
            query = query.filter(KplerFlow.to_split.in_(to_list(destination_type)))

        if platform:
            query = query.filter(KplerFlow.platform.in_(to_list(platform)))

        if date_from:
            query = query.filter(KplerFlow.date >= to_datetime(date_from))

        if date_to:
            query = query.filter(KplerFlow.date <= to_datetime(date_to))

        if pricing_scenario:
            query = query.filter(Price.scenario.in_(to_list(pricing_scenario)))

        if currency is not None:
            query = query.filter(Currency.currency.in_(to_list(currency)))

        subquery = query.subquery()
        query = session.query(subquery)

        if commodity:
            query = query.filter(subquery.c.commodity_equivalent.in_(to_list(commodity)))

        if destination_region:
            query = query.filter(subquery.c.destination_region.in_(to_list(destination_region)))

        return query
