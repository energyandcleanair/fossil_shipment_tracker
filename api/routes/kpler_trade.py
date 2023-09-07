from sqlalchemy import func
import sqlalchemy as sa
from sqlalchemy.orm import aliased
from sqlalchemy import case
from sqlalchemy import nullslast
from sqlalchemy import any_
from flask_restx import inputs
from sqlalchemy import true
from sqlalchemy.dialects.postgresql import aggregate_order_by

import datetime as dt

import base
from base import UNKNOWN_INSURER
from .security import key_required
from . import routes_api
from .template import TemplateResource
from base import PRICING_DEFAULT
from base import UNKNOWN_INSURER
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
    Company,
    ShipInsurer,
    ShipOwner,
)
from base.utils import to_datetime, to_list, intersect, df_to_json


@routes_api.route("/v1/kpler_trade", strict_slashes=False)
class KplerTradeResource(TemplateResource):
    parser = TemplateResource.parser.copy()

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
            "commodity_origin_country": [
                subquery.c.commodity_origin_iso2,
                subquery.c.commodity_origin_country,
                # subquery.c.commodity_origin_region,
            ],
            "commodity_origin_iso2": [
                subquery.c.commodity_origin_iso2,
                subquery.c.commodity_origin_country,
                # subquery.c.commodity_origin_region,
            ],
            # "origin": [
            #     subquery.c.origin_name,
            #     subquery.c.origin_iso2,
            #     subquery.c.origin_country,
            #     subquery.c.origin_region,
            # ],
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
            "destination": [
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
            "destination_date": [
                func.date_trunc("day", subquery.c.destination_date_utc).label("date")
            ],
            "destination_month": [
                func.date_trunc("month", subquery.c.destination_date_utc).label("month")
            ],
            "destination_year": [
                func.extract("year", subquery.c.destination_date_utc).label("year")
            ],
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
        params = KplerTradeResource.parser.parse_args(strict=True)
        return self.get_from_params(params)

    def initial_query(self, params=None):

        origin_zone = aliased(KplerZone)
        destination_zone = aliased(KplerZone)
        CommodityEquivalent = aliased(Commodity)
        CommodityOriginCountry = aliased(Country)
        CommodityDestinationCountry = aliased(Country)

        price_date = func.date_trunc("day", KplerTrade.departure_date_utc)

        unnested_vessels = (
            func.unnest(KplerTrade.vessel_imos)
            .table_valued(
                "ship_imo", with_ordinality="ship_order", name="ships", joins_implicitly=False
            )
            .render_derived()
        )

        # This gives us the vessel IMOs as a table
        # to make other queries possible. We sort
        # to get the best performance.
        trade_ship = (
            session.query(
                KplerTrade.id.label("trade_id"),
                KplerTrade.flow_id,
                unnested_vessels.c.ship_order,
                unnested_vessels.c.ship_imo,
            )
            .select_from(KplerTrade)
            .join(unnested_vessels, true())
            .order_by(KplerTrade.id, KplerTrade.flow_id, unnested_vessels.c.ship_order)
            .cte("trade_ship")
            .prefix_with("MATERIALIZED")
        )

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

        commodity_origin_iso2_field = case(
            [
                (KplerProduct.grade_name.in_(["CPC Kazakhstan", "KEBCO"]), "KZ"),
            ],
            else_=origin_zone.country_iso2,
        ).label("commodity_origin_iso2")

        insurance_buffer_days = 14

        # We get the earliest (but starting before the
        # departure date) insurance per trade's ship.
        voyage_insurer = (
            session.query(
                KplerTrade.id.label("trade_id"),
                KplerTrade.flow_id,
                trade_ship.c.ship_imo,
                trade_ship.c.ship_order,
                func.coalesce(Company.name, "unknown").label("name"),
                func.coalesce(Company.country_iso2, "unknown").label("iso2"),
                func.coalesce(Country.region, "unknown").label("region"),
            )
            .outerjoin(
                trade_ship,
                sa.and_(
                    KplerTrade.id == trade_ship.c.trade_id,
                    KplerTrade.flow_id == trade_ship.c.flow_id,
                ),
            )
            .outerjoin(
                ShipInsurer,
                sa.and_(
                    ShipInsurer.ship_imo == trade_ship.c.ship_imo,
                    sa.or_(
                        ShipInsurer.date_from
                        <= KplerTrade.departure_date_utc + dt.timedelta(days=insurance_buffer_days),
                        ShipInsurer.date_from == None,
                    ),
                ),
            )
            .outerjoin(Company, ShipInsurer.company_id == Company.id)
            .outerjoin(Country, Company.country_iso2 == Country.iso2)
            .distinct(KplerTrade.id, KplerTrade.flow_id, trade_ship.c.ship_imo)
            .order_by(
                KplerTrade.id,
                KplerTrade.flow_id,
                trade_ship.c.ship_imo,
                nullslast(ShipInsurer.date_from),
            )
            .cte("voyage_insurer")
            .prefix_with("MATERIALIZED")
        )

        # We get the earliest (but starting before the
        # departure date) owner per trade's ship.
        voyage_owner = (
            session.query(
                KplerTrade.id.label("trade_id"),
                KplerTrade.flow_id,
                trade_ship.c.ship_imo,
                trade_ship.c.ship_order,
                func.coalesce(Company.name, "unknown").label("name"),
                func.coalesce(Company.country_iso2, "unknown").label("iso2"),
                func.coalesce(Country.region, "unknown").label("region"),
            )
            .outerjoin(
                trade_ship,
                sa.and_(
                    KplerTrade.id == trade_ship.c.trade_id,
                    KplerTrade.flow_id == trade_ship.c.flow_id,
                ),
            )
            .outerjoin(
                ShipOwner,
                sa.and_(
                    ShipOwner.ship_imo == trade_ship.c.ship_imo,
                    sa.or_(
                        ShipOwner.date_from
                        <= KplerTrade.departure_date_utc + dt.timedelta(days=insurance_buffer_days),
                        ShipOwner.date_from == None,
                    ),
                ),
            )
            .outerjoin(Company, ShipOwner.company_id == Company.id)
            .outerjoin(Country, Company.country_iso2 == Country.iso2)
            .distinct(
                KplerTrade.id,
                KplerTrade.flow_id,
                trade_ship.c.ship_imo,
            )
            .order_by(
                KplerTrade.id,
                KplerTrade.flow_id,
                trade_ship.c.ship_imo,
                nullslast(ShipOwner.date_from),
            )
            .cte("voyage_owner")
            .prefix_with("MATERIALIZED")
        )

        # We need a separate query to get the best matching
        # price per ship per commodity. Afterwards we can choose the
        # one for each ship/commodity with the lowest price (the one
        # that is capped, if applicable).
        trade_ship_price = (
            session.query(
                KplerTrade.id.label("trade_id"),
                KplerTrade.flow_id,
                trade_ship.c.ship_imo,
                Price.scenario,
                Price.id.label("price_id"),
            )
            .outerjoin(
                trade_ship,
                sa.and_(
                    KplerTrade.id == trade_ship.c.trade_id,
                    KplerTrade.flow_id == trade_ship.c.flow_id,
                ),
            )
            .join(KplerProduct, KplerTrade.product_id == KplerProduct.id)
            .outerjoin(Commodity, commodity_id_field == Commodity.id)
            .outerjoin(origin_zone, KplerTrade.departure_zone_id == origin_zone.id)
            .outerjoin(destination_zone, KplerTrade.arrival_zone_id == destination_zone.id)
            .outerjoin(
                voyage_insurer,
                sa.and_(
                    voyage_insurer.c.trade_id == KplerTrade.id,
                    voyage_insurer.c.flow_id == KplerTrade.flow_id,
                    voyage_insurer.c.ship_imo == trade_ship.c.ship_imo,
                ),
            )
            .outerjoin(
                voyage_owner,
                sa.and_(
                    voyage_owner.c.trade_id == KplerTrade.id,
                    voyage_owner.c.flow_id == KplerTrade.flow_id,
                    voyage_owner.c.ship_imo == trade_ship.c.ship_imo,
                ),
            )
            .join(
                Price,
                sa.and_(
                    pricing_commodity_id_field == Price.commodity,
                    Price.date == price_date,
                    sa.or_(
                        voyage_insurer.c.iso2 == any_(Price.ship_insurer_iso2s),
                        Price.ship_insurer_iso2s == base.PRICE_NULLARRAY_CHAR,
                    ),
                    sa.or_(
                        voyage_owner.c.iso2 == any_(Price.ship_owner_iso2s),
                        Price.ship_owner_iso2s == base.PRICE_NULLARRAY_CHAR,
                    ),
                    sa.or_(
                        destination_zone.country_iso2 == any_(Price.destination_iso2s),
                        Price.destination_iso2s == base.PRICE_NULLARRAY_CHAR,
                    ),
                    Price.departure_port_ids == base.PRICE_NULLARRAY_INT,
                ),
            )
            .distinct(KplerTrade.id, KplerTrade.flow_id, trade_ship.c.ship_imo, Price.scenario)
            .order_by(
                KplerTrade.id,
                KplerTrade.flow_id,
                trade_ship.c.ship_imo,
                Price.scenario,
                Price.destination_iso2s,
                Price.ship_insurer_iso2s,
                Price.ship_owner_iso2s,
            )
            .cte("trade_ship_price")
            .prefix_with("MATERIALIZED")
        )

        trade_price = (
            session.query(
                KplerTrade.id.label("trade_id"), KplerTrade.flow_id, Price.id.label("price_id")
            )
            .outerjoin(
                trade_ship_price,
                sa.and_(
                    KplerTrade.id == trade_ship_price.c.trade_id,
                    KplerTrade.flow_id == trade_ship_price.c.flow_id,
                ),
            )
            .join(Price, trade_ship_price.c.price_id == Price.id)
            .distinct(
                KplerTrade.id,
                KplerTrade.flow_id,
                Price.scenario,
            )
            .order_by(
                KplerTrade.id, KplerTrade.flow_id, Price.scenario, nullslast(Price.eur_per_tonne)
            )
            .cte("trade_price")
            .prefix_with("MATERIALIZED")
        )

        value_eur_field = (KplerTrade.value_tonne * Price.eur_per_tonne).label("value_eur")

        all_insurers_for_trade = (
            session.query(
                KplerTrade.id.label("trade_id"),
                KplerTrade.flow_id,
                func.array_agg(
                    aggregate_order_by(
                        func.coalesce(voyage_insurer.c.name, UNKNOWN_INSURER),
                        voyage_insurer.c.ship_order,
                    )
                ).label("ship_insurer_names"),
                func.array_agg(
                    aggregate_order_by(voyage_insurer.c.iso2, voyage_insurer.c.ship_order)
                ).label("ship_insurer_iso2s"),
                func.array_agg(
                    aggregate_order_by(voyage_insurer.c.region, voyage_insurer.c.ship_order)
                ).label("ship_insurer_regions"),
            )
            .outerjoin(
                trade_ship,
                sa.and_(
                    KplerTrade.id == trade_ship.c.trade_id,
                    KplerTrade.flow_id == trade_ship.c.flow_id,
                ),
            )
            .outerjoin(
                voyage_insurer,
                sa.and_(
                    voyage_insurer.c.trade_id == KplerTrade.id,
                    voyage_insurer.c.flow_id == KplerTrade.flow_id,
                    voyage_insurer.c.ship_imo == trade_ship.c.ship_imo,
                ),
            )
            .group_by(
                KplerTrade.id,
                KplerTrade.flow_id,
            )
            .cte("all_insurers_for_trade")
            .prefix_with("MATERIALIZED")
        )

        all_owners_for_trade = (
            session.query(
                KplerTrade.id.label("trade_id"),
                KplerTrade.flow_id,
                func.array_agg(
                    aggregate_order_by(
                        func.coalesce(voyage_owner.c.name, UNKNOWN_INSURER),
                        voyage_owner.c.ship_order,
                    )
                ).label("ship_owner_names"),
                func.array_agg(
                    aggregate_order_by(voyage_owner.c.iso2, voyage_owner.c.ship_order)
                ).label("ship_owner_iso2s"),
                func.array_agg(
                    aggregate_order_by(voyage_owner.c.region, voyage_owner.c.ship_order)
                ).label("ship_owner_regions"),
            )
            .outerjoin(
                trade_ship,
                sa.and_(
                    KplerTrade.id == trade_ship.c.trade_id,
                    KplerTrade.flow_id == trade_ship.c.flow_id,
                ),
            )
            .outerjoin(
                voyage_owner,
                sa.and_(
                    voyage_owner.c.trade_id == KplerTrade.id,
                    voyage_owner.c.flow_id == KplerTrade.flow_id,
                    voyage_owner.c.ship_imo == trade_ship.c.ship_imo,
                ),
            )
            .group_by(KplerTrade.id, KplerTrade.flow_id)
            .cte("all_owners_for_trade")
            .prefix_with("MATERIALIZED")
        )

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
                commodity_origin_iso2_field,
                CommodityOriginCountry.name.label("commodity_origin_country"),
                CommodityOriginCountry.region.label("commodity_origin_region"),
                KplerTrade.arrival_date_utc.label("destination_date_utc"),
                destination_zone.name.label("destination_zone_name"),
                destination_zone.name.label("destination_zone_type"),
                destination_zone.port_id.label("destination_port_id"),
                destination_zone.port_name.label("destination_port_name"),
                destination_zone.country_name.label("destination_country"),
                destination_zone.country_iso2.label("destination_iso2"),
                CommodityDestinationCountry.region.label("destination_region"),
                destination_zone.country_name.label("commodity_destination_country"),
                destination_zone.country_iso2.label("commodity_destination_iso2"),
                CommodityDestinationCountry.region.label("commodity_destination_region"),
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
                Price.commodity.label("pricing_commodity"),
                KplerTrade.vessel_imos,
                KplerTrade.buyer_names,
                KplerTrade.seller_names,
                all_insurers_for_trade.c.ship_insurer_names,
                all_insurers_for_trade.c.ship_insurer_iso2s,
                all_insurers_for_trade.c.ship_insurer_regions,
                all_owners_for_trade.c.ship_owner_names,
                all_owners_for_trade.c.ship_owner_iso2s,
                all_owners_for_trade.c.ship_owner_regions,
            )
            .outerjoin(KplerProduct, KplerTrade.product_id == KplerProduct.id)
            .join(origin_zone, KplerTrade.departure_zone_id == origin_zone.id)
            .outerjoin(destination_zone, KplerTrade.arrival_zone_id == destination_zone.id)
            .outerjoin(
                CommodityOriginCountry,
                CommodityOriginCountry.iso2 == commodity_origin_iso2_field,
            )
            .outerjoin(
                CommodityDestinationCountry,
                CommodityDestinationCountry.iso2 == destination_zone.country_iso2,
            )
            .join(Commodity, commodity_id_field == Commodity.id)
            .join(CommodityEquivalent, Commodity.equivalent_id == CommodityEquivalent.id)
            .join(
                all_insurers_for_trade,
                sa.and_(
                    KplerTrade.id == all_insurers_for_trade.c.trade_id,
                    KplerTrade.flow_id == all_insurers_for_trade.c.flow_id,
                ),
            )
            .join(
                all_owners_for_trade,
                sa.and_(
                    KplerTrade.id == all_owners_for_trade.c.trade_id,
                    KplerTrade.flow_id == all_owners_for_trade.c.flow_id,
                ),
            )
            .join(
                trade_price,
                sa.and_(
                    KplerTrade.id == trade_price.c.trade_id,
                    KplerTrade.flow_id == trade_price.c.flow_id,
                ),
            )
            .join(
                Price,
                sa.and_(
                    trade_price.c.price_id == Price.id,
                    pricing_commodity_id_field == Price.commodity,
                ),
            )
            .outerjoin(Currency, Currency.date == price_date)
            .order_by(KplerTrade.id, KplerTrade.flow_id, Price.scenario, Currency.currency)
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
        destination_port_name = params.get("destination_port_name")
        destination_region = params.get("destination_region")
        exclude_within_country = params.get("exclude_within_country")

        trade_ids = params.get("trade_ids")

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

        buyer = params.get("buyer")
        seller = params.get("seller")

        if trade_ids:
            query = query.filter(KplerTrade.id.in_(to_list(trade_ids)))

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

        if buyer:
            query = query.filter(KplerTrade.buyer_names.overlap(to_list(buyer)))

        if seller:
            query = query.filter(KplerTrade.seller_names.overlap(to_list(seller)))

        subquery = query.subquery()
        query = session.query(subquery)

        if origin_iso2:
            query = query.filter(subquery.c.origin_iso2.in_(to_list(origin_iso2)))

        if origin_port_name:
            query = query.filter(subquery.c.origin_port_name.in_(to_list(origin_port_name)))

        if destination_port_name:
            query = query.filter(
                subquery.c.destination_port_name.in_(to_list(destination_port_name))
            )

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
