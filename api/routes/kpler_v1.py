import json
import pandas as pd
import numpy as np
import datetime as dt
import re
import pytz
import datetime as dt
from flask import Response
from flask_restx import Resource, reqparse, inputs
import pymongo
from sqlalchemy import func
import sqlalchemy as sa
from sqlalchemy.orm import aliased
from sqlalchemy import case


import base
from . import routes_api
from .template import TemplateResource
from base import PRICING_DEFAULT
from base.logger import logger
from base.db import session
from base.models import KplerFlow2, KplerProduct, Country, Price, Currency
from base.utils import to_datetime, to_list, intersect, df_to_json

KPLER_TOTAL = "Total"


@routes_api.route("/v1/kpler_flow", strict_slashes=False, doc=False)
class KplerFlowResource(TemplateResource):
    parser = TemplateResource.parser.copy()

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
        "origin_by",
        help="Split origin by either country or port",
        required=False,
        action="split",
        default=["country"],
    )

    parser.add_argument(
        "destination_by",
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

    must_group_by = ["unit"]
    date_cols = ["date"]
    value_cols = ["value"]
    pivot_dependencies = {}
    filename = "kpler_flow"

    def get_aggregate_cols_dict(self, subquery):
        return {}

    def get_agg_value_cols(self, subquery):
        return []

    @routes_api.expect(parser)
    def get(self):
        params = KplerFlowResource.parser.parse_args()
        return self.get_from_params(params)

    def initial_query(self, params=None):

        FromCountry = aliased(Country)
        ToCountry = aliased(Country)

        value_tonne_field = case(
            [(KplerFlow2.unit == "t", KplerFlow2.value)], else_=sa.null()
        ).label("value_tonne")

        value_eur_field = (value_tonne_field * Price.eur_per_tonne).label("value_eur")

        query = (
            session.query(
                KplerFlow2.from_iso2.label("origin_iso2"),
                FromCountry.name.label("origin_country"),
                FromCountry.region.label("origin_region"),
                KplerFlow2.from_split.label("origin_type"),
                KplerFlow2.from_zone_name.label("origin_name"),
                KplerFlow2.to_iso2.label("destination_iso2"),
                ToCountry.name.label("destination_country"),
                ToCountry.region.label("destination_region"),
                KplerFlow2.to_split.label("destination_type"),
                KplerFlow2.to_zone_name.label("destination_name"),
                KplerFlow2.date,
                KplerFlow2.product.label("product"),
                KplerProduct.group.label("product_group"),
                KplerProduct.family.label("product_family"),
                Price.scenario.label("pricing_scenario"),
                value_tonne_field,
                value_eur_field,
                Currency.currency,
                (value_eur_field * Currency.per_eur).label("value_currency"),
            )
            .outerjoin(
                FromCountry,
                FromCountry.iso2 == KplerFlow2.from_iso2,
            )
            .outerjoin(
                ToCountry,
                ToCountry.iso2 == KplerFlow2.to_iso2,
            )
            .outerjoin(
                KplerProduct,
                sa.and_(
                    KplerProduct.name == KplerFlow2.product,
                    KplerProduct.platform == KplerFlow2.platform,
                ),
            )
            .outerjoin(
                Price,
                sa.and_(
                    Price.date == KplerFlow2.date,
                    sa.or_(
                        KplerFlow2.to_iso2 == sa.any_(Price.destination_iso2s),
                        Price.destination_iso2s == base.PRICE_NULLARRAY_CHAR,
                    ),
                    Price.departure_port_ids == base.PRICE_NULLARRAY_INT,
                    Price.ship_owner_iso2s == base.PRICE_NULLARRAY_CHAR,
                    Price.ship_owner_iso2s == base.PRICE_NULLARRAY_CHAR,
                    sa.or_(
                        sa.and_(Price.commodity == "crude_oil", KplerProduct.family.in_(["Dirty"])),
                        sa.and_(
                            Price.commodity == "oil_products",
                            sa.or_(
                                KplerProduct.group.in_(["Fuel Oils"]),
                                KplerProduct.family.in_(["Light Ends", "Middle Distillates"]),
                            ),
                        ),
                        sa.and_(Price.commodity == "lng", KplerProduct.name.in_(["lng"])),
                    ),
                ),
            )
            .outerjoin(Currency, Currency.date == KplerFlow2.date)
            .order_by(
                KplerFlow2.id,
                Price.scenario,
                Currency.currency,
                # Price.departure_port_ids,
                Price.destination_iso2s,
                # Price.ship_insurer_iso2s,
                # Price.ship_owner_iso2s,
            )
            .distinct(
                KplerFlow2.id,
                Price.scenario,
                Currency.currency,
            )
        )
        return query

    def filter(self, query, params=None):
        origin_iso2 = params.get("origin_iso2")
        destination_iso2 = params.get("destination_iso2")
        origin_by = params.get("origin_by")
        destination_by = params.get("destination_by")
        product = params.get("product")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        platform = params.get("platform")
        pricing_scenario = params.get("pricing_scenario")
        currency = params.get("currency")

        if origin_iso2:
            query = query.filter(KplerFlow2.from_iso2.in_(to_list(origin_iso2)))

        if destination_iso2:
            query = query.filter(KplerFlow2.to_iso2.in_(to_list(destination_iso2)))

        if product:
            query = query.filter(KplerProduct.name.in_(to_list(product)))

        if origin_by:
            query = query.filter(KplerFlow2.from_split.in_(to_list(origin_by)))

        if destination_by:
            query = query.filter(KplerFlow2.to_split.in_(to_list(destination_by)))

        if platform:
            query = query.filter(KplerFlow2.platform.in_(to_list(platform)))

        if date_from:
            query = query.filter(KplerFlow2.date >= to_datetime(date_from))

        if date_to:
            query = query.filter(KplerFlow2.date <= to_datetime(date_to))

        if pricing_scenario:
            query = query.filter(Price.scenario.in_(to_list(pricing_scenario)))

        if currency is not None:
            query = query.filter(Currency.currency.in_(to_list(currency)))

        return query
