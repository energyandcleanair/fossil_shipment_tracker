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
from base.models import KplerFlow, KplerProduct, Country, Price, Currency
from base.utils import to_datetime, to_list, intersect, df_to_json

KPLER_TOTAL = "Total"


@routes_api.route("/v0/kpler_flow", strict_slashes=False, doc=False)
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
        "date_from",
        type=str,
        help="start date (format 2020-01-15)",
        default="2018-01-01",
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
        "from_installation", help="From installation", required=False, action="split", default=None
    )

    parser.add_argument(
        "by_installation",
        type=inputs.boolean,
        help="Whether to get flows by installation (i.e. refinery/port) or by country",
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
        DestinationCountry = aliased(Country)
        OriginCountry = aliased(Country)

        value_tonne_field = case([(KplerFlow.unit == "t", KplerFlow.value)], else_=sa.null()).label(
            "value_tonne"
        )

        value_eur_field = (value_tonne_field * Price.eur_per_tonne).label("value_eur")

        query = (
            session.query(
                KplerFlow,
                KplerProduct.group.label("product_group"),
                KplerProduct.family.label("product_family"),
                OriginCountry.name.label("origin_country"),
                OriginCountry.region.label("origin_region"),
                DestinationCountry.name.label("destination_country"),
                DestinationCountry.region.label("destination_region"),
                Price.scenario.label("pricing_scenario"),
                value_eur_field,
                Currency.currency,
                (value_eur_field * Currency.per_eur).label("value_currency"),
            )
            .outerjoin(
                OriginCountry,
                OriginCountry.iso2 == KplerFlow.origin_iso2,
            )
            .outerjoin(
                DestinationCountry,
                DestinationCountry.iso2 == KplerFlow.destination_iso2,
            )
            .outerjoin(
                KplerProduct,
                sa.and_(
                    KplerProduct.platform == KplerFlow.platform,
                    sa.or_(
                        KplerProduct.name == KplerFlow.product,
                        # Sometimes, Kpler only knows the product group
                        # We join name <> group and relies on the distinct below
                        # to remove duplicates
                        KplerProduct.name == KplerFlow.Group,
                    ),
                ),
            )
            .outerjoin(
                Price,
                sa.and_(
                    Price.date == func.date_trunc("day", KplerFlow.date),
                    sa.or_(
                        KplerFlow.destination_iso2 == sa.any_(Price.destination_iso2s),
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
            .outerjoin(Currency, Currency.date == func.date_trunc("day", KplerFlow.date))
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
        return query

    def filter(self, query, params=None):
        origin_iso2 = params.get("origin_iso2")
        destination_iso2 = params.get("destination_iso2")
        product = params.get("product")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        platform = params.get("platform")
        by_installation = params.get("by_installation")
        from_installation = params.get("from_installation")
        pricing_scenario = params.get("pricing_scenario")
        currency = params.get("currency")

        if origin_iso2:
            query = query.filter(KplerFlow.origin_iso2.in_(to_list(origin_iso2)))

        if destination_iso2:
            query = query.filter(KplerFlow.destination_iso2.in_(to_list(destination_iso2)))

        if product:
            query = query.filter(KplerProduct.name.in_(to_list(product)))

        if from_installation:
            query = query.filter(KplerFlow.from_installation.in_(to_list(from_installation)))

        if by_installation:
            query = query.filter(KplerFlow.from_installation != KPLER_TOTAL)
        else:
            query = query.filter(KplerFlow.from_installation == KPLER_TOTAL)

        # To be fixed like from_installation once we have full data
        query = query.filter(KplerFlow.to_installation == KPLER_TOTAL)

        if platform:
            query = query.filter(KplerProduct.platform.in_(to_list(platform)))

        if date_from:
            query = query.filter(KplerFlow.date >= to_datetime(date_from))

        if date_to:
            query = query.filter(KplerFlow.date <= to_datetime(date_to))

        if pricing_scenario:
            query = query.filter(Price.scenario.in_(to_list(pricing_scenario)))

        if currency is not None:
            query = query.filter(Currency.currency.in_(to_list(currency)))

        return query
