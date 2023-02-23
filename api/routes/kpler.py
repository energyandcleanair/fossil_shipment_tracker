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
from api.routes.template import TemplateResource
from base import PRICING_DEFAULT
from base.logger import logger
from base.db import session
from base.models import KplerFlow, KplerProduct
from base.utils import to_datetime, to_list, intersect, df_to_json
from api import postcompute
from engine.commodity import get_subquery as get_commodity_subquery


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
        default=None,
        required=False,
    )

    parser.add_argument(
        "date_to", type=str, help="End date", default=None, required=False
    )

    parser.add_argument(
        "product", help="Product", required=False, action="split", default=None
    )

    must_group_by = ["unit"]
    date_cols = ["date"]
    value_cols = ["value"]
    pivot_dependencies = {}
    filename = "kpler_flow.csv"

    def get_aggregate_cols_dict(self, subquery):
        return {}

    def get_agg_value_cols(self, subquery):
        return []

    @routes_api.expect(parser)
    def get(self):
        params = KplerFlowResource.parser.parse_args()
        return self.get_from_params(params)

    def initial_query(self, params=None):
        query = session.query(KplerFlow, KplerProduct.platform).outerjoin(
            KplerProduct,
            sa.and_(
                KplerProduct.name == KplerFlow.product,
                KplerProduct.platform == KplerFlow.platform,
            ),
        )
        return query

    def filter(self, query, params=None):
        origin_iso2 = params.get("origin_iso2")
        destination_iso2 = params.get("destination_iso2")
        product = params.get("product")
        date_from = params.get("date_from")
        date_to = params.get("date_to")

        if origin_iso2:
            query = query.filter(KplerFlow.origin_iso2.in_(to_list(origin_iso2)))

        if destination_iso2:
            query = query.filter(
                KplerFlow.destination_iso2.in_(to_list(destination_iso2))
            )

        if product:
            query = query.filter(KplerProduct.name.in_(to_list(product)))

        if date_from:
            query = query.filter(KplerFlow.date >= to_datetime(date_from))

        if date_to:
            query = query.filter(KplerFlow.date <= to_datetime(date_to))

        return query
