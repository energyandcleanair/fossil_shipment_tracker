from flask import Response
from flask_restx import Resource, reqparse, inputs
from base.db import session

from routes.security import key_required
from routes.template import TemplateResource

from . import routes_api

from base.models import ComtradeHsTradeRecord
from base.utils import to_datetime

MIN_QUANTITY = 1000


@routes_api.route("/v0/comtrade")
class ComtradeResource(TemplateResource):
    parser = TemplateResource.parser.copy()

    parser.add_argument(
        "commodity_code", type=str, help="The HS commodity code to filter results", default=None
    )
    parser.add_argument(
        "partner_iso2", type=str, help="The partner to filter results", default=None
    )
    parser.add_argument(
        "reporter_iso2", type=str, help="The reporter to filter results", default=None
    )
    parser.add_argument(
        "date_from",
        type=str,
        help="The start date to filter results (format 2020-01-01 or a number representing the relative days from today)",
        default="2020-01-01",
    )
    parser.add_argument(
        "date_to",
        type=str,
        help="The end date to filter results (format 2020-01-01 or a number representing the relative days from today)",
        default=-1,
    )
    parser.add_argument(
        "min_quantity_threshold",
        type=float,
        help="The minimum quantity to filter results",
        default=MIN_QUANTITY,
    )
    parser.add_argument(
        "flow_direction",
        type=str,
        help="The trade direction to filter results",
        choices=["Exports", "Imports"],
        default=None,
    )

    filename = "comtrade"
    date_cols = ["period"]
    value_cols = ["value", "quantity", "usd_per_tonne"]
    pivot_dependencies = {}

    def get_aggregate_cols_dict(self, subquery):
        return {
            "commodity_code": [subquery.c.commodity_code],
            "partner_iso2": [subquery.c.partner_iso2],
            "reporter_iso2": [subquery.c.reporter_iso2],
            "flow_direction": [subquery.c.flow_direction],
            "period": [subquery.c.period],
            "value_usd": [subquery.c.value],
            "quantity": [subquery.c.quantity],
            "quantity_unit": [subquery.c.quantity_unit],
        }

    @routes_api.expect(parser)
    @key_required
    def get(self):
        params = ComtradeResource.parser.parse_args()
        return self.get_from_params(params)

    def initial_query(self, params=None):
        return session.query(
            ComtradeHsTradeRecord.commodity_code,
            ComtradeHsTradeRecord.partner_iso2,
            ComtradeHsTradeRecord.reporter_iso2,
            ComtradeHsTradeRecord.flow_direction,
            ComtradeHsTradeRecord.period,
            ComtradeHsTradeRecord.value_usd.label("value"),
            ComtradeHsTradeRecord.quantity,
            ComtradeHsTradeRecord.quantity_unit,
            (ComtradeHsTradeRecord.value_usd / (ComtradeHsTradeRecord.quantity / 1000)).label(
                "usd_per_tonne"
            ),
        )

    def filter(self, query, params):
        commodity_code = params.get("commodity_code")
        partner = params.get("partner_iso2")
        reporter = params.get("reporter_iso2")
        flow_direction = params.get("flow_direction")

        min_quantity_threshold = params.get("min_quantity_threshold")

        date_from = params.get("date_from")
        date_to = params.get("date_to")

        if commodity_code:
            query = query.filter(ComtradeHsTradeRecord.commodity_code == commodity_code)
        if partner:
            query = query.filter(ComtradeHsTradeRecord.partner_iso2 == partner)
        if reporter:
            query = query.filter(ComtradeHsTradeRecord.reporter_iso2 == reporter)
        if flow_direction:
            query = query.filter(ComtradeHsTradeRecord.flow_direction == flow_direction)

        if min_quantity_threshold:
            query = query.filter(ComtradeHsTradeRecord.quantity >= min_quantity_threshold)

        if date_from:
            query = query.filter(ComtradeHsTradeRecord.period >= str(to_datetime(date_from)))
        if date_to:
            query = query.filter(ComtradeHsTradeRecord.period <= str(to_datetime(date_to)))

        return query
