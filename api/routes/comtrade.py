from flask import Response
from flask_restx import Resource, reqparse, inputs
from base.db import session

from routes.security import key_required
from routes.template import TemplateResource

from . import routes_api

from base.models import ComtradeHsTradeRecord


@routes_api.route("/v0/comtrade")
class ComtradeResource(TemplateResource):
    parser = TemplateResource.parser.copy()

    filename = "comtrade"
    date_cols = ["period"]
    value_cols = ["value_usd", "quantity"]
    pivot_dependencies = {}

    @routes_api.expect(parser)
    @key_required
    def get(self):
        params = ComtradeResource.parser.parse_args()
        return self.get_from_params(params)

    def initial_query(self, params=None):
        return session.query(ComtradeHsTradeRecord)
