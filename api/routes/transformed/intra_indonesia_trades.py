import pandas as pd
import json
import numpy as np
import datetime as dt
import re
from flask_restx import inputs
from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse


from sqlalchemy.orm import aliased

import base
from base import (
    CHARTS_USE_KPLER_DEFAULT,
    COMMODITY_GROUPING_DEFAULT,
    COMMODITY_GROUPING_CHOICES,
    COMMODITY_GROUPING_HELP,
)
from base.encoder import JsonEncoder
from base.utils import to_list

from routes.kpler_trade import KplerTradeResource
from routes.security import key_required
from .. import postcompute
from .. import routes_api, ns_transformed
from ..overland import PipelineFlowResource

from base.models.kpler import KplerZoneExtensionIndonesiaIsland


def find_argument_by_name(parser, name):
    return next(arg for arg in parser.args if arg.name == name)


@ns_transformed.route("/v0/transformed/intra_indonesia_trade", strict_slashes=False)
class IntraIndonesiaTrade(KplerTradeResource):
    parser: reqparse.RequestParser = KplerTradeResource.parser.copy()

    parser.replace_argument(
        "date_from",
        type=str,
        help=find_argument_by_name(KplerTradeResource.parser, "date_from").help,
        default="2017-01-01",
        required=False,
    )

    parser.replace_argument(
        "origin_iso2",
        type=str,
        action="split",
        help=find_argument_by_name(KplerTradeResource.parser, "origin_iso2").help,
        default=["ID"],
    )

    parser.replace_argument(
        "destination_iso2",
        type=str,
        action="split",
        help=find_argument_by_name(KplerTradeResource.parser, "destination_iso2").help,
        default=["ID"],
    )

    parser.replace_argument(
        "exclude_within_country",
        type=inputs.boolean,
        help=find_argument_by_name(KplerTradeResource.parser, "exclude_within_country").help,
        default=False,
    )

    @routes_api.expect(parser)
    @key_required
    def get(self):

        params = self.parser.parse_args(strict=True)

        return self.get_from_params(params)

    def get_aggregate_cols_dict(self, subquery, params):
        cols_dict = super().get_aggregate_cols_dict(subquery, params)

        return cols_dict | {
            "origin_island": [subquery.c.origin_island],
            "origin_indonesia_region": [
                subquery.c.origin_indonesia_region,
                subquery.c.origin_island,
            ],
            "destination_island": [subquery.c.destination_island],
            "destination_indonesia_region": [
                subquery.c.destination_indonesia_region,
                subquery.c.destination_island,
            ],
            "origin_zone": [
                subquery.c.origin_zone_name,
                subquery.c.origin_indonesia_region,
                subquery.c.origin_island,
            ],
            "destination_zone": [
                subquery.c.destination_zone_name,
                subquery.c.destination_indonesia_region,
                subquery.c.destination_island,
            ],
        }

    def initial_query(self, params):
        origin_indonesia_area = aliased(KplerZoneExtensionIndonesiaIsland)
        destination_indonesia_area = aliased(KplerZoneExtensionIndonesiaIsland)
        query = super(IntraIndonesiaTrade, self).initial_query(
            params,
            additional_columns=[
                origin_indonesia_area.island_name.label("origin_island"),
                origin_indonesia_area.region_name.label("origin_indonesia_region"),
                destination_indonesia_area.island_name.label("destination_island"),
                destination_indonesia_area.region_name.label("destination_indonesia_region"),
            ],
            query_modifier=lambda query, aliases: query.outerjoin(
                origin_indonesia_area, origin_indonesia_area.zone_id == aliases["origin_zone"].id
            ).outerjoin(
                destination_indonesia_area,
                destination_indonesia_area.zone_id == aliases["destination_zone"].id,
            ),
        )
        return query
