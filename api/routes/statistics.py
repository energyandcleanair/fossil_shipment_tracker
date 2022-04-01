import json

from flask import Response
from flask_restx import Resource, reqparse
from base.models import Ship
from base.encoder import JsonEncoder
from base.db import session
from . import routes_api

import pandas as pd


@routes_api.route('/v0/statistics', strict_slashes=False)
class StatisticsResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('date_from', help='start date for arrival (format 2020-01-15)',
                        default="2022-01-01", required=False)
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = StatisticsResource.parser.parse_args()
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        format = params.get("format")

        query = Ship.query
        if imo is not None:
            query = query.filter(Ship.imo.in_(imo))

        ships_df = pd.read_sql(query.statement, session.bind)

        if format == "csv":
            return Response(
                response=ships_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=ships.csv"})

        if format == "json":
            return Response(
                response=json.dumps({"data": ships_df.to_dict(orient="records")}, cls=JsonEncoder),
                status=200,
                mimetype='application/json')