import json

from flask import Response
from flask_restx import Resource, reqparse
from base.models import Departure
from base.encoder import JsonEncoder
from base.db import session
from . import routes_api

import pandas as pd


@routes_api.route('/v0/departure', strict_slashes=False)
class DepartureResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('unlocode', required=False, help='unlocode(s) of departure port', action='split')
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = DepartureResource.parser.parse_args()
        unlocode = params.get("unlocode")
        format = params.get("format")

        query = Departure.query
        if unlocode is not None:
            query = query.filter(Departure.port_unlocode.in_(unlocode))

        departures_df = pd.read_sql(query.statement, session.bind)

        if format == "csv":
            return Response(
                response=departures_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=departures.csv"})

        if format == "json":
            return Response(
                response=json.dumps({"data": departures_df.to_dict(orient="records")}, cls=JsonEncoder),
                status=200,
                mimetype='application/json')