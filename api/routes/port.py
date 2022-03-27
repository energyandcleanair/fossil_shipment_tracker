import json

from flask import Response
from flask_restx import Resource, reqparse
from base.models import Port
from base.encoder import JsonEncoder
from base.db import session
from . import routes_api

import pandas as pd


@routes_api.route('/v0/port', strict_slashes=False)
class PortResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('unlocode', required=False, help='unlocode(s) of ports (optional)', action='split')
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = PortResource.parser.parse_args()
        unlocode = params.get("unlocode")
        format = params.get("format")

        query = Port.query
        if unlocode is not None:
            query = query.filter(Port.unlocode.in_(unlocode))

        ports_df = pd.read_sql(query.statement, session.bind)
        ports_df.drop(["geometry"], axis=1, inplace=True)

        if format == "csv":
            return Response(
                response=ports_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=ports.csv"})

        if format == "json":
            return Response(
                response=json.dumps({"data": ports_df.to_dict(orient="records")}, cls=JsonEncoder),
                status=200,
                mimetype='application/json')