import simplejson as json

from . import routes_api
# from base.encoder import JsonEncoder
from timeit import default_timer as timer
from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse
from base.models import Ship
from base.encoder import JsonEncoder
from base.db import session

import pandas as pd


@routes_api.route('/v0/ship', strict_slashes=False)
class ShipResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('imo', required=False, action='split')
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = ShipResource.parser.parse_args()
        imo = params.get("imo")

        query = Ship.query
        if imo is not None:
            query = query.filter(Ship.imo == imo)

        ships_df = pd.read_sql(query.statement, session.bind)

        if format == "csv":
            return Response(
                response=ships_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=flows.csv"})

        if format == "json":
            import json

            return Response(
                response=json.dumps({"data": ships_df.to_dict(orient="records")}, cls=JsonEncoder),
                status=200,
                mimetype='application/json')