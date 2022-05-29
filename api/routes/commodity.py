import json

from flask import Response
from flask_restx import Resource, reqparse, inputs
from base.models import Commodity
from base.encoder import JsonEncoder
from base.db import session
from . import routes_api

import pandas as pd
import numpy as np
import shapely

@routes_api.route('/v0/commodity', methods=['GET'], strict_slashes=False)
class CommodityResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = CommodityResource.parser.parse_args()
        format = params.get("format")
        nest_in_data = params.get("nest_in_data")

        query = Commodity.query
        commodities_df = pd.read_sql(query.statement, session.bind)

        from base.utils import update_geometry_from_wkb
        commodities_df.replace({np.nan: None}, inplace=True)

        if format == "csv":
            return Response(
                response=commodities_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=commodities.csv"})

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps({"data": commodities_df.to_dict(orient="records")}, cls=JsonEncoder)
            else:
                resp_content = json.dumps(commodities_df.to_dict(orient="records"), cls=JsonEncoder)

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json')