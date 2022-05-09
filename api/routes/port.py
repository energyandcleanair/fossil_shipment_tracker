import json

from flask import Response
from flask_restx import Resource, reqparse, inputs
from base.models import Port
from base.encoder import JsonEncoder
from base.db import session
from . import routes_api

import pandas as pd
import numpy as np
import shapely

@routes_api.route('/v0/port', methods=['GET'], strict_slashes=False)
class PortResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('unlocode', required=False, help='unlocode(s) of ports (optional)', action='split')
    parser.add_argument('iso2', required=False, help='iso2 of country', action='split')
    parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = PortResource.parser.parse_args()
        unlocode = params.get("unlocode")
        iso2 = params.get("iso2")
        format = params.get("format")
        nest_in_data = params.get("nest_in_data")

        query = Port.query
        if unlocode is not None:
            query = query.filter(Port.unlocode.in_(unlocode))

        if iso2 is not None:
            query = query.filter(Port.iso2.in_(iso2))

        ports_df = pd.read_sql(query.statement, session.bind)

        from base.utils import update_geometry_from_wkb
        ports_df = update_geometry_from_wkb(ports_df, to='shape')
        ports_df["lon"] = ports_df.geometry.apply(lambda geom: geom.x if geom is not None and not geom.is_empty else None)
        ports_df["lat"] = ports_df.geometry.apply(lambda geom: geom.y if geom is not None and not geom.is_empty else None)
        ports_df.replace({np.nan: None}, inplace=True)
        ports_df.drop(["geometry"], axis=1, inplace=True)

        if format == "csv":
            return Response(
                response=ports_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=ports.csv"})

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps({"data": ports_df.to_dict(orient="records")}, cls=JsonEncoder)
            else:
                resp_content = json.dumps(ports_df.to_dict(orient="records"), cls=JsonEncoder)

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json')