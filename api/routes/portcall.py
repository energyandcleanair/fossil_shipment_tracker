import json

from flask import Response
from flask_restx import Resource, reqparse, inputs
from base.models import Port, PortCall
from base.encoder import JsonEncoder
from base.db import session
from . import routes_api

import pandas as pd


@routes_api.route(
    "/v0/portcall",
    methods=["GET"],
    strict_slashes=False,
    doc={
        "description": "Deprecated, use /v0/kpler_trade. Retrieve shipments of fossil fuels.",
        "deprecated": True,
    },
)
class PortCallResource(Resource):

    parser = reqparse.RequestParser()

    parser.add_argument(
        "unlocode", required=False, help="unlocode(s) of ports (optional)", action="split"
    )
    parser.add_argument(
        "check_departure_only",
        required=False,
        help="whether it is on the list of checked departures or not",
        type=inputs.boolean,
        default=False,
    )
    parser.add_argument(
        "nest_in_data",
        help="Whether to nest the geojson content in a data key.",
        type=inputs.boolean,
        default=True,
    )
    parser.add_argument(
        "format",
        type=str,
        help="format of returned results (json or csv)",
        required=False,
        default="json",
    )

    @routes_api.expect(parser)
    def get(self):
        params = PortCallResource.parser.parse_args()
        unlocode = params.get("unlocode")
        check_departure_only = params.get("check_departure_only")
        nest_in_data = params.get("nest_in_data")
        format = params.get("format")

        query = PortCall.query.join(Port, Port.id == PortCall.port_id)

        if unlocode is not None:
            query = query.filter(Port.unlocode.in_(unlocode))

        if check_departure_only is not None:
            query = query.filter(Port.check_departure)

        portcalls_df = pd.read_sql(query.statement, session.bind)

        if format == "csv":
            return Response(
                response=portcalls_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=portcalls.csv"},
            )

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps(
                    {"data": portcalls_df.to_dict(orient="records")}, cls=JsonEncoder
                )
            else:
                resp_content = json.dumps(portcalls_df.to_dict(orient="records"), cls=JsonEncoder)

            return Response(response=resp_content, status=200, mimetype="application/json")
