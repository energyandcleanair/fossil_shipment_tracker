import json

from flask import Response
from flask_restx import Resource, reqparse
from base.models import Position, FlowDepartureBerth, FlowArrivalBerth
from base.encoder import JsonEncoder
from base.db import session
from . import routes_api

import pandas as pd
import shapely


@routes_api.route('/v0/position', strict_slashes=False)
class PositionResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('voyage_id', required=True, help='id(s) of voyage', action='split')
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = PositionResource.parser.parse_args()
        voyage_id = params.get("voyage_id")
        format = params.get("format")

        query = session.query(Position,
                              FlowDepartureBerth.berth_id.label("departure_berth_id"),
                              FlowArrivalBerth.berth_id.label("arrival_berth_id")) \
            .join(FlowDepartureBerth, Position.id == FlowDepartureBerth.position_id, isouter=True) \
            .join(FlowArrivalBerth, Position.id == FlowArrivalBerth.position_id, isouter=True)

        if voyage_id is not None:
            query = query.filter(Position.flow_id.in_(voyage_id))

        query = query.order_by(Position.date_utc)
        positions_df = pd.read_sql(query.statement, session.bind)
        positions_df["lon"] = positions_df.geometry.apply(
            lambda geom: shapely.wkb.loads(bytes(geom.data)).x)
        positions_df["lat"] = positions_df.geometry.apply(
            lambda geom: shapely.wkb.loads(bytes(geom.data)).y)

        positions_df.drop(["geometry"], axis=1, inplace=True)

        if format == "csv":
            return Response(
                response=positions_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=positions.csv"})

        if format == "json":
            return Response(
                response=json.dumps({"data": positions_df.to_dict(orient="records")}, cls=JsonEncoder),
                status=200,
                mimetype='application/json')