import datetime as dt
import pandas as pd

from . import routes_api
from models import Flow, Ship, Arrival, Departure
from base.db import session
from api.encoder import JsonEncoder
from flask import Flask, jsonify

from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse


@routes_api.route('/flow', strict_slashes=False)
class FlowResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('date_from', help='start date (format 2020-01-15)', required=False)
    parser.add_argument('date_to', type=str, help='end date (format 2020-01-15)', required=False,
                        default=dt.datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="csv")

    @routes_api.expect(parser)
    def get(self):

        params = FlowResource.parser.parse_args()
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        format = params.get("format")

        # Query with joined information
        flows_rich = (session.query(Flow.id, Departure.date_utc,
                                    Arrival.date_utc,
                                    Ship.imo,
                                    Ship.type,
                                    Ship.subtype,
                                    Ship.commodity,
                                    Ship.quantity,
                                    Ship.unit)
             .join(Departure, Flow.departure_id == Departure.id)
             .join(Arrival, Departure.id == Arrival.departure_id)
             .join(Ship, Departure.ship_imo == Ship.imo)).all()

        columns = ["id", "departure_date_utc", "arrival_date_utc",
                   "ship_imo", "ship_type", "ship_subtype", "commodity",
                   "quantity", "unit"]

        def row_to_dict(row):
            return dict(zip(columns, row))

        flows_rich = [row_to_dict(x) for x in flows_rich]


        if format == "csv":
            flows_df = pd.DataFrame(flows_rich)
            return Response(
                response=flows_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=flows.csv"})

        if format == "json":
            import json

            return Response(
                response=json.dumps(flows_rich, cls=JsonEncoder),
                status=200,
                mimetype='application/json')

        return Response(response="Unknown format. Should be either csv or json",
                        status=HTTPStatus.BAD_REQUEST,
                        mimetype='application/json')

