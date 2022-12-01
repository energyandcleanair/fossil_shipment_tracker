import json
import pandas as pd
import datetime as dt

from flask import Response
from flask_restx import Resource, reqparse, inputs
from base.models import Departure, Port, Ship
from base.encoder import JsonEncoder
from base.db import session
from base.utils import to_datetime, to_list
from . import routes_api




@routes_api.route('/v0/departure', strict_slashes=False)
class DepartureResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('unlocode', required=False, help='unlocode(s) of departure port', action='split')
    parser.add_argument('iso2', required=False, help='iso2(s) of departure port', action='split')
    parser.add_argument('date_from', help='start date for arrival (format 2020-01-15)',
                        default="2022-01-01", required=False)
    parser.add_argument('date_to', type=str, help='end date for arrival (format 2020-01-15)', required=False,
                        default=dt.datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = DepartureResource.parser.parse_args()
        unlocode = params.get("unlocode")
        iso2 = params.get("iso2")
        format = params.get("format")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        nest_in_data = params.get("nest_in_data")

        query = session.query(Departure,
                              Port.name,
                              Port.iso2,
                              Ship.name,
                              Ship.commodity,
                              Ship.dwt) \
            .join(Port, Departure.port_id == Port.id) \
            .join(Ship, Departure.ship_imo == Ship.imo)

        if unlocode is not None:
            query = query.filter(Departure.port_unlocode.in_(to_list(unlocode)))

        if iso2 is not None:
            query = query.filter(Port.iso2.in_(to_list(iso2)))

        if date_from is not None:
            query = query.filter(Departure.date_utc >= to_datetime(date_from))

        if date_to is not None:
            query = query.filter(Departure.date_utc <= to_datetime(date_to))

        departures_df = pd.read_sql(query.statement, session.bind)


        if format == "csv":
            return Response(
                response=departures_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=departures.csv"})

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps({"data": departures_df.to_dict(orient="records")}, cls=JsonEncoder)
            else:
                resp_content = json.dumps(departures_df.to_dict(orient="records"), cls=JsonEncoder)

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json')