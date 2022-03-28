import datetime as dt
import pandas as pd
import json

from . import routes_api
from flask_restx import inputs


from base.models import Flow, Ship, Arrival, Departure, Port, Position
from base.db import session
from base.encoder import JsonEncoder

from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse
from sqlalchemy.orm import aliased


@routes_api.route('/v0/voyage', strict_slashes=False)
class VoyageResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('id', help='id(s) of voyage. Default: returns all of them',
                        default=None, action='split', required=False)
    parser.add_argument('commodity', help='commodity(ies) of interest. Default: returns all of them',
                        default=None, action='split', required=False)
    parser.add_argument('date_from', help='start date for arrival (format 2020-01-15)',
                        default="2022-01-01", required=False)
    parser.add_argument('date_to', type=str, help='end date for arrival (format 2020-01-15)', required=False,
                        default=dt.datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument('format', type=str, help='format of returned results (json, geojson or csv)',
                        required=False, default="json")
    parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('download', help='Whether to return results as a file or not.',
                        type=inputs.boolean, default=False)

    @routes_api.expect(parser)
    def get(self):

        params = VoyageResource.parser.parse_args()
        id = params.get("id")
        commodity = params.get("commodity")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        format = params.get("format")
        nest_in_data = params.get("nest_in_data")
        download = params.get("download")

        DeparturePort = aliased(Port)
        ArrivalPort = aliased(Port)

        # Query with joined information
        flows_rich = (session.query(Flow.id,
                                    Departure.date_utc,
                                    Departure.port_unlocode,
                                    DeparturePort.iso2,
                                    Arrival.date_utc,
                                    Arrival.port_unlocode,
                                    ArrivalPort.iso2,
                                    Ship.imo,
                                    Ship.mmsi,
                                    Ship.type,
                                    Ship.subtype,
                                    Ship.commodity,
                                    Ship.quantity,
                                    Ship.unit)
             .join(Departure, Flow.departure_id == Departure.id)
             .join(DeparturePort, Departure.port_unlocode == DeparturePort.unlocode)
             .join(Arrival, Departure.id == Arrival.departure_id)
             .join(ArrivalPort, Arrival.port_unlocode == ArrivalPort.unlocode)
             .join(Ship, Departure.ship_imo == Ship.imo))\

        if id is not None:
            flows_rich = flows_rich.filter(Flow.id.in_(id))

        if commodity is not None:
            flows_rich = flows_rich.filter(Ship.commodity.in_(commodity))

        if date_from is not None:
            flows_rich = flows_rich.filter(Arrival.date_utc >= dt.datetime.strptime(date_from, "%Y-%m-%d"))

        if date_to is not None:
            flows_rich = flows_rich.filter(Arrival.date_utc <= dt.datetime.strptime(date_to, "%Y-%m-%d"))

        columns = ["id",
                   "departure_date_utc",
                   "departure_unlocode",
                   "departure_iso2",
                   "arrival_date_utc",
                   "arrival_unlocode",
                   "arrival_iso2",
                   "ship_imo",
                   "ship_mmsi",
                   "ship_type",
                   "ship_subtype",
                   "commodity",
                   "quantity",
                   "unit"]

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
            if nest_in_data:
                resp_content = json.dumps({"data": flows_rich}, cls=JsonEncoder)
            else:
                resp_content = json.dumps(flows_rich, cls=JsonEncoder)

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json')

        if format == "geojson":
            flows_df = pd.DataFrame(flows_rich)
            flow_ids = list([int(x) for x in flows_df.id.unique()])

            #TODO find a faster option
            positions = pd.read_sql(session.query(Position.flow_id, Position.date_utc, Position.geometry).filter(Position.flow_id.in_(flow_ids)).statement,
                                    session.bind)
            import geopandas as gpd
            from geoalchemy2.shape import to_shape
            from shapely.geometry import LineString

            def pts_to_line(pts):
                if pts.size > 1:
                    return LineString([to_shape(y) for y in pts.tolist()])
                else:
                    return None

            lines = positions.sort_values(['flow_id', 'date_utc']) \
                .groupby(['flow_id'])['geometry'].apply(pts_to_line) \
                .rename("geometry") \
                .reset_index()

            flows_gdf = gpd.GeoDataFrame(flows_df.merge(lines.rename(columns={'flow_id':'id'})), geometry='geometry')
            flows_geojson = flows_gdf.to_json(cls=JsonEncoder)

            if nest_in_data:
                resp_content = '{"data": ' + flows_geojson + '}'
            else:
                resp_content = flows_geojson

            if download:
                headers = {"Content-disposition":
                               "attachment; filename=voyages.geojson"}
            else:
                headers={}

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json',
                headers=headers)

        return Response(response="Unknown format. Should be either csv, json or geojson",
                        status=HTTPStatus.BAD_REQUEST,
                        mimetype='application/json')

