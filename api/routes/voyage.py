import datetime as dt
import pandas as pd
import geopandas as gpd
import json

from . import routes_api
from flask_restx import inputs


from base.models import Flow, Ship, Arrival, Departure, Port, Berth,\
    FlowDepartureBerth, FlowArrivalBerth, Position, Trajectory, Destination
from base.db import session
from base.encoder import JsonEncoder

from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse
import sqlalchemy as sa
from sqlalchemy.orm import aliased
from sqlalchemy import or_
from sqlalchemy import func
from base.utils import update_geometry_from_wkb

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

        DepartureBerth= aliased(Berth)
        ArrivalBerth = aliased(Berth)

        DestinationPort = aliased(Port)

        # Query with joined information
        flows_rich = (session.query(Flow.id,
                                    Flow.status,
                                    Departure.date_utc,
                                    DeparturePort.unlocode,
                                    DeparturePort.iso2,
                                    DeparturePort.name,
                                    Arrival.date_utc,
                                    ArrivalPort.unlocode,
                                    ArrivalPort.iso2,
                                    ArrivalPort.name,
                                    Destination.name,
                                    func.coalesce(ArrivalPort.iso2, Destination.iso2),
                                    Ship.imo,
                                    Ship.mmsi,
                                    Ship.type,
                                    Ship.subtype,
                                    Ship.dwt,
                                    Ship.commodity,
                                    Ship.quantity,
                                    Ship.unit,
                                    DepartureBerth.id,
                                    DepartureBerth.name,
                                    DepartureBerth.commodity,
                                    DepartureBerth.port_unlocode,
                                    ArrivalBerth.id,
                                    ArrivalBerth.name,
                                    ArrivalBerth.commodity,
                                    ArrivalBerth.port_unlocode)
             .join(Departure, Flow.departure_id == Departure.id)
             .join(DeparturePort, Departure.port_id == DeparturePort.id)
             .outerjoin(Arrival, Departure.id == Arrival.departure_id)
             .outerjoin(ArrivalPort, Arrival.port_id == ArrivalPort.id)
             .join(Ship, Departure.ship_imo == Ship.imo)) \
             .outerjoin(FlowDepartureBerth, Flow.id == FlowDepartureBerth.flow_id) \
             .outerjoin(FlowArrivalBerth, Flow.id == FlowArrivalBerth.flow_id) \
             .outerjoin(DepartureBerth, DepartureBerth.id == FlowDepartureBerth.berth_id) \
             .outerjoin(ArrivalBerth, ArrivalBerth.id == FlowArrivalBerth.berth_id) \
             .outerjoin(Destination, Flow.last_destination_name == Destination.name) \
             .outerjoin(DestinationPort, Destination.port_id == DestinationPort.id)

        if id is not None:
            flows_rich = flows_rich.filter(Flow.id.in_(id))

        if commodity is not None:
            flows_rich = flows_rich.filter(Ship.commodity.in_(commodity))

        if date_from is not None:
            flows_rich = flows_rich.filter(
                sa.or_(
                    Arrival.date_utc >= dt.datetime.strptime(date_from, "%Y-%m-%d"),
                    Departure.date_utc >= dt.datetime.strptime(date_from, "%Y-%m-%d")
                ))

        if date_to is not None:
            flows_rich = flows_rich.filter(
                sa.or_(
                    Arrival.date_utc <= dt.datetime.strptime(date_to, "%Y-%m-%d"),
                    Departure.date_utc <= dt.datetime.strptime(date_to, "%Y-%m-%d")
                ))

        columns = ["id",
                   "status",
                   "departure_date_utc",
                   "departure_unlocode",
                   "departure_iso2",
                   "departure_port_name",
                   "arrival_date_utc",
                   "arrival_unlocode",
                   "arrival_iso2",
                   "arrival_port_name",
                   "destination_name",
                   "destination_iso2",
                   "ship_imo",
                   "ship_mmsi",
                   "ship_type",
                   "ship_subtype",
                   "ship_dwt",
                   "commodity",
                   "quantity",
                   "unit",
                   "departure_berth_id",
                   "departure_berth_name",
                   "departure_berth_commodity",
                   "departure_berth_unlocode",
                   "arrival_berth_id",
                   "arrival_berth_name",
                   "arrival_berth_commodity",
                   "arrival_berth_unlocode"]

        def row_to_dict(row):
            return dict(zip(columns, row))

        flows_rich = flows_rich.all()
        if len(flows_rich) == 0:
            return Response(
                status=HTTPStatus.NO_CONTENT,
                response="empty",
                mimetype='application/json')

        flows_rich = [row_to_dict(x) for x in flows_rich]


        # If bulk and departure berth is coal, replace commodity with coal
        flows_df = pd.DataFrame(flows_rich)
        flows_df.loc[(flows_df.commodity=="bulk") & \
                     (flows_df.departure_berth_commodity.str.contains("Coal", case=False, na=False)), "commodity"] = "coal"
        flows_df.loc[(flows_df.commodity == "bulk") & \
                     (~flows_df.departure_berth_commodity.str.contains("Coal", case=False, na=False)), "commodity"] = "bulk_notcoal"

        if format == "csv":
            return Response(
                response=flows_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=flows.csv"})

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps({"data": flows_df.to_dict(orient="records")}, cls=JsonEncoder)
            else:
                resp_content = json.dumps(flows_df.to_dict(orient="records"), cls=JsonEncoder)

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json')

        if format == "geojson":
            flow_ids = list([int(x) for x in flows_df.id.unique()])

            trajectories = session.query(Trajectory) \
                .filter(Trajectory.flow_id.in_(flow_ids))

            trajectories_df = pd.read_sql(trajectories.statement, session.bind)
            trajectories_df = update_geometry_from_wkb(trajectories_df)
            flows_gdf = gpd.GeoDataFrame(flows_df.merge(trajectories_df[["flow_id", "geometry"]].rename(columns={'flow_id': 'id'})), geometry='geometry')
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

