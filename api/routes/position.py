import json
import datetime as dt
import sqlalchemy as sa
import geopandas as gpd
from flask import Response, request
from flask_restx import Resource, reqparse, inputs
import pandas as pd
import shapely
from sqlalchemy.orm import aliased


import base
from base.models import Shipment, Position, ShipmentDepartureBerth, ShipmentArrivalBerth, Departure, Arrival, Ship, Berth
from base.encoder import JsonEncoder
from base.db import session
from base.utils import to_datetime, update_geometry_from_wkb
from . import routes_api




@routes_api.route('/v0/position', strict_slashes=False)
class PositionResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('ship_imo', required=False, help='imo(s) of ship', action='split')

    parser.add_argument('date_from', help='start date (format 2020-01-15)',
                        default="2022-01-01", required=False)

    parser.add_argument('date_to', type=str, help='end date (format 2020-01-15)', required=False,
                        default=dt.datetime.today().strftime("%Y-%m-%d"))

    parser.add_argument('commodity', help='commodity(ies) of interest (e.g. bulk, crude_oil). Default: returns all of them',
                        default=None, action='split', required=False)

    # Position related filters
    parser.add_argument('speed_max',
                        help='maximum speed (in Knot)',
                        type=float,
                        required=False,
                        default=None)

    parser.add_argument('navigation_status',
                        help='navigation status(es) e.g. moored',
                        action='split',
                        required=False,
                        default=None)

    # Voyage related filters
    parser.add_argument('status',
                        help='filter by the status of the associated voyage',
                        action='split',
                        required=False,
                        default=None)

    parser.add_argument('has_departure_berth',
                        type=inputs.boolean,
                        help='filter voyages that have (or have not) a departure berth',
                        required=False,
                        default=None)

    parser.add_argument('has_arrival_berth',
                        type=inputs.boolean,
                        help='filter voyages that have (or have not) an arrival berth',
                        required=False,
                        default=None)

    parser.add_argument('buffer_hour',
                        help='how many hours before departure or after arrival should we keep positions',
                        type=float,
                        required=False,
                        default=0)

    # Others
    parser.add_argument('format', type=str, help='format of returned results (json, csv, geojson, or kml)',
                        required=False, default="json")

    parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
                        type=inputs.boolean, default=True)

    parser.add_argument('download', help='Whether to return results as a file or not.',
                        type=inputs.boolean, default=False)

    parser.add_argument('geometry_only', help='Whether to only keep geometry to reduce file size.',
                        type=inputs.boolean, default=False)

    @routes_api.expect(parser)
    def get(self):

        params = PositionResource.parser.parse_args()
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        ship_imo = params.get("ship_imo")
        status = params.get("status")
        commodities = params.get("commodity")
        has_departure_berth = params.get("has_departure_berth")
        has_arrival_berth = params.get("has_arrival_berth")
        speed_max = params.get("speed_max")
        navigation_status = params.get("navigation_status")
        format = params.get("format")
        nest_in_data = params.get("nest_in_data")
        download = params.get("download")
        buffer_hour = params.get("buffer_hour")
        geometry_only = params.get("geometry_only")

        DepartureBerth = aliased(Berth)
        ArrivalBerth = aliased(Berth)

        query = session.query(Shipment.id.label("voyage_id"),
                              Shipment.status,
                              Position.date_utc.label("date_utc"),
                              Departure.date_utc.label("departure_date_utc"),
                              Arrival.date_utc.label("arrival_date_utc"),
                              Ship.imo,
                              Ship.commodity,
                              Ship.type.label("ship_type"),
                              Ship.subtype.label("ship_subtype"),
                              Position.speed,
                              Position.navigation_status,
                              Position.geometry,
                              Position.date_utc,
                              DepartureBerth.name.label("departure_berth_name"),
                              DepartureBerth.commodity.label("departure_berth_commodity"),
                              ArrivalBerth.name.label("arrival_berth_name"),
                              ArrivalBerth.commodity.label("arrival_berth_commodity")
                              ) \
            .join(Departure, Shipment.departure_id == Departure.id) \
            .join(Ship, Departure.ship_imo == Ship.imo) \
            .join(Arrival, Shipment.arrival_id == Arrival.id) \
            .join(Position, Position.ship_imo == Departure.ship_imo) \
            .filter(
                sa.and_(
                    Position.date_utc >= Departure.date_utc - dt.timedelta(hours=buffer_hour),
                    sa.or_(Arrival.date_utc == sa.null(),
                           Position.date_utc < Arrival.date_utc + dt.timedelta(hours=buffer_hour))
                )) \
            .outerjoin(ShipmentDepartureBerth, ShipmentDepartureBerth.shipment_id == Shipment.id) \
            .outerjoin(ShipmentArrivalBerth, ShipmentArrivalBerth.shipment_id == Shipment.id) \
            .outerjoin(DepartureBerth, ShipmentDepartureBerth.berth_id == DepartureBerth.id) \
            .outerjoin(ArrivalBerth, ShipmentArrivalBerth.berth_id == ArrivalBerth.id)

        if ship_imo is not None:
            query = query.filter(Position.ship_imo.in_(ship_imo))

        if date_from is not None:
            query = query.filter(Position.date_utc >= to_datetime(date_from))

        if date_to is not None:
            query = query.filter(Position.date_utc <= to_datetime(date_to))

        if status is not None:
            query = query.filter(Shipment.status.in_(status))

        if commodities is not None:
            query = query.filter(Ship.commodity.in_(commodities))

        if has_departure_berth is not None:
            query = query.filter(sa.not_((ShipmentDepartureBerth.id == sa.null()) == has_departure_berth))

        if has_arrival_berth is not None:
            query = query.filter(sa.not_((ShipmentArrivalBerth.id == sa.null()) == has_arrival_berth))

        if speed_max is not None:
            query = query.filter(sa.or_(Position.speed == sa.null(),
                                        Position.speed <= speed_max))
        if navigation_status is not None:
            query = query.filter(Position.navigation_status.in_(navigation_status))

        query = query.order_by(Position.date_utc)
        positions_df = pd.read_sql(query.statement, session.bind)


        if format == "csv":
            positions_df["lon"] = positions_df.geometry.apply(
                lambda geom: shapely.wkb.loads(bytes(geom.data)).x)
            positions_df["lat"] = positions_df.geometry.apply(
                lambda geom: shapely.wkb.loads(bytes(geom.data)).y)
            positions_df.drop(["geometry"], axis=1, inplace=True)
            return Response(
                response=positions_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=positions.csv"})

        if format == "json":
            positions_df["lon"] = positions_df.geometry.apply(
                lambda geom: shapely.wkb.loads(bytes(geom.data)).x)
            positions_df["lat"] = positions_df.geometry.apply(
                lambda geom: shapely.wkb.loads(bytes(geom.data)).y)
            positions_df.drop(["geometry"], axis=1, inplace=True)
            if nest_in_data:
                resp_content = json.dumps({"data": positions_df.to_dict(orient="records")}, cls=JsonEncoder)
            else:
                resp_content = json.dumps(positions_df.to_dict(orient="records"), cls=JsonEncoder)

            if download:
                headers = {"Content-disposition": "attachment; filename=positions.json"}
            else:
                headers = {}

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json',
                headers=headers)

        positions_df = update_geometry_from_wkb(positions_df)
        result_gdf = gpd.GeoDataFrame(positions_df.rename(columns={'shipment_id': 'id'}), geometry='geometry')
        if format == "geojson":
            result_geojson = result_gdf.to_json(cls=JsonEncoder)
            if nest_in_data:
                resp_content = '{"data": ' + result_geojson + '}'
            else:
                resp_content = result_geojson

            if download:
                headers = {"Content-disposition": "attachment; filename=voyages.geojson"}
            else:
                headers = {}

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json',
                headers=headers)

        if format == "kml":
            import fiona
            import io
            fiona.supported_drivers['KML'] = 'rw'
            file_kml = io.BytesIO()

            # Keep minimal infos to save space
            if geometry_only:
                result_gdf = result_gdf[["geometry"]]

            result_gdf.to_file(file_kml, driver='KML')
            headers = {"Content-disposition": "attachment; filename=positions.kml"}
            file_kml.seek(0)
            return Response(
                response=file_kml,
                status=200,
                mimetype='application/kml',
                headers=headers)



