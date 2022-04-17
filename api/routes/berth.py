import json
from http import HTTPStatus
from shapely import wkb

from flask import Response
from flask_restx import Resource, reqparse
from flask_restx import inputs

from base.models import Berth
from base.encoder import JsonEncoder
from base.db import session
from base.utils import update_geometry_from_wkb
from . import routes_api

import pandas as pd


@routes_api.route('/v0/berth', strict_slashes=False)
class BerthResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('id', required=False, help='id(s) of ships (optional)', action='split')
    parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('download', help='Whether to return results as a file or not.',
                        type=inputs.boolean, default=False)
    parser.add_argument('format', type=str, help='format of returned results (json, csv, or geojson)',
                        required=False, default="json")


    @routes_api.expect(parser)
    def get(self):

        params = BerthResource.parser.parse_args()
        id = params.get("id")
        nest_in_data = params.get("nest_in_data")
        format = params.get("format")
        download = params.get("download")

        query = Berth.query
        if id is not None:
            query = query.filter(Berth.imo.in_(id))

        berths_df = pd.read_sql(query.statement, session.bind)
        berths_df = update_geometry_from_wkb(berths_df)

        if format == "csv":
            return Response(
                response=berths_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=shipments.csv"})

        if format == "json":
            berths_df.drop(["geometry"], axis=1, inplace=True)
            if nest_in_data:
                resp_content = json.dumps({"data": berths_df.to_dict(orient="records")}, cls=JsonEncoder)
            else:
                resp_content = json.dumps(berths_df.to_dict(orient="records"), cls=JsonEncoder)

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json')

        if format == "geojson":
            import geopandas as gpd
            berths_gdf = gpd.GeoDataFrame(berths_df, geometry='geometry')
            berths_geojson = berths_gdf.to_json(cls=JsonEncoder)

            if nest_in_data:
                resp_content = '{"data": ' + berths_geojson + '}'
            else:
                resp_content = berths_geojson

            if download:
                headers = {"Content-disposition":
                               "attachment; filename=voyages.geojson"}
            else:
                headers = {}

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json',
                headers=headers)

        return Response(response="Unknown format. Should be either csv, json or geojson",
                        status=HTTPStatus.BAD_REQUEST,
                        mimetype='application/json')