import json
import pandas as pd
import datetime as dt
import numpy as np


from flask import Response
from flask_restx import Resource, reqparse, inputs
from base.models import Flaring, FlaringFacility
from base.encoder import JsonEncoder
from base.db import session
from base.utils import to_datetime, to_list

from . import routes_api


@routes_api.route('/v0/flaring', strict_slashes=False)
class DepartureResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('facility_id', help='facility id(s)',
                        type=str, action='split',
                        default=None, required=False)
    parser.add_argument('rolling_days', type=int,
                        help='rolling average window (in days). Default: no rolling averaging',
                        required=False, default=None)
    parser.add_argument('date_from', help='start date for arrival (format 2020-01-15)',
                        default="2022-01-01", required=False)
    parser.add_argument('date_to', type=str, help='end date for arrival (format 2020-01-15)', required=False,
                        default=dt.datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")
    parser.add_argument('download', help='Whether to return results as a file or not.',
                        type=inputs.boolean, default=False)

    @routes_api.expect(parser)
    def get(self):

        params = DepartureResource.parser.parse_args()
        facility_id = params.get("facility_id")
        format = params.get("format")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        nest_in_data = params.get("nest_in_data")
        rolling_days = params.get("rolling_days")
        download = params.get("download")

        query = session.query(FlaringFacility,
                              Flaring.date,
                              Flaring.value) \
                .join(Flaring, FlaringFacility.id == Flaring.facility_id)

        if facility_id is not None:
            query = query.filter(FlaringFacility.id.in_(to_list(facility_id)))

        if date_from is not None:
            query = query.filter(Flaring.date >= to_datetime(date_from))

        if date_to is not None:
            query = query.filter(Flaring.date <= to_datetime(date_to))

        result = pd.read_sql(query.statement, session.bind)

        result = self.roll_average(result=result, rolling_days=rolling_days)

        result = self.build_response(result=result,
                                     format=format,
                                     nest_in_data=nest_in_data,
                                     download=download)


    def build_response(self, result, format, nest_in_data, download):

        result.replace({np.nan: None}, inplace=True)
        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=flaring.csv"})

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps({"data": result.to_dict(orient="records")}, cls=JsonEncoder)
            else:
                resp_content = json.dumps(result.to_dict(orient="records"), cls=JsonEncoder)

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json')


        if format == "geojson":
            import geopandas as gpd
            berths_gdf = gpd.GeoDataFrame(result, geometry='geometry')
            berths_geojson = berths_gdf.to_json(cls=JsonEncoder)

            if nest_in_data:
                resp_content = '{"data": ' + berths_geojson + '}'
            else:
                resp_content = berths_geojson

            if download:
                headers = {"Content-disposition":
                               "attachment; filename=flaring.geojson"}
            else:
                headers = {}

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json',
                headers=headers)


    def roll_average(self, result, rolling_days):

        if rolling_days is not None:
            date_column = "date"
            value_columns = ['value']
            min_date = result[date_column].min()
            max_date = result[date_column].max()
            daterange = pd.date_range(min_date, max_date).rename(date_column)

            result[date_column] = result[date_column].dt.floor('D')  # Should have been done already
            result = result \
                .groupby([x for x in result.columns if x not in ([date_column] + value_columns)]) \
                .apply(lambda x: x.set_index(date_column) \
                       .resample("D").sum() \
                       .reindex(daterange) \
                       .fillna(0) \
                       .rolling(rolling_days, min_periods=rolling_days) \
                       .mean()) \
                .reset_index()

        return result