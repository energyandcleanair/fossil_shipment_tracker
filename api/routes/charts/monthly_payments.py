import pandas as pd
import json
import numpy as np

from .. import routes_api
from flask_restx import inputs
from base.encoder import JsonEncoder
from base.utils import to_list, df_to_json, to_datetime


from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse
from ..counter import RussiaCounterResource


@routes_api.route('/v0/chart/monthly_payments', strict_slashes=False)
class ChartMonthlyPayments(Resource):


    parser = reqparse.RequestParser()

    parser.add_argument('date_from', type=str, help='start date for counter data (format 2020-01-15)',
                        default="2021-01-01", required=False)
    parser.add_argument('date_to', type=str, help='start date for counter data (format 2020-01-15)',
                        default=-5,
                        required=False)

    parser.add_argument('aggregate_by', type=str, action='split',
                        default=['destination_region', 'commodity_group', 'date'],
                        help='which variables to aggregate by. Could be any of commodity, type, destination_region, date')

    parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('download', help='Whether to return results as a file or not.',
                        type=inputs.boolean, default=False)
    parser.add_argument('format', type=str, help='format of returned results (json, csv, or geojson)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = RussiaCounterResource.parser.parse_args()
        format = params.get('format')
        nest_in_data = params.get('nest_in_data')

        params.update(**{
            'pivot_by': ['commodity_group_name'],
            'pivot_value': 'value_eur',
            'use_eu': True,
            # 'date_from': '2022-01-01',
            'sort_by': ['value_eur'],
            'currency': 'EUR',
            'keep_zeros': True,
            'format': 'json',
            'nest_in_data': True
        })

        response = RussiaCounterResource().get_from_params(params)
        data = pd.DataFrame(response.json['data'])
        data['month'] = pd.to_datetime(data.date).dt.to_period('M').dt.to_timestamp()

        data = data.groupby(['destination_region', 'month', 'variable']) \
            .agg(Oil=('Oil', np.average),
                 Gas=('Gas', np.average),
                 Coal = ('Coal', np.average),
                 ndays=('Oil', len)) \
            .reset_index()
        data = data[data.ndays >= 10].drop(['ndays'], axis=1)

        # Sort by region
        data['Total'] = data.Coal + data.Oil + data.Gas
        regions = data.groupby(['destination_region'])['Total'].sum().sort_values(ascending=False).reset_index()[['destination_region']]
        data = regions.merge(data).drop('Total', axis=1)

        return self.build_response(result=data,
                                   format=format,
                                   nest_in_data=nest_in_data)



    def build_response(self, result, format, nest_in_data):

        result.replace({np.nan: None}, inplace=True)

        # If bulk and departure berth is coal, replace commodity with coal
        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=chart_monthly_payments.csv"})

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps({"data": result.to_dict(orient="records")}, cls=JsonEncoder)
            else:
                resp_content = json.dumps(result.to_dict(orient="records"), cls=JsonEncoder)

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json')

        return Response(response="Unknown format. Should be either csv or json",
                        status=HTTPStatus.BAD_REQUEST,
                        mimetype='application/json')