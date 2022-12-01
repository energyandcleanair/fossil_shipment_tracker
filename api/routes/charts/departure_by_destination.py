import pandas as pd
import json
import numpy as np
from flask_restx import inputs
from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse


import base
from base.encoder import JsonEncoder
from base.utils import to_list, df_to_json, to_datetime
from .. import routes_api
from ..voyage import VoyageResource


@routes_api.route('/v0/chart/departure_by_destination', strict_slashes=False)
class ChartDepartureDestination(Resource):

    parser = reqparse.RequestParser()

    parser.add_argument('departure_date_from', type=str, help='start date for counter data (format 2020-01-15)',
                        default="2021-12-01", required=False)
    parser.add_argument('date_to', type=str, help='start date for counter data (format 2020-01-15)',
                        default=-7,
                        required=False)

    parser.add_argument('commodity_grouping', type=str,
                        help="Grouping used (e.g. coal,oil,gas ('default') vs coal,oil,lng,pipeline_gas ('split_gas')",
                        default='split_gas_oil')

    parser.add_argument('commodity', help='Commodity(ies) of interest',
                        action='split',
                        required=False,
                        default=['crude_oil', 'oil_products', 'oil_or_chemical'])

    parser.add_argument('aggregate_by', type=str, action='split',
                        default=['destination_region', 'commodity_group', 'departure_date'],
                        help='which variables to aggregate by. Could be any of commodity, type, destination_region, date')

    parser.add_argument('rolling_days', type=int,
                        help='rolling average window (in days). Default: no rolling averaging',
                        required=False, default=30)

    parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('download', help='Whether to return results as a file or not.',
                        type=inputs.boolean, default=False)
    parser.add_argument('format', type=str, help='format of returned results (json, csv, or geojson)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = VoyageResource.parser.parse_args()
        params_chart = ChartDepartureDestination.parser.parse_args()
        format = params_chart.get('format')
        nest_in_data = params_chart.get('nest_in_data')

        params.update(**params_chart)
        params.update(**{
            'pivot_by': ['destination_region'],
            'pivot_value': 'value_tonne',
            'use_eu': True,
            'commodity_origin_iso2': 'RU',
            # 'date_from': '2022-01-01',
            'pricing_scenario': [base.PRICING_DEFAULT],
            # 'sort_by': ['value_tonne'],
            'currency': 'EUR',
            'keep_zeros': True,
            'format': 'json',
            'nest_in_data': True
        })

        response = VoyageResource().get_from_params(params)
        data = pd.DataFrame(response.json['data'])
        data['departure_date'] = pd.to_datetime(data.departure_date)
        data['Others'] = data.Others + data['For orders']
        data.drop(['For orders'], axis=1, inplace=True)
        data.rename(columns={base.UNKNOWN: 'Unknown'}, inplace=True)
        # data['month'] = pd.to_datetime(data.date).dt.to_period('M').dt.to_timestamp()
        #
        # data = data.groupby(['destination_region', 'month', 'variable']) \
        #     .agg(Oil=('Oil', np.average),
        #          Gas=('Gas', np.average),
        #          Coal = ('Coal', np.average),
        #          ndays=('Oil', len)) \
        #     .reset_index()
        # data = data[data.ndays >= 10].drop(['ndays'], axis=1)
        #
        # # Sort by region
        # data['Total'] = data.Coal + data.Oil + data.Gas
        # regions = data.groupby(['destination_region'])['Total'].sum().sort_values(ascending=False).reset_index()[['destination_region']]
        # data = regions.merge(data).drop('Total', axis=1)

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
                             "attachment; filename=departure_by_destination.csv"})

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