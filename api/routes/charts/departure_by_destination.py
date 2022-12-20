import pandas as pd
import json
import numpy as np
import datetime as dt
import re
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
                        default=-3,
                        required=False)

    parser.add_argument('country_grouping', type=str,
                        help="How to group countries. Can be 'region' or 'top_n' (e.g. top_5)",
                        default='top_8')

    parser.add_argument('commodity_grouping', type=str,
                        help="Grouping used (e.g. coal,oil,gas ('default') vs coal,oil,lng,pipeline_gas ('split_gas')",
                        default='split_gas_oil')

    parser.add_argument('commodity', help='Commodity(ies) of interest',
                        action='split',
                        required=False,
                        default=['crude_oil', 'oil_products', 'oil_or_chemical'])

    parser.add_argument('aggregate_by', type=str, action='split',
                        default=['destination_country', 'commodity_group', 'departure_date', 'status'],
                        help='which variables to aggregate by. Could be any of commodity, type, destination_region, date')

    parser.add_argument('language', type=str, help='en or ua',
                        default="en", required=False)

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
        country_grouping = params_chart.get('country_grouping')
        language = params_chart.get('language')

        params.update(**params_chart)
        params.update(**{
            # 'pivot_by': ['destination_region'],
            # 'pivot_value': 'value_tonne',
            'use_eu': True,
            'commodity_origin_iso2': 'RU',
            'commodity_destination_iso2_not': 'RU',
            # 'date_from': '2022-01-01',
            'pricing_scenario': [base.PRICING_DEFAULT],
            # 'sort_by': ['value_tonne'],
            'currency': 'EUR',
            'keep_zeros': True,
            'format': 'json',
            'nest_in_data': True
        })

        def group_countries(data, country_grouping):
            import re
            if re.search('top_[0-9]*', country_grouping):
                # Make EU a country
                data.loc[data.destination_region == 'EU', 'destination_country'] = 'EU'
                data.loc[data.destination_region == 'EU', 'destination_iso2'] = 'EU'

                n = int(country_grouping.replace('top_', ''))
                top_n = data[data.departure_date >= max(data.departure_date) - dt.timedelta(days=30)] \
                        .groupby(['commodity_group', 'destination_country']) \
                    .value_tonne.sum() \
                    .reset_index() \
                    .sort_values('value_tonne', ascending=False) \
                    .groupby(['commodity_group']) \
                    .head(n)

                top_n['region'] = top_n.destination_country
                # Keeping the same for all commodities
                # Otherwise Flourish will show empty lines
                # which might make viewer things values are actually 0
                top_n = top_n[['destination_country', 'region']].drop_duplicates()
                data = data \
                    .merge(top_n[['destination_country', 'region']],
                           how='left') \
                    .fillna({'region': 'Others'})

                # Keep for orders
                data.loc[data.destination_iso2 == base.FOR_ORDERS, 'region'] = 'For orders'

            else:
                data['region'] = data.destination_region

            data = data.groupby(['commodity_group', 'commodity_group_name',
                                 'region', 'departure_date'])['value_tonne'].sum() \
                .reset_index() \
                .sort_values(['departure_date'])

            return data

        def pivot_data(data, variable='value_tonne'):

            # Add the variable for transparency sake
            data['variable'] = variable
            result = data.groupby(['region', 'departure_date', 'commodity_group_name', 'variable']) \
                .value_tonne.sum() \
                .reset_index() \
                .pivot_table(index=['commodity_group_name', 'departure_date', 'variable'],
                             columns=['region'],
                             values=variable,
                             sort=False,
                             fill_value=0) \
                .reset_index()
            return result

        def translate(data, language):
            if language != "en":
                file_path = "assets/language/%s.json" % (language)
                with open(file_path, 'r') as file:
                    translate_dict = json.load(file)

                data = data.replace(translate_dict)
                data.columns = [translate_dict.get(x, x) for x in data.columns]

            return data


        response = VoyageResource().get_from_params(params)

        data = pd.DataFrame(response.json['data'])
        data = data[data.destination_iso2 != 'RU']
        data['departure_date'] = pd.to_datetime(data.departure_date)
        data.replace({base.UNKNOWN: 'Unknown'}, inplace=True)

        # Also remove shipments to EU since 2022-12-05 until we can verify these are correct/breaking sanctions
        # Any ongoing shipments do not show as to EU - this can look misleading so set them as unknown
        data['destination_region'] = np.where(((data['status'] == 'ongoing')
                                                        & (data['commodity_group_name'] == 'Crude oil')
                                                        & (data['destination_region'] == 'EU'))
                                                        |
                                                        (
                                                       (data['destination_region'] == 'EU')
                                                       & (data['commodity_group_name'] == 'Crude ol')
                                                       & (data['departure_date'] > '2022-12-05')
                                              ), 'Unknown',
                                                        data['destination_region'])

        data = data.drop('status', axis=1)

        data = group_countries(data, country_grouping)
        data = pivot_data(data)
        data = translate(data=data, language=language)
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