import datetime as dt
import pandas as pd
import json
import geopandas as gpd
import re
import numpy as np
import sqlalchemy.sql.expression

from .. import routes_api
from flask_restx import inputs


from base.models import Shipment, Ship, Arrival, Departure, Port, Berth,\
    ShipOwner, ShipInsurer, ShipManager, Company, \
    ShipmentDepartureBerth, ShipmentArrivalBerth, Commodity, Trajectory, \
    Destination, Price, Country, PortPrice, Currency, ShipmentWithSTS, Event
from base.db import session
from base.encoder import JsonEncoder
from base.utils import to_list, df_to_json, to_datetime
from base.logger import logger


from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse
from ..entsogflow import EntsogFlowResource
import sqlalchemy as sa
from sqlalchemy.orm import aliased
from sqlalchemy import or_
from sqlalchemy import func
from base.utils import update_geometry_from_wkb
import country_converter as coco
import base
from engine.commodity import get_subquery as get_commodity_subquery


@routes_api.route('/v0/chart/eu_gas_consumption', strict_slashes=False)
class ChartEUGasConsumption(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('date_from', type=str, help='start date for counter data (format 2020-01-15)',
                        default="2021-01-01", required=False)
    parser.add_argument('date_to', type=str, help='start date for counter data (format 2020-01-15)',
                        default=-7,
                        required=False)
    parser.add_argument('rolling_days', type=int,
                        help='rolling average window (in days). Default: no rolling averaging',
                        required=False, default=7)
    parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('download', help='Whether to return results as a file or not.',
                        type=inputs.boolean, default=False)
    parser.add_argument('format', type=str, help='format of returned results (json, csv, or geojson)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):
        params = ChartEUGasConsumption.parser.parse_args()
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        rolling_days = params.get("rolling_days")
        format = params.get("format")
        nest_in_data = params.get("nest_in_data")
        format = params.get("format")
        download = params.get("download")


        params_entsog = {
            "format": "json",
            "download": False,
            'aggregate_by': ['type', 'departure_region', 'destination_region', 'date'],
            "date_from": to_datetime(date_from) - dt.timedelta(days=rolling_days),
            "nest_in_data": False,
            'rolling_days': rolling_days,
            'type': ['distribution', 'consumption', 'storage_entry', 'storage_exit', 'crossborder', 'production'],
            'currency': 'EUR'
        }
        
        entsog_resp = EntsogFlowResource().get_from_params(params=params_entsog)
        entsog = json.loads(entsog_resp.response[0])
        entsog_df = pd.DataFrame(entsog)


        crossborder_in =  entsog_df[(entsog_df.type=='crossborder') \
            & (entsog_df.destination_region == 'EU') \
            & (entsog_df.departure_region != 'EU')]
        crossborder_in['type'] = 'crossborder_in'


        crossborder_out = entsog_df[(entsog_df.type == 'crossborder') \
                                   & (entsog_df.destination_region != 'EU') \
                                   & (entsog_df.departure_region == 'EU')]
        crossborder_out['type'] = 'crossborder_out'


        others =  entsog_df[(entsog_df.type != 'crossborder') \
                                   & (entsog_df.destination_region == 'EU')]

        merged = pd.concat([crossborder_in[['type', 'date', 'value_m3']],
                            crossborder_out[['type', 'date', 'value_m3']],
                            others[['type', 'date', 'value_m3']]],
                           axis=0,
                           ignore_index=True) \
            .groupby(['type', 'date']) \
            .agg({'value_m3': np.nansum}) \
            .reset_index()

        wide = pd.pivot(merged,
                 index='date',
                 columns='type',
                 values='value_m3').reset_index()

        storage_drawdown = 'Storage drawdown'
        imports = 'Imports'
        implied_consumption = 'Implied consumption'
        production = 'Production'

        wide['date'] = pd.to_datetime(wide.date).dt.date
        wide[storage_drawdown] = wide.storage_entry - wide.storage_exit
        wide[production] = wide.production
        wide[imports] = wide.crossborder_in - wide.crossborder_out
        wide[implied_consumption] = wide[imports] + wide.production + wide[storage_drawdown]

        if date_from:
            wide = wide[wide.date >=pd.to_datetime(to_datetime(date_from))]

        if date_to:
            wide = wide[wide.date <= pd.to_datetime(to_datetime(date_to))]


        return self.build_response(result=wide,
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
                             "attachment; filename=chart_gas_consumption.csv"})

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