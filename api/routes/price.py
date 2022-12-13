import json
import pandas as pd
import datetime as dt
import numpy as np
from flask import Response
from flask_restx import Resource, reqparse, inputs
from sqlalchemy import func

from base.models import PriceNew, Port, Currency
from base.encoder import JsonEncoder
from base.db import session
from base.utils import to_list, to_datetime
from base import PRICING_DEFAULT
from . import routes_api


@routes_api.route('/v0/price', methods=['GET'], strict_slashes=False)
class PriceResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('commodity', help='commodity(ies) of interest. Default: returns all of them',
                        default=None, action='split', required=False)
    parser.add_argument('date_from', help='start date (format 2020-01-15)',
                        default="2022-01-01", required=False)

    parser.add_argument('date_to', type=str, help='end date (format 2020-01-15)', required=False,
                        default=dt.datetime.today().strftime("%Y-%m-%d"))

    parser.add_argument('scenario', help='Pricing scenario (standard or pricecap)',
                        default=PRICING_DEFAULT,
                        required=False)

    parser.add_argument('nest_in_data', help='Whether to nest the json content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = PriceResource.parser.parse_args()
        commodity = params.get("commodity")
        date_from = params.get("date_from")
        scenario = params.get("scenario")
        date_to = params.get("date_to")
        format = params.get("format")
        nest_in_data = params.get("nest_in_data")

        query = PriceNew.query.filter(PriceNew.scenario==scenario)

        if commodity is not None:
            query = query.filter(PriceNew.commodity.in_(to_list(commodity)))

        if date_from is not None:
            query = query.filter(PriceNew.date >= dt.datetime.strptime(date_from, "%Y-%m-%d"))

        if date_to is not None:
            query = query.filter(PriceNew.date <= dt.datetime.strptime(date_to, "%Y-%m-%d"))

        price_df = pd.read_sql(query.statement, session.bind)
        price_df.replace({np.nan: None}, inplace=True)

        if format == "csv":
            return Response(
                response=price_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=prices.csv"})

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps({"data": price_df.to_dict(orient="records")}, cls=JsonEncoder)
            else:
                resp_content = json.dumps(price_df.to_dict(orient="records"), cls=JsonEncoder)

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json')



@routes_api.route('/v0/portprice', methods=['GET'], strict_slashes=False)
class PortPriceResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('unlocode', help='UNLOCODE',
                        default=None, action='split', required=False)
    parser.add_argument('commodity', help='commodity(ies) of interest. Default: returns all of them',
                        default=None, action='split', required=False)
    parser.add_argument('date_from', help='start date (format 2020-01-15)',
                        default="2022-01-01", required=False)
    parser.add_argument('date_to', type=str, help='end date (format 2020-01-15)', required=False,
                        default=dt.datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument('scenario', help='Pricing scenario (standard or pricecap)',
                        default=PRICING_DEFAULT,
                        required=False)
    parser.add_argument('nest_in_data', help='Whether to nest the json content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = PortPriceResource.parser.parse_args()
        unlocode = params.get("unlocode")
        commodity = params.get("commodity")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        scenario = params.get("scenario")
        format = params.get("format")
        nest_in_data = params.get("nest_in_data")

        unnested_query = session.query(PriceNew.date,
                                       PriceNew.commodity,
                                       PriceNew.date,
                                       PriceNew.scenario,
                                       PriceNew.destination_iso2s,
                                       func.unnest(PriceNew.departure_port_ids).label('port_id'),
                                       (Currency.per_eur * PriceNew.eur_per_tonne).label('usd_per_tonne'),
                                       (Currency.per_eur * PriceNew.eur_per_tonne * 0.138).label('usd_per_barrel')
                              ) \
            .join(Currency, Currency.date == PriceNew.date) \
            .filter(PriceNew.scenario.in_(to_list(scenario)),
                    Currency.currency == 'USD') \
            .subquery()

        query = session.query(unnested_query) \
            .join(Port, Port.id == unnested_query.c.port_id)

        if unlocode is not None:
            query = query.filter(Port.unlocode.in_(to_list(unlocode)))

        if commodity is not None:
            query = query.filter(PriceNew.commodity.in_(to_list(commodity)))

        if date_from is not None:
            query = query.filter(PriceNew.date >= to_datetime(date_from))

        if date_to is not None:
            query = query.filter(PriceNew.date <= to_datetime(date_to))

        price_df = pd.read_sql(query.statement, session.bind)
        price_df.replace({np.nan: None}, inplace=True)
        price_df.sort_values(['date'], inplace=True)

        if format == "csv":
            return Response(
                response=price_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=portprices.csv"})

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps({"data": price_df.to_dict(orient="records")}, cls=JsonEncoder)
            else:
                resp_content = json.dumps(price_df.to_dict(orient="records"), cls=JsonEncoder)

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json')