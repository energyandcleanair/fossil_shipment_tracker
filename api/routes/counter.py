import json
import pandas as pd
import numpy as np
import pytz
import datetime as dt
from flask import Response
from flask_restx import Resource, reqparse, inputs
import pymongo
from sqlalchemy import func
import sqlalchemy as sa
from sqlalchemy.orm import aliased


from . import routes_api
from base.encoder import JsonEncoder
from base.db import session
from base.models import Counter


@routes_api.route('/v0/counter', strict_slashes=False)
class RussiaCounterResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('cumulate', type=inputs.boolean, help='whether or not to cumulate (i.e. sum) data over time',
                        required=False,
                        default=False)
    parser.add_argument('rolling_days', type=int,
                        help='rolling average window (in days). Default: no rolling averaging',
                        required=False, default=None)
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = RussiaCounterResource.parser.parse_args()
        format = params.get("format")
        cumulate = params.get("cumulate")
        rolling_days = params.get("rolling_days")

        query = Counter.query
        counter = pd.read_sql(query.statement, session.bind)
        counter.replace({np.nan: None}, inplace=True)
        counter.drop(["id"], axis=1, inplace=True)

        # Resample
        import datetime as dt
        daterange = pd.date_range(min(counter.date), dt.datetime.today()).rename("date")
        counter["date"] = pd.to_datetime(counter["date"]).dt.floor('D')  # Should have been done already
        counter = counter \
            .groupby(["commodity", "destination_region"]) \
            .apply(lambda x: x.set_index("date") \
                   .resample("D").sum() \
                   .reindex(daterange) \
                   .fillna(0)) \
            .reset_index() \
            .sort_values(['commodity', 'date'])


        if cumulate:
            counter['value_eur'] = counter.groupby(['commodity','destination_region'])['value_eur'].transform(pd.Series.cumsum)
            counter['value_tonne'] = counter.groupby(['commodity','destination_region'])['value_tonne'].transform(pd.Series.cumsum)

        if rolling_days is not None and rolling_days > 1:
            counter = counter \
                .groupby(["commodity", "destination_region"]) \
                .apply(lambda x: x.set_index('date') \
                       .resample("D").sum() \
                       .reindex(daterange) \
                       .fillna(0) \
                       .rolling(rolling_days, min_periods=rolling_days) \
                       .mean()) \
                .reset_index()

        if format == "csv":
            return Response(
                response=counter.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=counter.csv"})

        if format == "json":
            return Response(
                response=json.dumps({"data": counter.to_dict(orient="records")}, cls=JsonEncoder),
                status=200,
                mimetype='application/json')

# def get_counter_prices():
#     from base.db import mongo_client
#     db = mongo_client['russian_fossil']
#     prices = db.get_collection("counter_prices").find().sort("date", pymongo.DESCENDING)
#     prices_df = pd.DataFrame(list(prices))
#     prices_df = prices_df[["commodity", "date", "eur_per_tonne", "value", "unit"]]
#     return prices_df
#
#
# @routes_api.route('/v1/russia_counter', strict_slashes=False)
# class RussiaCounterResource(Resource):
#
#     parser = reqparse.RequestParser()
#
#     @routes_api.expect(parser)
#     def get(self):
#
#         from base.db import mongo_client
#         db = mongo_client['russian_fossil']
#         last_item = db.get_collection("counter").find().sort("date", pymongo.DESCENDING).limit(1)[0]
#         timezone = pytz.timezone("UTC")
#         try:
#             last_date = timezone.localize(dt.datetime.strptime(last_item['date'], "%Y-%m-%d %H:%M:%S"))
#         except:
#             last_date = timezone.localize(dt.datetime.strptime(last_item['date'], "%Y-%m-%d"))
#         n_days = dt.datetime.now(dt.timezone.utc) - last_date
#         n_days = n_days.total_seconds() / 24 / 3600
#
#         result = {
#             'date': str(dt.datetime.now(dt.timezone.utc))
#         }
#
#         commodities = ['coal_eur', 'gas_eur', 'oil_eur', 'total_eur']
#         for c in commodities:
#             result[c] = last_item['cumulated_' + c] + last_item[c] * n_days
#             result[c + '_per_sec'] = last_item[c] / 24 / 3600
#
#         response = Response(
#             response=json.dumps(result),
#             status=200,
#             mimetype='application/json'
#         )
#         return response
#
#
#
#
#
# @routes_api.route('/v1/russia_counter_prices', strict_slashes=False)
# class RussiaCounterPriceResource(Resource):
#
#     parser = reqparse.RequestParser()
#     parser.add_argument('format', type=str, help='format of returned results (json or csv)',
#                         required=False, default="json")
#
#     @routes_api.expect(parser)
#     def get(self):
#
#         params = RussiaCounterPriceResource.parser.parse_args()
#         format = params.get("format")
#         prices_df = get_counter_prices()
#
#         if format == "json":
#             response = Response(
#                 response=json.dumps(prices_df.to_dict(orient='records')),
#                 status=200,
#                 mimetype='application/json'
#             )
#             return response
#
#         if format == "csv":
#             response = Response(
#                 response=prices_df.to_csv(),
#                 mimetype="text/csv",
#                 headers={"Content-disposition":
#                              "attachment; filename=counter_prices.csv"})
#
#             return response