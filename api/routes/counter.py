import json
import pandas as pd
import numpy as np
import datetime as dt
import pytz
import datetime as dt
from flask import Response
from flask_restx import Resource, reqparse, inputs
import pymongo
from sqlalchemy import func
import sqlalchemy as sa
from sqlalchemy.orm import aliased

import base
from . import routes_api
from base.encoder import JsonEncoder
from base.logger import logger
from base.db import session
from base.models import Counter, Commodity
from base.utils import to_datetime, to_list, intersect


@routes_api.route('/v0/counter_last', strict_slashes=False)
class RussiaCounterLastResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('destination_region', type=str, help='EU28,China etc.',
                        required=False, default=None)
    parser.add_argument('date_from', type=str, help='date at which counter should start',
                        required=False, default='2022-02-24')
    parser.add_argument('date_to', type=str, help='date at which counter should stop',
                        required=False, default=None)
    parser.add_argument('fill_with_estimates', type=inputs.boolean,
                        help='whether or not to fill late days with estimates',
                        required=False,
                        default=False)
    parser.add_argument('aggregate_by', help='aggregation e.g. commodity_group,destination_region',
                        required=False, default=None, action='split')
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")


    @routes_api.expect(parser)
    def get(self):
        params = RussiaCounterLastResource.parser.parse_args()
        destination_region = params.get("destination_region")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        aggregate_by = params.get("aggregate_by")
        fill_with_estimates = params.get("fill_with_estimates")
        format = params.get("format")

        query = session.query(
            Counter.commodity,
            Commodity.group.label("commodity_group"),
            Counter.destination_region,
            Counter.date,
            func.sum(Counter.value_tonne).label("value_tonne"),
            func.sum(Counter.value_eur).label("value_eur")
        ) \
            .outerjoin(Commodity, Counter.commodity == Commodity.id) \
            .group_by(Counter.commodity, Counter.destination_region, Counter.date, Commodity.group)

        if destination_region:
            query = query.filter(Counter.destination_region == destination_region)

        if date_from:
            query = query.filter(Counter.date >= to_datetime(date_from))

        if date_to:
            query = query.filter(Counter.date <= to_datetime(date_to))

        if not fill_with_estimates:
            query = query.filter(Counter.type != base.COUNTER_ESTIMATED)

        # Important to force this
        query = query.filter(Counter.date <= dt.date.today())


        # Aggregate
        query = self.aggregate(query=query, aggregate_by=aggregate_by)

        counter = pd.read_sql(query.statement, session.bind)
        counter.replace({np.nan: None}, inplace=True)

        groupby_cols = [x for x in ['destination_region', 'commodity', 'commodity_group'] if
                        aggregate_by is None or not aggregate_by or x in aggregate_by]

        counter_last = self.get_last(counter=counter, groupby_cols=groupby_cols)


        now = dt.datetime.utcnow()
        counter_last["now"] = now
        counter_last['total_eur'] = counter_last.total_eur + (now - counter_last.date) / np.timedelta64(1, 'D') * counter_last.eur_per_day

        if "commodity_group" in counter_last.columns:
            counter_last = counter_last.loc[~counter_last.commodity_group.isna()]

        counter_last = counter_last.groupby(groupby_cols).sum()

        if not aggregate_by or 'destination_region' in aggregate_by:
            total = counter_last.groupby(['destination_region']).sum()
            total[groupby_cols] = "total"
            counter_last = pd.concat([
                counter_last.reset_index(),
                total
            ]).reset_index()
        else:
            counter_last.loc["total"] = counter_last.sum()
            counter_last.reset_index(inplace=True)

        counter_last['date'] = now
        counter_last['eur_per_sec'] = counter_last['eur_per_day'] / 24 / 3600

        if format == "csv":
            return Response(
                response=counter_last.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=counter_last.csv"})

        if format == "json":
            return Response(
                response=json.dumps({"data": counter_last.to_dict(orient="records")}, cls=JsonEncoder),
                status=200,
                mimetype='application/json')


    def get_last(self, counter, groupby_cols):

        counter_last = counter.sort_values(['date']) \
            .groupby(groupby_cols) \
            .agg(
            total_tonne=pd.NamedAgg(column='value_tonne', aggfunc=np.sum),
            total_eur=pd.NamedAgg(column='value_eur', aggfunc=np.sum),
            date=pd.NamedAgg(column='date', aggfunc='last')) \
            .reset_index()

        n_days = 7
        shift_days = 2
        daterange = pd.date_range(dt.datetime.today() - dt.timedelta(days=n_days+1),
                                  dt.datetime.today()).rename("date")

        def resample_and_fill(x):
            x = x.set_index("date") \
                .resample("D").sum() \
                .fillna(0)
            # cut 2 last days and take the 7-day mean
            means = x[["value_tonne", "value_eur"]].shift(shift_days).tail(7).mean()

            x = x.reindex(daterange) \
                .fillna(means)
            return x

        counter_last_increment = counter \
            .sort_values(['date']) \
            .groupby(groupby_cols) \
            .apply(resample_and_fill) \
            .reset_index() \
            .rename(columns={"value_tonne": "tonne_per_day",
                             "value_eur": "eur_per_day"}) \
            .drop(['date'], axis=1) \
            .drop_duplicates()

        counter_last_merged = counter_last.merge(counter_last_increment,
                                       how='left', on=groupby_cols)
        return counter_last_merged


    def aggregate(self, query, aggregate_by):
        """Perform aggregation based on user agparameters"""

        if not aggregate_by:
            return query

        subquery = query.subquery()

        # Aggregate
        value_cols = [
            func.sum(subquery.c.value_tonne).label("value_tonne"),
            func.sum(subquery.c.value_eur).label("value_eur")
        ]

        # Adding must have grouping columns
        must_group_by = ['date']
        aggregate_by.extend([x for x in must_group_by if x not in aggregate_by])
        if '' in aggregate_by:
            aggregate_by.remove('')

        # Aggregating
        aggregateby_cols_dict = {
            'date': [subquery.c.date],
            'destination_region': [subquery.c.destination_region],
            'commodity_group': [subquery.c.commodity_group],
            'commodity': [subquery.c.commodity, subquery.c.commodity_group]
        }

        if any([x not in aggregateby_cols_dict for x in aggregate_by]):
            logger.warning("aggregate_by can only be a selection of %s" % (",".join(aggregateby_cols_dict.keys())))
            aggregate_by = [x for x in aggregate_by if x in aggregateby_cols_dict]

        groupby_cols = []
        for x in aggregate_by:
            groupby_cols.extend(aggregateby_cols_dict[x])

        query = session.query(*groupby_cols, *value_cols).group_by(*groupby_cols)
        return query



@routes_api.route('/v0/counter', strict_slashes=False)
class RussiaCounterResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('cumulate', type=inputs.boolean, help='whether or not to cumulate (i.e. sum) data over time',
                        required=False,
                        default=False)
    parser.add_argument('fill_with_estimates', type=inputs.boolean, help='whether or not to fill late days with estimates',
                        required=False,
                        default=False)
    parser.add_argument('aggregate_by', type=str, action='split',
                        default=None,
                        help='which variables to aggregate by. Could be any of commodity, type, destination_region, date')
    parser.add_argument('rolling_days', type=int,
                        help='rolling average window (in days). Default: no rolling averaging',
                        required=False, default=None)
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")
    parser.add_argument('date_from', help='start date for counter data (format 2020-01-15)',
                        default="2022-02-24", required=False)
    parser.add_argument('destination_region', action='split', help='region(s) of destination e.g. EU,Turkey',
                        required=False,
                        default=None)

    @routes_api.expect(parser)
    def get(self):

        params = RussiaCounterResource.parser.parse_args()
        format = params.get("format")
        cumulate = params.get("cumulate")
        rolling_days = params.get("rolling_days")
        date_from = params.get("date_from")
        aggregate_by = params.get("aggregate_by")
        destination_region = params.get("destination_region")
        fill_with_estimates = params.get("fill_with_estimates")

        if aggregate_by and '' in aggregate_by:
            aggregate_by.remove('')

        query = session.query(
                Counter.commodity,
                Commodity.group.label("commodity_group"),
                Counter.destination_region,
                Counter.date,
                Counter.value_tonne,
                Counter.value_eur,
                Counter.type
            ) \
            .outerjoin(Commodity, Counter.commodity == Commodity.id) \
            .filter(Counter.date >= to_datetime(date_from)) \
            .filter(sa.or_(fill_with_estimates, Counter.type != base.COUNTER_ESTIMATED))

        if destination_region:
            query = query.filter(Counter.destination_region.in_(to_list(destination_region)))

        query = self.aggregate(query, aggregate_by)

        counter = pd.read_sql(query.statement, session.bind)
        counter.replace({np.nan: None}, inplace=True)

        if "id" in counter:
            counter.drop(["id"], axis=1, inplace=True)

        # Resample
        if "date" in counter:
            daterange = pd.date_range(min(counter.date), dt.datetime.today()).rename("date")
            counter["date"] = pd.to_datetime(counter["date"]).dt.floor('D')  # Should have been done already
            counter = counter \
                .groupby(intersect(["commodity", "commodity_group", "destination_region"], counter.columns)) \
                .apply(lambda x: x.set_index("date") \
                       .resample("D").sum() \
                       .reindex(daterange) \
                       .fillna(0)) \
                .reset_index() \
                .sort_values(intersect(['commodity', 'date'], counter.columns))


        if cumulate and "date" in counter:
            groupby_cols = [x for x in ['commodity', 'commodity_group', 'destination_region'] if aggregate_by is None or not aggregate_by or x in aggregate_by]
            counter['value_eur'] = counter.groupby(groupby_cols)['value_eur'].transform(pd.Series.cumsum)
            counter['value_tonne'] = counter.groupby(groupby_cols)['value_tonne'].transform(pd.Series.cumsum)


        if rolling_days is not None and rolling_days > 1:
            counter = counter \
                .groupby(intersect(["commodity", "commodity_group", "destination_region"], counter.columns)) \
                .apply(lambda x: x.set_index('date') \
                       .resample("D").sum() \
                       .reindex(daterange) \
                       .fillna(0) \
                       .rolling(rolling_days, min_periods=rolling_days) \
                       .mean()) \
                .reset_index() \
                .replace({np.nan: None})

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


    def aggregate(self, query, aggregate_by):
        """Perform aggregation based on user agparameters"""

        if not aggregate_by:
            return query

        subquery = query.subquery()

        # Aggregate
        value_cols = [
            func.sum(subquery.c.value_tonne).label("value_tonne"),
            func.sum(subquery.c.value_eur).label("value_eur")
        ]

        # Adding must have grouping columns
        must_group_by = []
        aggregate_by.extend([x for x in must_group_by if x not in aggregate_by])
        if '' in aggregate_by:
            aggregate_by.remove('')

        # Aggregating
        aggregateby_cols_dict = {
            'date': [subquery.c.date],
            'commodity': [subquery.c.commodity, subquery.c.commodity_group],
            'commodity_group': [subquery.c.commodity_group],
            'destination_region': [subquery.c.destination_region],
            'type': [subquery.c.type]
        }

        if any([x not in aggregateby_cols_dict for x in aggregate_by]):
            logger.warning("aggregate_by can only be a selection of %s" % (",".join(aggregateby_cols_dict.keys())))
            aggregate_by = [x for x in aggregate_by if x in aggregateby_cols_dict]

        groupby_cols = []
        for x in aggregate_by:
            groupby_cols.extend(aggregateby_cols_dict[x])

        query = session.query(*groupby_cols, *value_cols).group_by(*groupby_cols)
        return query



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