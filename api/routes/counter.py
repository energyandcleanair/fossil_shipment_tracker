import json
import pandas as pd
import numpy as np
import datetime as dt
import re
import pytz
import datetime as dt
from flask import Response
from flask_restx import Resource, reqparse, inputs
import pymongo
from sqlalchemy import func
import sqlalchemy as sa
from sqlalchemy.orm import aliased
from sqlalchemy import case

import base
from . import routes_api

from base.logger import logger
from base.db import session
from base.models import Counter, Commodity, Country, Currency
from base.utils import to_datetime, to_list, intersect, df_to_json


@routes_api.route('/v0/counter', strict_slashes=False)
class RussiaCounterResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('cumulate', type=inputs.boolean, help='whether or not to cumulate (i.e. sum) data over time',
                        required=False,
                        default=False)
    parser.add_argument('fill_with_estimates', type=inputs.boolean, help='whether or not to fill late days with estimates',
                        required=False,
                        default=False)
    parser.add_argument('use_eu', type=inputs.boolean,
                        help='use EU instead of EU28',
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
    parser.add_argument('date_from', type=str, help='start date for counter data (format 2020-01-15)',
                        default="2022-02-24", required=False)
    parser.add_argument('date_to', type=str, help='end date for arrival (format 2020-01-15)', required=False,
                        default=dt.datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument('destination_iso2',  action='split', help='ISO2(s) of country of interest',
                        required=False, default=None)
    parser.add_argument('destination_region', action='split', help='region(s) of destination e.g. EU,Turkey',
                        required=False,
                        default=None)
    parser.add_argument('commodity', action='split',
                        help='commodity to include e.g. crude_oil,oil_products,lng (see commodity endpoint to get the whole list). Defaults to all.',
                        required=False,
                        default=None)
    parser.add_argument('commodity_group', action='split', help='commodity group(s) to include e.g. oil,coal,gas Defaults to all.',
                        required=False,
                        default=None)
    parser.add_argument('currency', action='split', help='currency(ies) of returned results e.g. EUR,USD,GBP',
                        required=False,
                        default=['EUR', 'USD'])
    parser.add_argument('nest_in_data', help='Whether to nest the json content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('sort_by', type=str, help='sorting results e.g. asc(commodity),desc(value_eur)',
                        required=False, action='split', default=None)

    @routes_api.expect(parser)
    def get(self):

        params = RussiaCounterResource.parser.parse_args()
        format = params.get("format")
        cumulate = params.get("cumulate")
        rolling_days = params.get("rolling_days")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        aggregate_by = params.get("aggregate_by")
        destination_iso2 = params.get("destination_iso2")
        destination_region = params.get("destination_region")
        commodity = params.get("commodity")
        commodity_group = params.get("commodity_group")
        fill_with_estimates = params.get("fill_with_estimates")
        nest_in_data = params.get("nest_in_data")
        use_eu = params.get("use_eu")
        currency = params.get("currency")
        sort_by = params.get("sort_by")

        if aggregate_by and '' in aggregate_by:
            aggregate_by.remove('')

        destination_region_field = case(
            [
                (sa.and_(use_eu, Counter.destination_iso2 == 'GB'), 'United Kingdom'),
                (sa.and_(use_eu, Country.region == 'EU28', Counter.destination_iso2 != 'GB'), 'EU')
            ],
            else_=Country.region
        ).label('destination_region')

        value_currency_field = (Counter.value_eur * Currency.per_eur).label('value_currency')

        query = session.query(
                Counter.commodity,
                Commodity.group.label("commodity_group"),
                Counter.destination_iso2,
                Country.name.label('destination_country'),
                destination_region_field,
                Counter.date,
                Counter.value_tonne,
                Counter.value_eur,
                Counter.type,
                Currency.currency,
                value_currency_field
            ) \
            .outerjoin(Commodity, Counter.commodity == Commodity.id) \
            .join(Country, Counter.destination_iso2 == Country.iso2) \
            .outerjoin(Currency, Counter.date == Currency.date) \
            .filter(Counter.date >= to_datetime(date_from)) \
            .filter(sa.or_(fill_with_estimates, Counter.type != base.COUNTER_ESTIMATED))

        if destination_iso2:
            query = query.filter(Counter.destination_iso2.in_(to_list(destination_iso2)))

        if destination_region:
            query = query.filter(destination_region_field.in_(to_list(destination_region)))

        if commodity:
            query = query.filter(Commodity.id.in_(to_list(commodity)))

        if commodity_group:
            query = query.filter(Commodity.group.in_(to_list(commodity_group)))

        if currency is not None:
            query = query.filter(Currency.currency.in_(to_list(currency)))

        if date_to is not None:
            query = query.filter(Counter.date <= to_datetime(date_to))

        query = self.aggregate(query, aggregate_by)
        counter = pd.read_sql(query.statement, session.bind)
        counter.replace({np.nan: None}, inplace=True)

        if "id" in counter:
            counter.drop(["id"], axis=1, inplace=True)

        # Resample
        if "date" in counter:
            daterange = pd.date_range(min(counter.date), max(counter.date)).rename("date")
            counter["date"] = pd.to_datetime(counter["date"]).dt.floor('D')  # Should have been done already
            cols = intersect(["commodity", "commodity_group", 'destination_iso2',
                                    'destination_country', "destination_region", 'type', 'currency'], counter.columns)

            counter = counter \
                .groupby(cols) \
                .apply(lambda x: x.set_index("date") \
                       .resample("D").sum() \
                       .reindex(daterange) \
                       # .drop(cols, axis=1) \
                       .fillna(0)) \
                .reset_index() \
                .sort_values(intersect(['commodity', 'date'], counter.columns))



        if cumulate and "date" in counter:
            groupby_cols = [x for x in ['commodity', 'commodity_group', 'destination_iso2', 'destination_country', 'destination_region', 'currency'] if aggregate_by is None or not aggregate_by or x in aggregate_by]
            counter['value_eur'] = counter.groupby(groupby_cols)['value_eur'].transform(pd.Series.cumsum)
            counter['value_tonne'] = counter.groupby(groupby_cols)['value_tonne'].transform(pd.Series.cumsum)
            counter['value_currency'] = counter.groupby(groupby_cols)['value_currency'].transform(pd.Series.cumsum)


        if rolling_days is not None and rolling_days > 1:
            counter = counter \
                .groupby(intersect(["commodity", "commodity_group", 'destination_iso2',
                                    'destination_country',
                                    "destination_region", 'currency'], counter.columns)) \
                .apply(lambda x: x.set_index('date') \
                       .resample("D").sum() \
                       .reindex(daterange) \
                       .fillna(0) \
                       .rolling(rolling_days, min_periods=rolling_days) \
                       .mean()) \
                .reset_index() \
                .replace({np.nan: None})

        # Spread currencies
        counter = self.spread_currencies(result=counter)

        # Sort results
        counter = self.sort_result(result=counter, sort_by=sort_by)

        if format == "csv":
            return Response(
                response=counter.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=counter.csv"})

        if format == "json":
            return Response(
                response=df_to_json(counter, nest_in_data=nest_in_data),
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
            func.sum(subquery.c.value_eur).label("value_eur"),
            func.sum(subquery.c.value_currency).label("value_currency"),
        ]

        # Adding must have grouping columns
        must_group_by = ['currency']
        aggregate_by.extend([x for x in must_group_by if x not in aggregate_by])
        if '' in aggregate_by:
            aggregate_by.remove('')

        # Aggregating
        aggregateby_cols_dict = {
            'currency': [subquery.c.currency],
            'date': [subquery.c.date],
            'commodity': [subquery.c.commodity, subquery.c.commodity_group],
            'commodity_group': [subquery.c.commodity_group],
            'destination_iso2': [subquery.c.destination_iso2, subquery.c.destination_country, subquery.c.destination_region],
            'destination_country': [subquery.c.destination_iso2, subquery.c.destination_country, subquery.c.destination_region],
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


    def spread_currencies(self, result):
        len_before = len(result)
        n_currencies = len(result.currency.unique())

        result['currency'] = 'value_' + result.currency.str.lower()
        index_cols = list(set(result.columns) - set(['currency', 'value_currency', 'value_eur']))

        result = result[index_cols + ['currency', 'value_currency']] \
            .set_index(index_cols + ['currency'])['value_currency'] \
            .unstack(-1) \
            .reset_index()

        # Quick sanity check
        len_after = len(result)
        assert len_after == len_before / n_currencies
        result.replace({np.nan: None}, inplace=True)

        return result

    def sort_result(self, result, sort_by):
        by = []
        ascending = []
        default_ascending = True
        if sort_by:
            for s in sort_by:
                m = re.match("(.*)\\((.*)\\)", s)
                if m:
                    ascending.append(m[1] == "asc")
                    by.append(m[2])
                else:
                    # No asc(.*) or desc(.*)
                    ascending.append(default_ascending)
                    by.append(s)

            result.sort_values(by=by, ascending=ascending, inplace=True)

        return result
