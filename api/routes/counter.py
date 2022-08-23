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
from engine.commodity import get_subquery as get_commodity_subquery


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
                        default=True)
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
    parser.add_argument('destination_region_not', action='split', help='region(s) of destination to exclude e.g. For orders',
                        required=False,
                        default=None)
    parser.add_argument('commodity', action='split',
                        help='commodity to include e.g. crude_oil,oil_products,lng (see commodity endpoint to get the whole list). Defaults to all.',
                        required=False,
                        default=None)
    parser.add_argument('commodity_group', action='split', help='commodity group(s) to include e.g. oil,coal,gas Defaults to all.',
                        required=False,
                        default=None)
    parser.add_argument('commodity_grouping', type=str,
                        help="Grouping used (e.g. coal,oil,gas ('default') vs coal,oil,lng,pipeline_gas ('split_gas')",
                        default='default')
    parser.add_argument('currency', action='split', help='currency(ies) of returned results e.g. EUR,USD,GBP',
                        required=False,
                        default=['EUR', 'USD'])
    parser.add_argument('nest_in_data', help='Whether to nest the json content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('sort_by', type=str, help='sorting results e.g. asc(commodity),desc(value_eur)',
                        required=False, action='split', default=None)
    parser.add_argument('pivot_by', type=str, help='pivoting value_eur (or any other specified by pivot_value) by e.g. commodity_group.',
                        required=False, action='split', default=None)
    parser.add_argument('pivot_value', type=str, help='pivoted value. Default: value_eur.',
                        required=False, default='value_eur')
    parser.add_argument('limit', type=int, help='how many result records do you want (default: keeps all)',
                        required=False, default=None)
    parser.add_argument('limit_by', action='split',
                        help='in which group do you want to limit to n records',
                        required=False, default=None)

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
        destination_region_not = params.get("destination_region_not")
        commodity = params.get("commodity")
        commodity_group = params.get("commodity_group")
        commodity_grouping = params.get("commodity_grouping")
        fill_with_estimates = params.get("fill_with_estimates")
        nest_in_data = params.get("nest_in_data")
        use_eu = params.get("use_eu")
        currency = params.get("currency")
        sort_by = params.get("sort_by")
        pivot_by = params.get("pivot_by")
        pivot_value = params.get("pivot_value")
        limit = params.get("limit")
        limit_by = params.get("limit_by")

        if aggregate_by and '' in aggregate_by:
            aggregate_by.remove('')

        destination_region_field = case(
            [
                (sa.and_(use_eu, Counter.destination_iso2 == 'GB'), 'United Kingdom'),
                (sa.and_(use_eu, Country.region == 'EU28', Counter.destination_iso2 != 'GB'), 'EU'),
                (sa.and_(not use_eu, Counter.destination_iso2 == 'GB'), 'EU28'),
                (sa.and_(not use_eu, Country.region == 'EU'), 'EU28')
            ],
            else_=Country.region
        ).label('destination_region')

        value_currency_field = (Counter.value_eur * Currency.per_eur).label('value_currency')

        commodity_subquery = get_commodity_subquery(session=session, grouping_name=commodity_grouping)

        query = session.query(
                Counter.commodity,
                commodity_subquery.c.group.label("commodity_group"),
                commodity_subquery.c.group_name.label("commodity_group_name"),
                Counter.destination_iso2,
                Country.name.label('destination_country'),
                destination_region_field,
                Counter.date,
                Counter.value_tonne,
                Counter.value_eur,
                Currency.currency,
                value_currency_field
            ) \
            .outerjoin(commodity_subquery, Counter.commodity == commodity_subquery.c.id) \
            .join(Country, Counter.destination_iso2 == Country.iso2) \
            .outerjoin(Currency, Counter.date == Currency.date) \
            .filter(Counter.date >= to_datetime(date_from)) \
            .filter(sa.or_(fill_with_estimates, Counter.type != base.COUNTER_ESTIMATED))

        if destination_iso2:
            query = query.filter(Counter.destination_iso2.in_(to_list(destination_iso2)))

        if destination_region:
            query = query.filter(destination_region_field.in_(to_list(destination_region)))

        if destination_region_not:
            query = query.filter(destination_region_field.notin_(to_list(destination_region_not)))

        if commodity:
            query = query.filter(commodity_subquery.c.id.in_(to_list(commodity)))

        if commodity_group:
            query = query.filter(commodity_subquery.c.group.in_(to_list(commodity_group)))

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
            cols = intersect(["commodity", "commodity_group", 'commodity_group_name', 'destination_iso2',
                                    'destination_country', "destination_region",'price_type', 'currency'], counter.columns)

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
            groupby_cols = [x for x in ['commodity', 'commodity_group', 'commodity_group_name', 'destination_iso2', 'destination_country', 'destination_region', 'currency'] if aggregate_by is None or not aggregate_by or x in aggregate_by]
            counter['value_eur'] = counter.groupby(groupby_cols)['value_eur'].transform(pd.Series.cumsum)
            counter['value_tonne'] = counter.groupby(groupby_cols)['value_tonne'].transform(pd.Series.cumsum)
            counter['value_currency'] = counter.groupby(groupby_cols)['value_currency'].transform(pd.Series.cumsum)


        if rolling_days is not None and rolling_days > 1:
            counter = counter \
                .groupby(intersect(["commodity", "commodity_group", 'commodity_group_name',
                                    'destination_iso2',
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
        counter = self.sort_result(result=counter, sort_by=sort_by, aggregate_by=aggregate_by)

        # Keep only n records
        counter = self.limit_result(result=counter,
                                    limit=limit,
                                    aggregate_by=aggregate_by,
                                    sort_by=sort_by,
                                    limit_by=limit_by)

        # Pivot
        counter = self.pivot_result(result=counter, pivot_by=pivot_by, pivot_value=pivot_value)

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
            'month': [func.date_trunc('month', subquery.c.date).label("month")],
            'year': [func.date_trunc('year', subquery.c.date).label("year")],

            'commodity': [subquery.c.commodity, subquery.c.commodity_group, subquery.c.commodity_group_name],
            'commodity_group': [subquery.c.commodity_group, subquery.c.commodity_group_name],
            'destination_iso2': [subquery.c.destination_iso2, subquery.c.destination_country, subquery.c.destination_region],
            'destination_country': [subquery.c.destination_iso2, subquery.c.destination_country, subquery.c.destination_region],
            'destination_region': [subquery.c.destination_region],
            # 'type': [subquery.c.type]
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
        index_cols = [x for x in result.columns if x not in ['currency', 'value_currency', 'value_eur']]

        result = result[index_cols + ['currency', 'value_currency']] \
            .set_index(index_cols + ['currency'])['value_currency'] \
            .unstack(-1) \
            .reset_index()

        # Quick sanity check
        len_after = len(result)
        assert len_after == len_before / n_currencies
        result.replace({np.nan: None}, inplace=True)

        return result


    def sort_result(self, result, sort_by, aggregate_by):
        by = []
        ascending = []
        default_ascending = False

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

            if not aggregate_by:
                sorting_groupers = ['destination_country']

            if aggregate_by:

                dependencies = {
                    'commodity': ['commodity_group', 'commodity_group_name'],
                    'commodity_group': ['commodity', 'commodity_group_name'],
                    'commodity_group_name': ['commodity', 'commodity_group']
                }

                aggregate_by_dependencies = [d for x in to_list(aggregate_by) for d in dependencies.get(x, [])]

                sorting_groupers = [x for x in aggregate_by \
                                    if not x in aggregate_by_dependencies \
                                    and not x in ['date', 'month', 'year', 'currency'] \
                                    and x in result.columns]

            sorted = result.groupby(sorting_groupers)[sort_by].sum() \
                .reset_index() \
                .sort_values(by=by, ascending=ascending) \
                .drop(sort_by, axis=1)

            result = pd.merge(sorted, result, how='left')


            # Sort commodity group manually


        return result

    def pivot_result(self, result, pivot_by, pivot_value):

        dependencies = {
            'commodity': ['commodity_group','commodity_group_name'],
            'commodity_group': ['commodity', 'commodity_group_name'],
            'commodity_group_name': ['commodity', 'commodity_group']
        }

        if pivot_by:

            pivot_by_dependencies = [d for x in to_list(pivot_by) for d in dependencies.get(x,[])]
            index = [x for x in result.columns
                        if not x.startswith('value') \
                        and x not in to_list(pivot_by)
                        and x not in pivot_by_dependencies]

            result = result.pivot_table(index=index,
                                        columns=to_list(pivot_by),
                                        values=pivot_value,
                                        sort=False,
                                        fill_value=0).reset_index()
            result['variable'] = pivot_value

        return result

    def limit_result(self, result, limit, aggregate_by, sort_by, limit_by):

        if not limit:
            return result

        limit_by = to_list(limit_by) or []

        if not aggregate_by:
            group_by = ['destination_country']

        if aggregate_by:
            group_by = [x for x in aggregate_by \
                              if not x.startswith('commodity') \
                              and not x in ['date','month','year'] \
                              and x in result.columns]

        sort_by = sort_by or 'value_eur'
        group_by = group_by + [x for x in limit_by if x not in group_by]

        # Can only take one
        sort_by = to_list(sort_by)[0]
        top = result.groupby(group_by) \
            .agg({sort_by: 'sum'}) \
            .reset_index() \
            .sort_values(limit_by + to_list(sort_by), ascending=False)

        if limit_by:
            top = top.groupby(limit_by, as_index=False)

        top = top \
            .head(limit) \
            .drop(sort_by, axis=1)

        result = pd.merge(result, top, how='inner')

        return result
