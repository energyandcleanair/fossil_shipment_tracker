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
from sqlalchemy import case

import base
from . import routes_api
from base.encoder import JsonEncoder
from base.logger import logger
from base.db import session
from base.models import Counter, Commodity, Country, CurrencyExchange
from base.utils import to_datetime, to_list, intersect


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
    parser.add_argument('date_from', help='start date for counter data (format 2020-01-15)',
                        default="2022-02-24", required=False)
    parser.add_argument('destination_iso2', type=str, help='ISO2 of country of interest',
                        required=False, default=None)
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
        destination_iso2 = params.get("destination_iso2")
        destination_region = params.get("destination_region")
        fill_with_estimates = params.get("fill_with_estimates")
        use_eu = params.get("use_eu")

        if aggregate_by and '' in aggregate_by:
            aggregate_by.remove('')

        destination_region_field = case(
            [
                (sa.and_(use_eu, Counter.destination_iso2 == 'GB'), 'United Kingdom'),
                (sa.and_(use_eu, Country.region == 'EU28', Counter.destination_iso2 != 'GB'), 'EU')
            ],
            else_=Country.region
        ).label('destination_region')

        value_usd_field = (
                Counter.value_eur * CurrencyExchange.usd_per_eur
        ).label('value_usd')

        value_gbp_field = (
                Counter.value_eur * CurrencyExchange.gbp_per_eur
        ).label('value_gbp')

        value_jpy_field = (
                Counter.value_eur * CurrencyExchange.jpy_per_eur
        ).label('value_jpy')

        value_krw_field = (
                Counter.value_eur * CurrencyExchange.krw_per_eur
        ).label('value_krw')

        query = session.query(
                Counter.commodity,
                Commodity.group.label("commodity_group"),
                Counter.destination_iso2,
                Country.name.label('destination_country'),
                destination_region_field,
                Counter.date,
                Counter.value_tonne,
                Counter.value_eur,
                value_usd_field,
                value_gbp_field,
                value_jpy_field,
                value_krw_field,
                Counter.type
            ) \
            .outerjoin(Commodity, Counter.commodity == Commodity.id) \
            .outerjoin(CurrencyExchange, CurrencyExchange.date == Counter.date) \
            .join(Country, Counter.destination_iso2 == Country.iso2) \
            .filter(Counter.date >= to_datetime(date_from)) \
            .filter(sa.or_(fill_with_estimates, Counter.type != base.COUNTER_ESTIMATED))

        if destination_iso2:
            query = query.filter(Counter.destination_iso2.in_(to_list(destination_iso2)))

        if destination_region:
            query = query.filter(destination_region_field.in_(to_list(destination_region)))

        query = self.aggregate(query, aggregate_by)

        counter = pd.read_sql(query.statement, session.bind)
        counter.replace({np.nan: None}, inplace=True)

        if "id" in counter:
            counter.drop(["id"], axis=1, inplace=True)

        # Resample
        if "date" in counter:
            daterange = pd.date_range(min(counter.date), dt.datetime.today()).rename("date")
            counter["date"] = pd.to_datetime(counter["date"]).dt.floor('D')  # Should have been done already
            cols = intersect(["commodity", "commodity_group", 'destination_iso2',
                                    'destination_country', "destination_region", 'type'], counter.columns)
            counter = counter \
                .groupby(cols) \
                .apply(lambda x: x.set_index("date") \
                       .resample("D").sum() \
                       .reindex(daterange) \
                       .drop(cols, axis=1) \
                       .fillna(0)) \
                .reset_index() \
                .sort_values(intersect(['commodity', 'date'], counter.columns))



        if cumulate and "date" in counter:
            groupby_cols = [x for x in ['commodity', 'commodity_group', 'destination_iso2', 'destination_country', 'destination_region'] if aggregate_by is None or not aggregate_by or x in aggregate_by]
            counter['value_eur'] = counter.groupby(groupby_cols)['value_eur'].transform(pd.Series.cumsum)
            counter['value_usd'] = counter.groupby(groupby_cols)['value_usd'].transform(pd.Series.cumsum)
            counter['value_jpy'] = counter.groupby(groupby_cols)['value_jpy'].transform(pd.Series.cumsum)
            counter['value_gbp'] = counter.groupby(groupby_cols)['value_gbp'].transform(pd.Series.cumsum)
            counter['value_krw'] = counter.groupby(groupby_cols)['value_krw'].transform(pd.Series.cumsum)
            counter['value_tonne'] = counter.groupby(groupby_cols)['value_tonne'].transform(pd.Series.cumsum)


        if rolling_days is not None and rolling_days > 1:
            counter = counter \
                .groupby(intersect(["commodity", "commodity_group", 'destination_iso2',
                                    'destination_country',
                                    "destination_region"], counter.columns)) \
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
            func.sum(subquery.c.value_eur).label("value_eur"),
            func.sum(subquery.c.value_usd).label("value_usd"),
            func.sum(subquery.c.value_gbp).label("value_gbp"),
            func.sum(subquery.c.value_krw).label("value_krw"),
            func.sum(subquery.c.value_jpy).label("value_jpy"),
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
