import json
import pandas as pd
import datetime as dt
from sqlalchemy import func
from flask import Response
from flask_restx import Resource, reqparse, inputs
import numpy as np


from base.models import Departure, Port, Ship, Country
from base.encoder import JsonEncoder
from base.db import session
from base.utils import to_datetime, to_list
from base.logger import logger

from . import routes_api



@routes_api.route('/v0/departure', strict_slashes=False)
class DepartureResource(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('unlocode', required=False, help='unlocode(s) of departure port', action='split')
    parser.add_argument('iso2', required=False, help='iso2(s) of departure port', action='split')
    parser.add_argument('commodity', help='commodity(ies) of interest. Default: returns all of them',
                        default=None, action='split', required=False)
    parser.add_argument('date_from', help='start date for arrival (format 2020-01-15)',
                        default="2022-01-01", required=False)
    parser.add_argument('date_to', type=str, help='end date for arrival (format 2020-01-15)', required=False,
                        default=dt.datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument('aggregate_by', type=str, action='split',
                        default=None,
                        help='which variables to aggregate by. Could be any of commodity, date')
    parser.add_argument('rolling_days', type=int,
                        help='rolling average window (in days). Default: no rolling averaging',
                        required=False, default=None)
    parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('format', type=str, help='format of returned results (json or csv)',
                        required=False, default="json")

    @routes_api.expect(parser)
    def get(self):

        params = DepartureResource.parser.parse_args()
        unlocode = params.get("unlocode")
        iso2 = params.get("iso2")
        commodity = params.get("commodity")
        format = params.get("format")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        nest_in_data = params.get("nest_in_data")
        aggregate_by = params.get("aggregate_by")
        rolling_days = params.get("rolling_days")

        query = session.query(Departure,
                              Ship.commodity,
                              Port.name.label('port_name'),
                              Port.iso2.label('port_iso2'),
                              Country.name.label('port_country'),
                              Country.region.label('port_region'),
                              Port.area.label('port_area'),
                              Ship.name.label('ship_name'),
                              Ship.dwt.label('ship_dwt'),) \
            .join(Port, Departure.port_id == Port.id) \
            .outerjoin(Country, Port.iso2 == Country.iso2) \
            .join(Ship, Departure.ship_imo == Ship.imo)

        if unlocode is not None:
            query = query.filter(Departure.port_unlocode.in_(to_list(unlocode)))

        if iso2 is not None:
            query = query.filter(Port.iso2.in_(to_list(iso2)))

        if commodity is not None:
            query = query.filter(Ship.commodity.in_(to_list(commodity)))

        if date_from is not None:
            query = query.filter(Departure.date_utc >= to_datetime(date_from))

        if date_to is not None:
            query = query.filter(Departure.date_utc <= to_datetime(date_to))

        query = self.aggregate(query, aggregate_by=aggregate_by)
        result = pd.read_sql(query.statement, session.bind)

        result = self.roll_average(result=result, aggregate_by=aggregate_by, rolling_days=rolling_days)

        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=departures.csv"})

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps({"data": result.to_dict(orient="records")}, cls=JsonEncoder)
            else:
                resp_content = json.dumps(result.to_dict(orient="records"), cls=JsonEncoder)

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json')


    def aggregate(self, query, aggregate_by):
        """Perform aggregation based on user parameters"""

        if not aggregate_by:
            return query

        subquery = query.subquery()

        # Aggregate
        value_cols = [
            func.sum(subquery.c.ship_dwt).label("ship_dwt"),
            func.count(subquery.c.id).label("count"),
        ]

        # Adding must have grouping columns
        must_group_by = []
        aggregate_by.extend([x for x in must_group_by if x not in aggregate_by])
        if '' in aggregate_by:
            aggregate_by.remove('')
        # Aggregating

        aggregateby_cols_dict = {
            'commodity': [subquery.c.commodity],

            'date': [func.date_trunc('day', subquery.c.date_utc).label("date")],
            'month': [func.date_trunc('month', subquery.c.date_utc).label("month")],
            'year': [func.date_trunc('year', subquery.c.date_utc).label("year")],

            'port_area': [subquery.c.port_area],
            'port_country': [subquery.c.port_iso2, subquery.c.port_country, subquery.c.port_region],
        }

        if any([x not in aggregateby_cols_dict for x in aggregate_by]):
            logger.warning("aggregate_by can only be a selection of %s" % (",".join(aggregateby_cols_dict.keys())))
            aggregate_by = [x for x in aggregate_by if x in aggregateby_cols_dict]

        groupby_cols = []
        for x in aggregate_by:
            groupby_cols.extend(aggregateby_cols_dict[x])

        query = session.query(*groupby_cols, *value_cols).group_by(*groupby_cols)
        return query


    def roll_average(self, result, aggregate_by, rolling_days):

        if rolling_days is not None:
            date_column = None
            if aggregate_by is not None and "date" in aggregate_by:
                date_column = "date"
            if date_column is None:
                logger.warning("No date to roll-average with. Not doing anything")
            else:
                min_date = result[date_column].min()
                max_date = result[date_column].max()  # change your date here
                daterange = pd.date_range(min_date, max_date).rename(date_column)

                result[date_column] = result[date_column].dt.floor('D')  # Should have been done already
                result = result[~pd.isna(result[date_column])]  # Can happen for ongoing + arrival_date
                result = result \
                    .groupby([x for x in result.columns if x not in [date_column, 'ship_dwt', 'count']],
                             dropna=False) \
                    .apply(lambda x: x.set_index(date_column) \
                           .resample("D").sum() \
                           .reindex(daterange) \
                           .fillna(0) \
                           .rolling(rolling_days, min_periods=rolling_days) \
                           .mean()) \
                    .reset_index() \
                    .replace({np.nan: None})

        return result
