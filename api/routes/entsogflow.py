import datetime as dt
import pandas as pd
import geopandas as gpd
import json
import numpy as np

from . import routes_api
from flask_restx import inputs


from base.models import EntsogFlow, Price, Country, Commodity
from base.db import session
from base.encoder import JsonEncoder
from base.utils import to_list, to_datetime
from base.logger import logger


from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse
import sqlalchemy as sa
from sqlalchemy.orm import aliased
from sqlalchemy import func


@routes_api.route('/v0/entsogflow', strict_slashes=False, doc=False)
class EntsogFlowResource(Resource):

    parser = reqparse.RequestParser()

    # Query content
    parser.add_argument('id', help='id(s) of entsogflow. Default: returns all of them',
                        default=None, action='split', required=False)
    parser.add_argument('commodity', help='commodity(ies) of interest. Default: returns all of them',
                        default=None, action='split', required=False)
    parser.add_argument('date_from', help='start date (format 2020-01-15)',
                        default="2022-01-01", required=False)
    parser.add_argument('date_to', type=str, help='end date (format 2020-01-15)', required=False,
                        default=dt.datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument('departure_iso2', action='split', help='iso2(s) of departure',
                        required=False,
                        default=None)
    parser.add_argument('destination_iso2', action='split', help='iso2(s) of destination',
                        required=False,
                        default=None)
    parser.add_argument('destination_region', action='split', help='region(s) of destination e.g. EU,Turkey',
                        required=False,
                        default=None)
    # Query processing
    parser.add_argument('aggregate_by', type=str, action='split',
                        default=None,
                        help='which variables to aggregate by. Could be any of commodity, destination_country, destination_region')
    parser.add_argument('rolling_days', type=int, help='rolling average window (in days). Default: no rolling averaging',
                        required=False, default=None)


    # Query format
    parser.add_argument('format', type=str, help='format of returned results (json, geojson or csv)',
                        required=False, default="json")
    parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
                        type=inputs.boolean, default=True)
    parser.add_argument('download', help='Whether to return results as a file or not.',
                        type=inputs.boolean, default=False)

    @routes_api.expect(parser)
    def get(self):
        params = EntsogFlowResource.parser.parse_args()
        return self.get_from_params(params)

    def get_from_params(self, params):
        id = params.get("id")
        commodity = params.get("commodity")
        date_from = params.get("date_from")
        departure_iso2 = params.get("departure_iso2")
        destination_iso2 = params.get("destination_iso2")
        destination_region = params.get("destination_region")
        date_to = params.get("date_to")
        aggregate_by = params.get("aggregate_by")
        format = params.get("format", "json")
        nest_in_data = params.get("nest_in_data")
        download = params.get("download")
        rolling_days = params.get("rolling_days")

        if aggregate_by and '' in aggregate_by:
            aggregate_by.remove('')

        # Price for all countries without country-specific price
        default_price = session.query(Price).filter(Price.country_iso2 == sa.null()).subquery()

        value_eur_field = (
            EntsogFlow.value_tonne * func.coalesce(Price.eur_per_tonne, default_price.c.eur_per_tonne)
        ).label('value_eur')


        DepartureCountry = aliased(Country)


        # Gas transits through Ukraine for EU
        from sqlalchemy import case
        destination_country = session.query(Country.iso2,
                                            Country.name,
                                            case([(Country.iso2 == "UA", 'EU28')],
                                                 else_=Country.region).label('region')) \
            .subquery()



        # Query with joined information
        flows_rich = (session.query(EntsogFlow.id,
                                    EntsogFlow.commodity,
                                    Commodity.group.label('commodity_group'),
                                    EntsogFlow.date,
                                    EntsogFlow.departure_iso2,
                                    DepartureCountry.name.label('departure_country'),
                                    DepartureCountry.region.label('departure_region'),
                                    EntsogFlow.destination_iso2,
                                    destination_country.c.name.label("destination_country"),
                                    destination_country.c.region.label("destination_region"),
                                    EntsogFlow.value_tonne,
                                    EntsogFlow.value_m3,
                                    value_eur_field)
             .join(DepartureCountry, DepartureCountry.iso2 == EntsogFlow.departure_iso2)
             .outerjoin(destination_country, EntsogFlow.destination_iso2 == destination_country.c.iso2)
             .outerjoin(Commodity, EntsogFlow.commodity == Commodity.id)
             .outerjoin(Price,
                        sa.and_(Price.date == EntsogFlow.date,
                                Price.commodity == Commodity.pricing_commodity,
                                sa.or_(
                                    sa.and_(Price.country_iso2 == sa.null(), EntsogFlow.destination_iso2 == sa.null()),
                                    Price.country_iso2 == EntsogFlow.destination_iso2)
                                )
                        )
             .outerjoin(default_price,
                         sa.and_(default_price.c.date == EntsogFlow.date,
                                 default_price.c.commodity == Commodity.pricing_commodity
                                 )
                        )
             .filter(EntsogFlow.destination_iso2 != "RU"))


        # Return only >0 values. Otherwise we hit response size limit
        flows_rich = flows_rich.filter(EntsogFlow.value_tonne > 0)

        if id is not None:
            flows_rich = flows_rich.filter(EntsogFlow.id.in_(to_list(id)))

        if commodity is not None:
            flows_rich = flows_rich.filter(EntsogFlow.commodity.in_(to_list(commodity)))

        if date_from is not None:
            flows_rich = flows_rich.filter(EntsogFlow.date >= to_datetime(date_from))

        if date_to is not None:
            flows_rich = flows_rich.filter(EntsogFlow.date <= to_datetime(date_to))

        if departure_iso2 is not None:
            flows_rich = flows_rich.filter(EntsogFlow.departure_iso2.in_(to_list(departure_iso2)))

        if destination_iso2 is not None:
            flows_rich = flows_rich.filter(EntsogFlow.destination_iso2.in_(to_list(destination_iso2)))

        if destination_region is not None:
            flows_rich = flows_rich.filter(destination_country.c.region.in_(to_list(destination_region)))

        # Aggregate
        query = self.aggregate(query=flows_rich, aggregate_by=aggregate_by)

        # Query
        result = pd.read_sql(query.statement, session.bind)

        if len(result) == 0:
            return Response(
                status=HTTPStatus.NO_CONTENT,
                response="empty",
                mimetype='application/json')

        # Rolling average
        result = self.roll_average(result=result, aggregate_by=aggregate_by, rolling_days=rolling_days)
        response = self.build_response(result=result, format=format, nest_in_data=nest_in_data,
                                       aggregate_by=aggregate_by, download=download)
        return response


    def aggregate(self, query, aggregate_by):
        """Perform aggregation based on user agparameters"""

        if not aggregate_by:
            return query

        subquery = query.subquery()

        # Aggregate
        value_cols = [
            func.sum(subquery.c.value_tonne).label("value_tonne"),
            func.sum(subquery.c.value_m3).label("value_m3"),
            func.sum(subquery.c.value_eur).label("value_eur")
        ]

        # Adding must have grouping columns
        must_group_by = []
        aggregate_by.extend([x for x in must_group_by if x not in aggregate_by])
        if '' in aggregate_by:
            aggregate_by.remove('')
        # Aggregating
        aggregateby_cols_dict = {
            'commodity': [subquery.c.commodity, subquery.c.commodity_group],
            'commodity_group': [subquery.c.commodity_group],
            'date': [subquery.c.date],
            'departure_country': [subquery.c.departure_iso2, subquery.c.departure_country,
                                    subquery.c.departure_region],
            'departure_iso2': [subquery.c.departure_iso2, subquery.c.departure_country,
                                  subquery.c.departure_region],
            'destination_country': [subquery.c.destination_iso2, subquery.c.destination_country, subquery.c.destination_region],
            'destination_iso2': [subquery.c.destination_iso2, subquery.c.destination_country, subquery.c.destination_region],
            'destination_region': [subquery.c.destination_region]
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
            date_column = "date"
            min_date = result[date_column].min()
            max_date = result[date_column].max() # change your date here
            daterange = pd.date_range(min_date, max_date).rename(date_column)

            result[date_column] = result[date_column].dt.floor('D')  # Should have been done already
            result = result \
                .groupby([x for x in result.columns if x not in [date_column, "ship_dwt", "value_tonne", "value_m3", "value_eur"]]) \
                .apply(lambda x: x.set_index(date_column) \
                       .resample("D").sum() \
                       .reindex(daterange) \
                       .fillna(0) \
                       .rolling(rolling_days, min_periods=rolling_days) \
                       .mean()) \
                .reset_index()

        return result


    def build_response(self, result, format, nest_in_data, aggregate_by, download):

        result.replace({np.nan: None}, inplace=True)

        # If bulk and departure berth is coal, replace commodity with coal
        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition":
                             "attachment; filename=entsogflow.csv"})

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