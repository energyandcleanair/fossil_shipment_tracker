# import datetime as dt
# import pandas as pd
# import geopandas as gpd
# import json
# import numpy as np
#
# from . import routes_api
# from flask_restx import inputs
#
#
# from base.models import Shipment, Ship, Arrival, Departure, Port, Berth,\
#     ShipmentDepartureBerth, ShipmentArrivalBerth, PipelineFlow, Trajectory, Destination, Price
# from base.db import session
# from base.encoder import JsonEncoder
# from base.utils import to_list
# from base.logger import logger
#
#
# from http import HTTPStatus
# from flask import Response
# from flask_restx import Resource, reqparse
# import sqlalchemy as sa
# from sqlalchemy.orm import aliased
# from sqlalchemy import or_
# from sqlalchemy import func
# from base.utils import update_geometry_from_wkb
# import country_converter as coco
#
#
#
# @routes_api.route('/v0/pipelineflow', strict_slashes=False)
# class PipelineFlowResource(Resource):
#
#     parser = reqparse.RequestParser()
#
#     # Query content
#     parser.add_argument('commodity', help='commodity(ies) of interest. Default: returns all of them',
#                         default=None, action='split', required=False)
#     parser.add_argument('date_from', help='start date (format 2020-01-15)',
#                         default="2022-01-01", required=False)
#     parser.add_argument('date_to', type=str, help='end date (format 2020-01-15)', required=False,
#                         default=dt.datetime.today().strftime("%Y-%m-%d"))
#     parser.add_argument('departure_iso2', action='split', help='iso2(s) of departure (only RU should be available)',
#                         required=False,
#                         default=None)
#     parser.add_argument('destination_iso2', action='split', help='iso2(s) of destination',
#                         required=False,
#                         default=None)
#     # Query processing
#     parser.add_argument('aggregate_by', type=str, action='split',
#                         default=None,
#                         help='which variables to aggregate by. Could be any of commodity, date, departure_country, destination_country')
#     parser.add_argument('rolling_days', type=int, help='rolling average window (in days). Default: no rolling averaging',
#                         required=False, default=None)
#
#
#     # Query format
#     parser.add_argument('format', type=str, help='format of returned results (json, geojson or csv)',
#                         required=False, default="json")
#     parser.add_argument('nest_in_data', help='Whether to nest the geojson content in a data key.',
#                         type=inputs.boolean, default=True)
#     parser.add_argument('download', help='Whether to return results as a file or not.',
#                         type=inputs.boolean, default=False)
#
#     @routes_api.expect(parser)
#     def get(self):
#
#         params = PipelineFlowResource.parser.parse_args()
#         date_from = params.get("date_from")
#         date_to = params.get("date_to")
#         departure_iso2 = params.get("departure_iso2")
#         destination_iso2 = params.get("destination_iso2")
#
#         aggregate_by = params.get("aggregate_by")
#         rolling_days = params.get("rolling_days")
#
#         format = params.get("format")
#         nest_in_data = params.get("nest_in_data")
#         download = params.get("download")
#
#
#         # Query with joined information
#         pipelineflows = session.query(PipelineFlow.date,
#                                         PipelineFlow.departure_iso2,
#                                         PipelineFlow.destination_iso2,
#                                         PipelineFlow.commodity,
#                                         PipelineFlow.value_tonne,
#                                         PipelineFlow.value_mwh,
#                                         (PipelineFlow.value_tonne * Price.eur_per_tonne).label('value_eur')
#                                         ) \
#              .outerjoin(Price, sa.and_(Price.date == PipelineFlow.date,
#                                        Price.commodity == PipelineFlow.commodity))
#
#
#         if date_from is not None:
#             pipelineflows = pipelineflows.filter(PipelineFlow.date >= dt.datetime.strptime(date_from, "%Y-%m-%d"))
#
#         if date_to is not None:
#             pipelineflows = pipelineflows.filter(PipelineFlow.date <= dt.datetime.strptime(date_to, "%Y-%m-%d"))
#
#         if departure_iso2 is not None:
#             pipelineflows = pipelineflows.filter(PipelineFlow.date.departure_iso2.in_(to_list(departure_iso2)))
#
#         if destination_iso2 is not None:
#             pipelineflows = pipelineflows.filter(PipelineFlow.destination_iso2.in_(to_list(destination_iso2)))
#
#         # Aggregate
#         query = self.aggregate(query=pipelineflows, aggregate_by=aggregate_by)
#
#         # Query
#         result = pd.read_sql(query.statement, session.bind)
#
#         if len(result) == 0:
#             return Response(
#                 status=HTTPStatus.NO_CONTENT,
#                 response="empty",
#                 mimetype='application/json')
#
#         # Some modifications aorund countries, commodities etc.
#         if "departure_iso2" in result.columns:
#             result = self.fill_country(result, iso2_column="departure_iso2", country_column='departure_country')
#
#         if "destination_iso2" in result.columns:
#             result = self.fill_country(result, iso2_column="destination_iso2", country_column='destination_country')
#
#         # Rolling average
#         result = self.roll_average(result = result, aggregate_by=aggregate_by, rolling_days=rolling_days)
#         response = self.build_response(result=result, format=format, nest_in_data=nest_in_data,
#                                        aggregate_by=aggregate_by, download=download)
#         return response
#
#
#     def fill_country(self, result, iso2_column, country_column):
#
#         cc = coco.CountryConverter()
#
#         def country_convert(x):
#             return cc.convert(names=x.iloc[0], to='name_short', not_found=None)
#
#         result[country_column] = result[[iso2_column]] \
#             .fillna("NULL_COUNTRY_PLACEHOLDER") \
#             .groupby(iso2_column)[iso2_column] \
#             .transform(country_convert)
#
#         result.replace({'NULL_COUNTRY_PLACEHOLDER': None}, inplace=True)
#         return result
#
#
#     def aggregate(self, query, aggregate_by):
#         """Perform aggregation based on user agparameters"""
#
#         if aggregate_by is None:
#             return query
#
#         subquery = query.subquery()
#
#         # Aggregate
#         value_cols = [
#             func.sum(subquery.c.ship_dwt).label("ship_dwt"),
#             func.sum(subquery.c.value_tonne).label("value_tonne"),
#             func.sum(subquery.c.value_m3).label("value_m3"),
#             func.sum(subquery.c.value_eur).label("value_eur")
#         ]
#
#         # Adding must have grouping columns
#         must_group_by = []
#         aggregate_by.extend([x for x in must_group_by if x not in aggregate_by])
#         if '' in aggregate_by:
#             aggregate_by.remove('')
#         # Aggregating
#         aggregateby_cols_dict = {
#             'commodity': [subquery.c.commodity],
#             'status': [subquery.c.status],
#
#             'departure_date': [func.date_trunc('day', subquery.c.departure_date_utc).label("departure_date")],
#             'arrival_date': [func.date_trunc('day', subquery.c.arrival_date_utc).label('arrival_date')],
#
#             'departure_port': [subquery.c.departure_port_name, subquery.c.departure_unlocode,
#                                subquery.c.departure_iso2],
#             'departure_country': [subquery.c.departure_iso2],
#             'departure_iso2': [subquery.c.departure_iso2],
#
#             'destination_port': [subquery.c.arrival_port_name, subquery.c.arrival_unlocode,
#                                  subquery.c.destination_iso2],
#             'destination_country': [subquery.c.destination_iso2],
#             'destination_iso2': [subquery.c.destination_iso2]
#         }
#
#         if any([x not in aggregateby_cols_dict for x in aggregate_by]):
#             logger.warning("aggregate_by can only be a selection of %s" % (",".join(aggregateby_cols_dict.keys())))
#             aggregate_by = [x for x in aggregate_by if x in aggregateby_cols_dict]
#
#         groupby_cols = []
#         for x in aggregate_by:
#             groupby_cols.extend(aggregateby_cols_dict[x])
#
#         query = session.query(*groupby_cols, *value_cols).group_by(*groupby_cols)
#         return query
#
#
#     def roll_average(self, result, aggregate_by, rolling_days):
#
#         if rolling_days is not None:
#             date_column = None
#             if aggregate_by is not None and "departure_date" in aggregate_by:
#                 date_column = "departure_date"
#             if aggregate_by is not None and "arrival_date" in aggregate_by:
#                 date_column = "arrival_date"
#             if date_column is None:
#                 logger.warning("No date to roll-average with. Not doing anything")
#             else:
#                 min_date = result[date_column].min()
#                 max_date = result[date_column].max() # change your date here
#                 daterange = pd.date_range(min_date, max_date).rename(date_column)
#
#                 result[date_column] = result[date_column].dt.floor('D')  # Should have been done already
#                 result = result \
#                     .groupby([x for x in result.columns if x not in [date_column, "ship_dwt", "value_tonne", "value_m3", "value_eur"]]) \
#                     .apply(lambda x: x.set_index(date_column) \
#                            .resample("D").sum() \
#                            .reindex(daterange) \
#                            .fillna(0) \
#                            .rolling(rolling_days, min_periods=rolling_days) \
#                            .mean()) \
#                     .reset_index()
#
#         return result
#
#
#     def build_response(self, result, format, nest_in_data, aggregate_by, download):
#
#         result.replace({np.nan: None}, inplace=True)
#
#         # If bulk and departure berth is coal, replace commodity with coal
#         if format == "csv":
#             return Response(
#                 response=result.to_csv(index=False),
#                 mimetype="text/csv",
#                 headers={"Content-disposition":
#                              "attachment; filename=shipments.csv"})
#
#         if format == "json":
#             if nest_in_data:
#                 resp_content = json.dumps({"data": result.to_dict(orient="records")}, cls=JsonEncoder)
#             else:
#                 resp_content = json.dumps(result.to_dict(orient="records"), cls=JsonEncoder)
#
#             return Response(
#                 response=resp_content,
#                 status=200,
#                 mimetype='application/json')
#
#         if format == "geojson":
#             if aggregate_by is not None:
#                 return Response(
#                     response="Cannot query geojson with aggregation.",
#                     status=HTTPStatus.BAD_REQUEST,
#                     mimetype='application/json')
#
#             shipment_ids = list([int(x) for x in result.id.unique()])
#
#             trajectories = session.query(Trajectory) \
#                 .filter(Trajectory.shipment_id.in_(shipment_ids))
#
#             trajectories_df = pd.read_sql(trajectories.statement, session.bind)
#             trajectories_df = update_geometry_from_wkb(trajectories_df)
#             result_gdf = gpd.GeoDataFrame(
#                 result.merge(trajectories_df[["shipment_id", "geometry"]].rename(columns={'shipment_id': 'id'})),
#                 geometry='geometry')
#             result_geojson = result_gdf.to_json(cls=JsonEncoder)
#
#             if nest_in_data:
#                 resp_content = '{"data": ' + result_geojson + '}'
#             else:
#                 resp_content = result_geojson
#
#             if download:
#                 headers = {"Content-disposition":
#                                "attachment; filename=voyages.geojson"}
#             else:
#                 headers = {}
#
#             return Response(
#                 response=resp_content,
#                 status=200,
#                 mimetype='application/json',
#                 headers=headers)
#
#         return Response(response="Unknown format. Should be either csv, json or geojson",
#                         status=HTTPStatus.BAD_REQUEST,
#                         mimetype='application/json')