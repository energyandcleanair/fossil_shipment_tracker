import datetime as dt
import pandas as pd
import geopandas as gpd
import json
import numpy as np

from . import routes_api
from flask_restx import inputs


from base.models import Shipment, Ship, Arrival, Departure, Port, Berth,\
    ShipmentDepartureBerth, ShipmentArrivalBerth, Position, Trajectory, Destination, Price, Country
from base.db import session
from base.encoder import JsonEncoder
from base.utils import to_list
from base.logger import logger


from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse
import sqlalchemy as sa
from sqlalchemy.orm import aliased
from sqlalchemy import or_
from sqlalchemy import func
from base.utils import update_geometry_from_wkb
import country_converter as coco
import base


@routes_api.route('/v0/voyage', strict_slashes=False)
class VoyageResource(Resource):

    parser = reqparse.RequestParser()

    # Query content
    parser.add_argument('id', help='id(s) of voyage. Default: returns all of them',
                        default=None, action='split', required=False)
    parser.add_argument('commodity', help='commodity(ies) of interest. Default: returns all of them',
                        default=None, action='split', required=False)
    parser.add_argument('status', help='status of shipments. Could be any or several of completed, ongoing, undetected_arrival. Default: returns all of them',
                        default=None, action='split', required=False)
    parser.add_argument('date_from', help='start date for departure or arrival (format 2020-01-15)',
                        default="2022-01-01", required=False)
    parser.add_argument('date_to', type=str, help='end date for departure or arrival arrival (format 2020-01-15)', required=False,
                        default=dt.datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument('departure_iso2', action='split', help='iso2(s) of departure (only RU should be available)',
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
                        help='which variables to aggregate by. Could be any of commodity, status, departure_date, arrival_date, departure_port, departure_country,'
                             'destination_port, destination_country, destination_region')
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
        params = VoyageResource.parser.parse_args()
        return self.get_from_params(params)

    def get_from_params(self, params):
        id = params.get("id")
        commodity = params.get("commodity")
        status = params.get("status")
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

        DeparturePort = aliased(Port)
        ArrivalPort = aliased(Port)

        DepartureBerth= aliased(Berth)
        ArrivalBerth = aliased(Berth)

        DestinationPort = aliased(Port)
        DestinationCountry = aliased(Country)

        if '' in aggregate_by:
            aggregate_by.remove('')


        # Commodity
        from sqlalchemy import case
        commodity_field = case(
            [
                (sa.and_(Ship.commodity.in_([base.BULK, base.GENERAL_CARGO]),
                        DepartureBerth.commodity.ilike('%coal%')), 'coal'),
                (sa.and_(Ship.commodity.in_([base.BULK, base.GENERAL_CARGO]),
                         ArrivalBerth.commodity.ilike('%coal%')), 'coal'),
                (Ship.commodity.ilike('%bulk%'), 'bulk_not_coal')
            ],
            else_ = Ship.commodity
        ).label('commodity')

        value_eur_field = (
            Ship.dwt * Price.eur_per_tonne
        ).label('value_eur')

        # Technically, we could pivot long -> wide
        # but since we know there's a single ship per shipment
        # a rename will be faster
        value_tonne_field = case(
            [
                (Ship.unit == 'tonne', Ship.quantity),
            ],
            else_=Ship.dwt
        ).label('value_tonne')

        value_m3_field = case(
            [
                (Ship.unit == 'm3', Ship.quantity)
            ],
            else_=sa.null()
        ).label('value_m3')

        destination_iso2_field = func.coalesce(ArrivalPort.iso2, Destination.iso2, DestinationPort.iso2) \
                                     .label('destination_iso2')

        # Query with joined information
        shipments_rich = (session.query(Shipment.id,
                                    Shipment.status,
                                    Departure.date_utc.label("departure_date_utc"),
                                    DeparturePort.unlocode.label("departure_unlocode"),
                                    DeparturePort.iso2.label("departure_iso2"),
                                    DeparturePort.name.label("departure_port_name"),
                                    Arrival.date_utc.label("arrival_date_utc"),
                                    ArrivalPort.unlocode.label("arrival_unlocode"),
                                    ArrivalPort.iso2.label("arrival_iso2"),
                                    ArrivalPort.name.label("arrival_port_name"),
                                    Destination.name.label("destination_name"),
                                    destination_iso2_field,
                                    DestinationCountry.region.label("destination_region"),
                                    Ship.imo.label("ship_imo"),
                                    Ship.mmsi.label("ship_mmsi"),
                                    Ship.type.label("ship_type"),
                                    Ship.subtype.label("ship_subtype"),
                                    Ship.dwt.label("ship_dwt"),
                                    commodity_field,
                                    value_tonne_field,
                                    value_m3_field,
                                    value_eur_field,
                                    DepartureBerth.id.label("departure_berth_id"),
                                    DepartureBerth.name.label("departure_berth_name"),
                                    DepartureBerth.commodity.label("departure_berth_commodity"),
                                    DepartureBerth.port_unlocode.label("departure_berth_unlocode"),
                                    ArrivalBerth.id.label("arrival_berth_id"),
                                    ArrivalBerth.name.label("arrival_berth_name"),
                                    ArrivalBerth.commodity.label("arrival_berth_commodity"),
                                    ArrivalBerth.port_unlocode.label("arrival_berth_unlocode"))
             .join(Departure, Shipment.departure_id == Departure.id)
             .join(DeparturePort, Departure.port_id == DeparturePort.id)
             .outerjoin(Arrival, Shipment.arrival_id == Arrival.id)
             .outerjoin(ArrivalPort, Arrival.port_id == ArrivalPort.id)
             .join(Ship, Departure.ship_imo == Ship.imo)
             .outerjoin(ShipmentDepartureBerth, Shipment.id == ShipmentDepartureBerth.shipment_id)
             .outerjoin(ShipmentArrivalBerth, Shipment.id == ShipmentArrivalBerth.shipment_id)
             .outerjoin(DepartureBerth, DepartureBerth.id == ShipmentDepartureBerth.berth_id)
             .outerjoin(ArrivalBerth, ArrivalBerth.id == ShipmentArrivalBerth.berth_id)
             .outerjoin(Destination, Shipment.last_destination_name == Destination.name)
             .outerjoin(DestinationPort, Destination.port_id == DestinationPort.id)
             .outerjoin(Price,
                        sa.and_(Price.date == func.date_trunc('day', Departure.date_utc),
                                Price.commodity == commodity_field,
                                sa.or_(
                                    sa.and_(Price.country_iso2 == sa.null(), destination_iso2_field == sa.null()),
                                    Price.country_iso2 == destination_iso2_field)
                                )
                        )
             .outerjoin(DestinationCountry, DestinationCountry.iso2 == destination_iso2_field)
             .filter(destination_iso2_field != "RU"))

        if id is not None:
            shipments_rich = shipments_rich.filter(Shipment.id.in_(id))

        if commodity is not None:
            shipments_rich = shipments_rich.filter(commodity_field.in_(to_list(commodity)))

        if status is not None:
            shipments_rich = shipments_rich.filter(Shipment.status.in_(status))

        if date_from is not None:
            shipments_rich = shipments_rich.filter(
                sa.or_(
                    # Arrival.date_utc >= dt.datetime.strptime(date_from, "%Y-%m-%d"),
                    Departure.date_utc >= dt.datetime.strptime(date_from, "%Y-%m-%d")
                ))

        if date_to is not None:
            shipments_rich = shipments_rich.filter(
                sa.or_(
                    # Arrival.date_utc <= dt.datetime.strptime(date_to, "%Y-%m-%d"),
                    Departure.date_utc <= dt.datetime.strptime(date_to, "%Y-%m-%d")
                ))

        if departure_iso2 is not None:
            shipments_rich = shipments_rich.filter(DeparturePort.iso2.in_(to_list(departure_iso2)))

        if destination_iso2 is not None:
            shipments_rich = shipments_rich.filter(destination_iso2_field.in_(to_list(destination_iso2)))

        if destination_region is not None:
            shipments_rich = shipments_rich.filter(DestinationCountry.region.in_(to_list(destination_region)))

        # Aggregate
        query = self.aggregate(query=shipments_rich, aggregate_by=aggregate_by)

        # Query
        result = pd.read_sql(query.statement, session.bind)

        if len(result) == 0:
            return Response(
                status=HTTPStatus.NO_CONTENT,
                response="empty",
                mimetype='application/json')

        # Some modifications aorund countries, commodities etc.
        if "departure_iso2" in result.columns:
            result = self.fill_country(result, iso2_column="departure_iso2", country_column='departure_country')

        if "destination_iso2" in result.columns:
            result = self.fill_country(result, iso2_column="destination_iso2", country_column='destination_country')

        # Rolling average
        result = self.roll_average(result = result, aggregate_by=aggregate_by, rolling_days=rolling_days)
        response = self.build_response(result=result, format=format, nest_in_data=nest_in_data,
                                       aggregate_by=aggregate_by, download=download)
        return response


    def fill_country(self, result, iso2_column, country_column):

        cc = coco.CountryConverter()

        def country_convert(x):
            return cc.convert(names=x.iloc[0], to='name_short', not_found=None)

        result[country_column] = result[[iso2_column]] \
            .fillna("NULL_COUNTRY_PLACEHOLDER") \
            .groupby(iso2_column)[iso2_column] \
            .transform(country_convert)

        result.replace({'NULL_COUNTRY_PLACEHOLDER': None}, inplace=True)
        return result


    def aggregate(self, query, aggregate_by):
        """Perform aggregation based on user agparameters"""

        if not aggregate_by:
            return query

        subquery = query.subquery()

        # Aggregate
        value_cols = [
            func.sum(subquery.c.ship_dwt).label("ship_dwt"),
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
            'commodity': [subquery.c.commodity],
            'status': [subquery.c.status],

            'departure_date': [func.date_trunc('day', subquery.c.departure_date_utc).label("departure_date")],
            'arrival_date': [func.date_trunc('day', subquery.c.arrival_date_utc).label('arrival_date')],

            'departure_port': [subquery.c.departure_port_name, subquery.c.departure_unlocode,
                               subquery.c.departure_iso2],
            'departure_country': [subquery.c.departure_iso2],
            'departure_iso2': [subquery.c.departure_iso2],

            'destination_port': [subquery.c.arrival_port_name, subquery.c.arrival_unlocode,
                                 subquery.c.destination_iso2],
            'destination_country': [subquery.c.destination_iso2],
            'destination_iso2': [subquery.c.destination_iso2],
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
            date_column = None
            if aggregate_by is not None and "departure_date" in aggregate_by:
                date_column = "departure_date"
            if aggregate_by is not None and "arrival_date" in aggregate_by:
                date_column = "arrival_date"
            if date_column is None:
                logger.warning("No date to roll-average with. Not doing anything")
            else:
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
                             "attachment; filename=shipments.csv"})

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps({"data": result.to_dict(orient="records")}, cls=JsonEncoder)
            else:
                resp_content = json.dumps(result.to_dict(orient="records"), cls=JsonEncoder)

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json')

        if format == "geojson":
            if aggregate_by is not None:
                return Response(
                    response="Cannot query geojson with aggregation.",
                    status=HTTPStatus.BAD_REQUEST,
                    mimetype='application/json')

            shipment_ids = list([int(x) for x in result.id.unique()])

            trajectories = session.query(Trajectory) \
                .filter(Trajectory.shipment_id.in_(shipment_ids))

            trajectories_df = pd.read_sql(trajectories.statement, session.bind)
            trajectories_df = update_geometry_from_wkb(trajectories_df)
            result_gdf = gpd.GeoDataFrame(
                result.merge(trajectories_df[["shipment_id", "geometry"]].rename(columns={'shipment_id': 'id'})),
                geometry='geometry')
            result_geojson = result_gdf.to_json(cls=JsonEncoder)

            if nest_in_data:
                resp_content = '{"data": ' + result_geojson + '}'
            else:
                resp_content = result_geojson

            if download:
                headers = {"Content-disposition":
                               "attachment; filename=voyages.geojson"}
            else:
                headers = {}

            return Response(
                response=resp_content,
                status=200,
                mimetype='application/json',
                headers=headers)

        return Response(response="Unknown format. Should be either csv, json or geojson",
                        status=HTTPStatus.BAD_REQUEST,
                        mimetype='application/json')