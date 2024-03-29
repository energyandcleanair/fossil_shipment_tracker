import json
import pandas as pd
import datetime as dt
import numpy as np
import geopandas as gpd
from shapely import wkb
from sqlalchemy.sql import text
from sqlalchemy import func
from flask import Response
from flask_restx import Resource, reqparse, inputs
from base.models import Flaring, FlaringFacility
from base.encoder import JsonEncoder
from base.db import session, engine
from base.utils import to_datetime, to_list, update_geometry_from_wkb
from base.logger import logger

from . import routes_api, ns_flaring


@ns_flaring.route("/v0/flaring_facility", strict_slashes=False)
class FlaringFacilityResource(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument(
        "facility_id",
        help="facility id(s)",
        type=str,
        action="split",
        default=None,
        required=False,
    )
    parser.add_argument(
        "with_anomaly_index",
        help="Add an anomaly index to detect anomaly after 2022-02-24",
        type=inputs.boolean,
        default=True,
    )
    parser.add_argument(
        "nest_in_data",
        help="Whether to nest the geojson content in a data key.",
        type=inputs.boolean,
        default=True,
    )
    parser.add_argument(
        "format",
        type=str,
        help="format of returned results (json or csv)",
        required=False,
        default="json",
    )
    parser.add_argument(
        "download",
        help="Whether to return results as a file or not.",
        type=inputs.boolean,
        default=False,
    )

    @ns_flaring.expect(parser)
    def get(self):
        params = FlaringFacilityResource.parser.parse_args()
        facility_id = params.get("facility_id")
        with_anomaly_index = params.get("with_anomaly_index")
        format = params.get("format")
        nest_in_data = params.get("nest_in_data")
        download = params.get("download")

        if with_anomaly_index:
            with open("assets/flaring_anomaly_index.sql", "r") as file:
                sql_content = file.read()

            with engine.connect() as con:
                s = text(sql_content)
                rs = con.execute(
                    s,
                    **{
                        "facility_id": to_list([int(x) for x in facility_id])
                        if facility_id
                        else None
                    }
                )

            result = [r for r in rs]
            result = pd.DataFrame(result)
            result.columns = [
                "id",
                "name",
                "name_en",
                "type",
                "url",
                "geometry",
                "anomaly_index",
            ]
            result["geometry"] = result.geometry.apply(lambda x: wkb.loads(bytes(x)) if x else None)
            result = result.sort_values("anomaly_index", axis=0, ascending=False)

        else:
            query = session.query(FlaringFacility)

            if facility_id is not None:
                query = query.filter(FlaringFacility.id.in_(to_list(facility_id)))

            result = pd.read_sql(query.statement, session.bind)
            result = update_geometry_from_wkb(result, to="shape")

        response = self.build_response(
            result=result, format=format, nest_in_data=nest_in_data, download=download
        )
        return response

    def build_response(self, result, format, nest_in_data, download):
        result.replace({np.nan: None}, inplace=True)
        if format == "csv":
            result.drop("geometry", axis=1, inplace=True)
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=flaringfacility.csv"},
            )

        if format == "json":
            result.drop("geometry", axis=1, inplace=True)
            if nest_in_data:
                resp_content = json.dumps(
                    {"data": result.to_dict(orient="records")}, cls=JsonEncoder
                )
            else:
                resp_content = json.dumps(result.to_dict(orient="records"), cls=JsonEncoder)

            return Response(response=resp_content, status=200, mimetype="application/json")

        if format == "geojson":
            berths_gdf = gpd.GeoDataFrame(result, geometry="geometry")
            berths_geojson = berths_gdf.to_json(cls=JsonEncoder)

            if nest_in_data:
                resp_content = '{"data": ' + berths_geojson + "}"
            else:
                resp_content = berths_geojson

            if download:
                headers = {"Content-disposition": "attachment; filename=flaringfacility.geojson"}
            else:
                headers = {}

            return Response(
                response=resp_content,
                status=200,
                mimetype="application/json",
                headers=headers,
            )


@ns_flaring.route("/v0/flaring", strict_slashes=False)
class FlaringResource(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument(
        "facility_id",
        help="facility id(s)",
        type=str,
        action="split",
        default=None,
        required=False,
    )
    parser.add_argument(
        "facility_name",
        help="name(s) of facility",
        type=str,
        action="split",
        default=None,
        required=False,
    )
    parser.add_argument(
        "unit",
        help="bcm_est,mw",
        type=str,
        action="split",
        default=["bcm_est"],
        required=False,
    )
    parser.add_argument(
        "rolling_days",
        type=int,
        help="rolling average window (in days). Default: no rolling averaging",
        required=False,
        default=None,
    )
    parser.add_argument(
        "date_from",
        help="start date for arrival (format 2020-01-15)",
        default="2018-01-01",
        required=False,
    )
    parser.add_argument(
        "date_to",
        type=str,
        help="end date for arrival (format 2020-01-15)",
        required=False,
        default=dt.datetime.today().strftime("%Y-%m-%d"),
    )
    parser.add_argument(
        "nest_in_data",
        help="Whether to nest the geojson content in a data key.",
        type=inputs.boolean,
        default=True,
    )
    parser.add_argument(
        "aggregate_by",
        type=str,
        action="split",
        default=None,
        help="which variables to aggregate by. Could be any of facility, facility_type, date",
    )

    parser.add_argument(
        "format",
        type=str,
        help="format of returned results (json or csv)",
        required=False,
        default="json",
    )
    parser.add_argument(
        "download",
        help="Whether to return results as a file or not.",
        type=inputs.boolean,
        default=False,
    )

    @routes_api.expect(parser)
    def get(self):
        params = FlaringResource.parser.parse_args()
        facility_id = params.get("facility_id")
        unit = params.get("unit")
        format = params.get("format")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        nest_in_data = params.get("nest_in_data")
        rolling_days = params.get("rolling_days")
        download = params.get("download")
        aggregate_by = params.get("aggregate_by")

        query = session.query(
            FlaringFacility.id,
            FlaringFacility.name,
            FlaringFacility.name_en,
            FlaringFacility.type,
            Flaring.date,
            Flaring.unit,
            Flaring.value,
            Flaring.buffer_km,
        ).join(Flaring, FlaringFacility.id == Flaring.facility_id)

        if facility_id is not None:
            query = query.filter(FlaringFacility.id.in_(to_list(facility_id)))

        if unit is not None:
            query = query.filter(Flaring.unit.in_(to_list(unit)))

        if date_from is not None:
            query = query.filter(Flaring.date >= to_datetime(date_from))

        if date_to is not None:
            query = query.filter(Flaring.date <= to_datetime(date_to))

        query = self.aggregate(query=query, aggregate_by=aggregate_by)
        result = pd.read_sql(query.statement, session.bind)

        if len(result) > 0:
            result = self.roll_average(result=result, rolling_days=rolling_days)

        response = self.build_response(
            result=result, format=format, nest_in_data=nest_in_data, download=download
        )
        return response

    def build_response(self, result, format, nest_in_data, download):
        result.replace({np.nan: None}, inplace=True)
        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=flaring.csv"},
            )

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps(
                    {"data": result.to_dict(orient="records")}, cls=JsonEncoder
                )
            else:
                resp_content = json.dumps(result.to_dict(orient="records"), cls=JsonEncoder)

            return Response(response=resp_content, status=200, mimetype="application/json")

        if format == "geojson":
            import geopandas as gpd

            berths_gdf = gpd.GeoDataFrame(result, geometry="geometry")
            berths_geojson = berths_gdf.to_json(cls=JsonEncoder)

            if nest_in_data:
                resp_content = '{"data": ' + berths_geojson + "}"
            else:
                resp_content = berths_geojson

            if download:
                headers = {"Content-disposition": "attachment; filename=flaring.geojson"}
            else:
                headers = {}

            return Response(
                response=resp_content,
                status=200,
                mimetype="application/json",
                headers=headers,
            )

    def roll_average(self, result, rolling_days):
        if rolling_days is not None:
            result["date"] = pd.to_datetime(result.date)
            date_column = "date"
            value_columns = ["value"]
            min_date = result[date_column].min()
            max_date = result[date_column].max()
            daterange = pd.date_range(min_date, max_date).rename(date_column)

            result[date_column] = result[date_column].dt.floor("D")  # Should have been done already
            result = (
                result.groupby(
                    [x for x in result.columns if x not in ([date_column] + value_columns)]
                )
                .apply(
                    lambda x: x.set_index(date_column)
                    .resample("D")[value_columns]
                    .sum()
                    .reindex(daterange)
                    .fillna(0)
                    .rolling(rolling_days, min_periods=rolling_days)
                    .mean()
                )
                .reset_index()
            )

        return result

    def aggregate(self, query, aggregate_by):
        """Perform aggregation based on user agparameters"""

        if not aggregate_by:
            return query

        subquery = query.subquery()
        # Aggregate
        value_cols = [func.sum(subquery.c.value).label("value")]

        # Adding must have grouping columns
        must_group_by = ["buffer_km", "unit"]

        aggregate_by.extend([x for x in must_group_by if x not in aggregate_by])
        if "" in aggregate_by:
            aggregate_by.remove("")

        # Aggregating
        aggregateby_cols_dict = {
            "facility": [
                subquery.c.id,
                subquery.c.name,
                subquery.c.name_en,
                subquery.c.type,
            ],
            "facility_type": [subquery.c.type],
            "date": [subquery.c.date],
            "unit": [subquery.c.unit],
            "buffer_km": [subquery.c.buffer_km],
        }

        if any([x not in aggregateby_cols_dict for x in aggregate_by]):
            logger.warning(
                "aggregate_by can only be a selection of %s"
                % (",".join(aggregateby_cols_dict.keys()))
            )
            aggregate_by = [x for x in aggregate_by if x in aggregateby_cols_dict]

        groupby_cols = []
        for x in aggregate_by:
            groupby_cols.extend(aggregateby_cols_dict[x])

        query = session.query(*groupby_cols, *value_cols).group_by(*groupby_cols)
        return query
