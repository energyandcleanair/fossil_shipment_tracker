import pandas as pd
import json
import numpy as np
from flask_restx import inputs
from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse

import base
from base.encoder import JsonEncoder
from base.utils import to_list, df_to_json, to_datetime

from base import COMMODITY_GROUPING_DEFAULT, COMMODITY_GROUPING_CHOICES, COMMODITY_GROUPING_HELP

from .back_compat import voyage_parser, get_voyages
from .. import routes_api, ns_charts


aggregate_column_map = {
    "zone": "end_sts_zone_name",
    "country": "end_sts_iso2",
    "region": "end_sts_region",
}

aggregate_name_map = {
    "zone": "end_sts_zone",
    "country": "end_sts_country",
    "region": "end_sts_region",
}


@ns_charts.route("/v0/chart/sts_locations", strict_slashes=False)
class ChartStsLocations(Resource):
    parser = reqparse.RequestParser()

    parser.add_argument(
        "destination_date_from",
        type=str,
        help="start date for data (format 2020-01-15)",
        default="2020-01-01",
    )

    parser.add_argument(
        "destination_date_to",
        type=str,
        help="end date for data (format 2020-01-15)",
        default=-5,
    )

    parser.add_argument(
        "limit_sts_locations_n",
        type=int,
        help="how many locations to include",
        default=8,
    )

    parser.add_argument(
        "limit_sts_locations_by",
        type=str,
        default="value_tonne",
        help="how to limit locations",
        choices=["value_eur", "value_tonne", "trade_count"],
    )

    parser.add_argument(
        "aggregate_level",
        type=str,
        default="zone",
        choices=["zone", "country", "region"],
    )

    parser.add_argument(
        "commodity_origin_iso2",
        type=str,
        help="Country of origin",
        default="RU",
    )

    parser.add_argument(
        "commodity",
        type=str,
        action="split",
        help="Commodity to filter by",
    )

    parser.add_argument(
        "format",
        type=str,
        default="csv",
        choices=["csv", "json"],
    )

    parser.add_argument(
        "nest_in_data",
        type=inputs.boolean,
        default=True,
    )

    parser.add_argument(
        "rolling_days",
        type=int,
        default=30,
    )

    @routes_api.expect(parser)
    def get(self):
        params = voyage_parser.parse_args()

        params_chart = ChartStsLocations.parser.parse_args()
        format = params_chart.get("format")
        nest_in_data = params_chart.get("nest_in_data")

        aggregate_level = params_chart.get("aggregate_level")

        base_aggregate_by = ["destination_date"]

        aggregate_by = base_aggregate_by + [aggregate_name_map.get(aggregate_level)]

        aggregate_column = aggregate_column_map.get(aggregate_level)

        params.update(**params_chart)
        params.update(
            **{
                "format": "json",
                "nest_in_data": True,
                "aggregate_by": aggregate_by,
                "is_sts": True,
                "nest_ships": False,
            }
        )

        data = get_voyages(params)

        result = data

        result = result[result[aggregate_column].notnull()]

        limit_sts_locations_n = params_chart.get("limit_sts_locations_n")
        limit_sts_locations_by = params_chart.get("limit_sts_locations_by")

        result = self.limit_sts_locations(
            data=result,
            aggregate_column=aggregate_column,
            limit_sts_locations_n=limit_sts_locations_n,
            limit_sts_locations_by=limit_sts_locations_by,
        )

        result["destination_date"] = pd.to_datetime(result["destination_date"]).dt.date

        result = self.sort(data=result)

        result = result[result["value_tonne"].notnull()]
        result = result[result["value_eur"].notnull()]

        return self.build_response(result=result, format=format, nest_in_data=nest_in_data)

    def limit_sts_locations(
        self,
        *,
        data: pd.DataFrame,
        aggregate_column: str,
        limit_sts_locations_n: int,
        limit_sts_locations_by: str
    ):

        locations_by_aggregate = data.groupby(aggregate_column)[limit_sts_locations_by].sum()

        location_names_in_order = (
            locations_by_aggregate.sort_values(ascending=False)
            .head(limit_sts_locations_n)
            .reset_index()[aggregate_column]
            .tolist()
        )

        data_only_locations_in_order = data[data[aggregate_column].isin(location_names_in_order)]

        data_only_locations_in_order["sts_sort_order"] = data_only_locations_in_order[
            aggregate_column
        ].apply(lambda x: location_names_in_order.index(x) if x in location_names_in_order else 999)

        return data_only_locations_in_order

    def sort(self, *, data):

        data.sort_values(by=["destination_date", "sts_sort_order"], inplace=True)

        return data

    def build_response(self, result, format, nest_in_data):
        result.replace({np.nan: None}, inplace=True)

        # If bulk and departure berth is coal, replace commodity with coal
        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=product_on_water.csv"},
            )

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps(
                    {"data": result.to_dict(orient="records")}, cls=JsonEncoder
                )
            else:
                resp_content = json.dumps(result.to_dict(orient="records"), cls=JsonEncoder)

            return Response(response=resp_content, status=200, mimetype="application/json")

        return Response(
            response="Unknown format. Should be either csv or json",
            status=HTTPStatus.BAD_REQUEST,
            mimetype="application/json",
        )
