import datetime as dt
import pandas as pd
import json
import geopandas as gpd
import re
import numpy as np
import sqlalchemy.sql.expression

from .. import routes_api, ns_charts
from flask_restx import inputs

from base.encoder import JsonEncoder
from base.utils import to_list, df_to_json, to_datetime
from base.logger import logger
from base import PRICING_DEFAULT


from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse
from ..entsogflow import EntsogFlowResource


@ns_charts.route("/v0/chart/eu_gas_consumption", strict_slashes=False)
class ChartEUGasConsumption(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument(
        "date_from",
        type=str,
        help="start date for counter data (format 2020-01-15)",
        default="2021-01-01",
        required=False,
    )
    parser.add_argument(
        "date_to",
        type=str,
        help="start date for counter data (format 2020-01-15)",
        default=-7,
        required=False,
    )
    parser.add_argument(
        "rolling_days",
        type=int,
        help="rolling average window (in days). Default: no rolling averaging",
        required=False,
        default=7,
    )
    parser.add_argument(
        "pivot_by_year",
        type=inputs.boolean,
        help="whether to pivot data by year or not",
        required=False,
        default=False,
    )

    parser.add_argument(
        "nest_in_data",
        help="Whether to nest the geojson content in a data key.",
        type=inputs.boolean,
        default=True,
    )
    parser.add_argument(
        "download",
        help="Whether to return results as a file or not.",
        type=inputs.boolean,
        default=False,
    )
    parser.add_argument(
        "format",
        type=str,
        help="format of returned results (json, csv, or geojson)",
        required=False,
        default="json",
    )

    @routes_api.expect(parser)
    def get(self):
        params = ChartEUGasConsumption.parser.parse_args()
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        rolling_days = params.get("rolling_days")
        nest_in_data = params.get("nest_in_data")
        pivot_by_year = params.get("pivot_by_year")
        format = params.get("format")
        download = params.get("download")

        params_entsog = {
            "format": "json",
            "download": False,
            "aggregate_by": ["type", "departure_region", "destination_region", "date"],
            "date_from": to_datetime(date_from) - dt.timedelta(days=rolling_days),
            "nest_in_data": False,
            "rolling_days": rolling_days,
            "type": [
                "distribution",
                "consumption",
                "storage",
                "crossborder",
                "production",
            ],
            "currency": "EUR",
            "pricing_scenario": [PRICING_DEFAULT],
        }

        entsog_resp = EntsogFlowResource().get_from_params(params=params_entsog)
        entsog = json.loads(entsog_resp.response[0])
        entsog_df = pd.DataFrame(entsog)

        crossborder_in = entsog_df[
            (entsog_df.type == "crossborder")
            & (entsog_df.destination_region == "EU")
            & (entsog_df.departure_region != "EU")
        ]
        crossborder_in["type"] = "crossborder_in"

        crossborder_out = entsog_df[
            (entsog_df.type == "crossborder")
            & (entsog_df.destination_region != "EU")
            & (entsog_df.departure_region == "EU")
        ]
        crossborder_out["type"] = "crossborder_out"

        others = entsog_df[
            (entsog_df.type != "crossborder") & (entsog_df.destination_region == "EU")
        ]

        merged = (
            pd.concat(
                [
                    crossborder_in[["type", "date", "value_m3"]],
                    crossborder_out[["type", "date", "value_m3"]],
                    others[["type", "date", "value_m3"]],
                ],
                axis=0,
                ignore_index=True,
            )
            .groupby(["type", "date"])
            .agg({"value_m3": np.nansum})
            .reset_index()
        )

        wide = pd.pivot(
            merged, index="date", columns="type", values="value_m3"
        ).reset_index()

        storage_drawdown = "Storage drawdown"
        imports = "Imports"
        implied_consumption = "Implied consumption"
        production = "Production"

        wide["date"] = pd.to_datetime(wide.date).dt.date
        wide[storage_drawdown] = wide.storage
        wide[production] = wide.production
        wide[imports] = wide.crossborder_in - wide.crossborder_out
        wide[implied_consumption] = (
            wide[imports] + wide.production + wide[storage_drawdown]
        )

        if date_from:
            wide = wide[wide.date >= pd.to_datetime(to_datetime(date_from))]

        if date_to:
            wide = wide[wide.date <= pd.to_datetime(to_datetime(date_to))]

        if pivot_by_year:
            wide["year"] = pd.to_datetime(wide.date).dt.year
            wide["date"] = pd.to_datetime(
                "1900-" + pd.to_datetime(wide.date).dt.strftime("%m-%d")
            )
            wide = (
                wide[
                    [
                        "date",
                        "year",
                        implied_consumption,
                        storage_drawdown,
                        production,
                        imports,
                    ]
                ]
                .melt(
                    id_vars=["date", "year"],
                    value_vars=[
                        implied_consumption,
                        storage_drawdown,
                        production,
                        imports,
                    ],
                )
                .pivot(index=["date", "type"], columns="year", values="value")
                .reset_index()
            )

        return self.build_response(
            result=wide, format=format, nest_in_data=nest_in_data
        )

    def build_response(self, result, format, nest_in_data):

        result.replace({np.nan: None}, inplace=True)

        # If bulk and departure berth is coal, replace commodity with coal
        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={
                    "Content-disposition": "attachment; filename=chart_gas_consumption.csv"
                },
            )

        if format == "json":
            if nest_in_data:
                resp_content = json.dumps(
                    {"data": result.to_dict(orient="records")}, cls=JsonEncoder
                )
            else:
                resp_content = json.dumps(
                    result.to_dict(orient="records"), cls=JsonEncoder
                )

            return Response(
                response=resp_content, status=200, mimetype="application/json"
            )

        return Response(
            response="Unknown format. Should be either csv or json",
            status=HTTPStatus.BAD_REQUEST,
            mimetype="application/json",
        )
