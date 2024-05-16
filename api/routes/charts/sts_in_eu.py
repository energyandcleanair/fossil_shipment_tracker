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

from .voyage_data_proxy import get_voyages
from .. import routes_api, ns_charts
from ..voyage import VoyageResource


@ns_charts.route("/v0/chart/sts_in_eu", strict_slashes=False)
class ChartStsInEu(Resource):
    parser = reqparse.RequestParser()

    parser.add_argument(
        "destination_date_from",
        type=str,
        help="start date for counter data (format 2020-01-15)",
        default="2021-12-01",
        required=False,
    )

    parser.add_argument(
        "destination_date_to",
        type=str,
        help="start date for counter data (format 2020-01-15)",
        default=-1,
        required=False,
    )

    parser.add_argument(
        "commodity_grouping",
        type=str,
        help=COMMODITY_GROUPING_HELP,
        default=COMMODITY_GROUPING_DEFAULT,
        choices=COMMODITY_GROUPING_CHOICES,
    )

    parser.add_argument(
        "commodity",
        help="Commodity(ies) of interest",
        action="split",
        required=False,
        default=["oil_products", "crude_oil"],
    )

    parser.add_argument(
        "aggregate_by",
        type=str,
        action="split",
        default=["destination_date", "ownership_sanction_coverage"],
        help="which variables to aggregate by. Could be any of commodity, type, destination_region, date",
    )

    parser.add_argument(
        "rolling_days",
        type=int,
        help="rolling average window (in days). Default: no rolling averaging",
        required=False,
        default=None,
    )

    parser.add_argument(
        "pivot_value",
        type=str,
        help="pivoted value. Default: value_eur.",
        required=False,
        default="value_eur",
    )

    parser.add_argument("language", type=str, help="en or ua", default="en", required=False)

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

    parser.add_argument(
        "use_kpler",
        help="Whether to use Kpler or MT",
        type=inputs.boolean,
        default=base.CHARTS_USE_KPLER_DEFAULT,
    )

    @routes_api.expect(parser)
    def get(self):
        params = VoyageResource.parser.parse_args()
        params_chart = ChartStsInEu.parser.parse_args()
        format = params_chart.get("format")
        language = params_chart.get("language")
        nest_in_data = params_chart.get("nest_in_data")
        use_kpler = params_chart.get("use_kpler")

        params.update(**params_chart)
        params.update(
            **{
                "use_eu": True,
                "commodity_origin_iso2": "RU",
                "pivot_by": "ownership_sanction_coverage",
                "pricing_scenario": [base.PRICING_DEFAULT],
                "currency": "EUR",
                "keep_zeros": True,
                "format": "json",
                "nest_in_data": True,
            }
        )

        def translate(data, language):
            if language != "en":
                file_path = "assets/language/%s.json" % (language)
                with open(file_path, "r") as file:
                    translate_dict = json.load(file)

                data = data.replace(translate_dict)
                data.columns = [translate_dict.get(x, x) for x in data.columns]

            return data

        data = get_voyages(params, use_kpler=use_kpler)

        result = data

        result["date"] = pd.to_datetime(result["destination_date"]).dt.date

        result = self.sort(data=result)

        result = translate(data=result, language=language)

        return self.build_response(result=result, format=format, nest_in_data=nest_in_data)

    def sort(self, data):

        data.sort_values(by=["date"], inplace=True)

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
