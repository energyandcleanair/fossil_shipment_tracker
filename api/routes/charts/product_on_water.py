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


@ns_charts.route("/v0/chart/product_on_water", strict_slashes=False)
class ChartProductOnWater(Resource):
    parser = reqparse.RequestParser()

    parser.add_argument(
        "date_from",
        type=str,
        help="start date for counter data (format 2020-01-15)",
        default="2021-09-01",
        required=False,
    )

    parser.add_argument(
        "date_to",
        type=str,
        help="start date for counter data (format 2020-01-15)",
        default=None,
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
        default=None,
    )

    parser.add_argument(
        "aggregate_by",
        type=str,
        action="split",
        default=[
            "commodity_destination_region",
            "commodity_destination_country",
            "commodity",
            "arrival_detected_date",
            "departure_date",
            "status",
        ],
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
        help="pivoted value. Default: value_tonne.",
        required=False,
        default="value_tonne",
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

    recode_commodity = {
        "coal": "Coal",
        "coal_rail_road": "Coal",
        "coke_rail_road": "Coal",
        "lng": "LNG",
        "lng_pipeline": "LNG",
        "crude_oil": "Crude oil",
        "crude_oil_rail_road": "Crude oil",
        "natural_gas": "Pipeline gas",
        "lng_rail_road": "LNG",
        "oil_products": "Oil products and chemicals",
        "oil_products_pipeline": "Oil products and chemicals",
        "oil_products_rail_road": "Oil products and chemicals",
        "oil_or_chemical": "Oil products and chemicals",
        "pipeline_oil": "Crude oil",
        "lpg": "Oil products and chemicals",
        "bulk_not_coal": "Others",
        "general_cargo": "Others",
        "oil_or_ore": "Others",
    }

    order_commodity = [
        "Crude oil",
        "Oil products and chemicals",
        "LNG",
        "Coal",
        "Pipeline gas",
        "Others",
    ]

    @routes_api.expect(parser)
    def get(self):
        params = VoyageResource.parser.parse_args()
        params_chart = ChartProductOnWater.parser.parse_args()
        format = params_chart.get("format")
        language = params_chart.get("language")
        nest_in_data = params_chart.get("nest_in_data")
        pivot_value = params_chart.get("pivot_value")
        use_kpler = params_chart.get("use_kpler")

        params.update(**params_chart)
        params.update(
            **{
                "use_eu": True,
                "commodity_origin_iso2": "RU",
                "pricing_scenario": [base.PRICING_DEFAULT],
                "currency": "EUR",
                "keep_zeros": True,
                "format": "json",
                "nest_in_data": True,
                "status": ["completed", "ongoing"],
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

        data["arrival_detected_date"] = pd.to_datetime(data["arrival_detected_date"])
        data["arrival_detected_date"].fillna(
            pd.to_datetime("now") + pd.Timedelta(days=7), inplace=True
        )

        data["departure_date"] = pd.to_datetime(data["departure_date"])

        data.replace({"commodity": self.recode_commodity}, inplace=True)
        data["commodity"].fillna("Others", inplace=True)

        data = data[
            (data["commodity"] != "unknown")
            & (data["commodity"] != "Others")
            & (data["commodity"].notnull())
            & ~(
                (data["commodity"] == "Coal")
                & (data["commodity_destination_region"] == "EU")
                & (data["arrival_detected_date"] > pd.to_datetime("2022-08-11"))
                & (data["status"] == "completed")
            )
        ]

        #
        data["commodity_destination_region"] = np.where(
            data["commodity_destination_country"].isin(
                ["United Kingdom", "United States", "South Korea"]
            ),
            "Others",
            data["commodity_destination_region"],
        )

        # Fix coal
        data["commodity_destination_region"] = np.where(
            (data["status"] == "ongoing")
            & (data["commodity"] == "Coal")
            & (data["commodity_destination_region"] == "EU"),
            "Unknown",
            data["commodity_destination_region"],
        )

        # Also remove shipments to EU since 2022-12-05 until we can verify these are correct/breaking sanctions
        # Any ongoing shipments do not show as to EU - this can look misleading so set them as unknown
        data["commodity_destination_region"] = np.where(
            (
                (data["status"] == "ongoing")
                & (data["commodity"] == "Crude oil")
                & (data["commodity_destination_region"] == "EU")
            )
            | (
                (data["commodity_destination_region"] == "EU")
                & (data["commodity"] == "Crude oil")
                & (data["departure_date"] > "2022-12-05")
            ),
            "Unknown",
            data["commodity_destination_region"],
        )

        date_range = pd.date_range("2022-01-01", data["departure_date"].max(), freq="D")
        result = []

        for d in date_range:
            _data = (
                data[(data["departure_date"] <= d) & (data["arrival_detected_date"] >= d)]
                .groupby(["commodity", "commodity_destination_region"])
                .agg({pivot_value: "sum"})
                .reset_index()
            )
            _data["date"] = d
            result.append(_data)

        result = pd.concat(result)

        def pivot_data(data, variable):
            # Add the variable for transparency sake
            data["variable"] = variable
            result = (
                data.groupby(
                    ["commodity_destination_region", "date", "commodity", "variable"],
                    dropna=False,
                )[variable]
                .sum()
                .reset_index()
                .pivot_table(
                    index=["commodity", "date", "variable"],
                    columns=["commodity_destination_region"],
                    values=variable,
                    sort=False,
                    fill_value=0,
                )
                .reset_index()
            )
            return result

        result["date"] = pd.to_datetime(result["date"]).dt.date

        result = pivot_data(result, variable=pivot_value)

        result = self.sort(data=result)

        result = translate(data=result, language=language)

        return self.build_response(result=result, format=format, nest_in_data=nest_in_data)

    def sort(self, data):
        """Sort the data by date and commodity"""
        if "commodity" in data.columns:
            # Create category variable
            data["commodity"] = pd.Categorical(
                data["commodity"],
                categories=self.order_commodity,
                ordered=True,
            )
            data.sort_values(by=["commodity", "date"], inplace=True)

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
