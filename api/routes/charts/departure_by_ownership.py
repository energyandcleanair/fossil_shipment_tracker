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
from .. import routes_api, ns_charts
from ..voyage import VoyageResource


@ns_charts.route("/v0/chart/departure_by_ownership", strict_slashes=False)
class ChartDepartureOwnership(Resource):

    parser = reqparse.RequestParser()

    parser.add_argument(
        "departure_date_from",
        type=str,
        help="start date for counter data (format 2020-01-15)",
        default="2021-12-01",
        required=False,
    )

    parser.add_argument(
        "date_to",
        type=str,
        help="start date for counter data (format 2020-01-15)",
        default=-3,
        required=False,
    )

    parser.add_argument(
        "commodity_grouping",
        type=str,
        help="Grouping used (e.g. coal,oil,gas ('default') vs coal,oil,lng,pipeline_gas ('split_gas')",
        default="split_gas_oil",
    )

    parser.add_argument(
        "commodity",
        help="Commodity(ies) of interest",
        action="split",
        required=False,
        default=["crude_oil", "oil_products", "oil_or_chemical"],
    )

    parser.add_argument(
        "aggregate_by",
        type=str,
        action="split",
        default=[
            "ship_owner_country",
            "ship_insurer_country",
            "departure_date",
            "commodity_group",
        ],
        help="which variables to aggregate by. Could be any of commodity, type, destination_region, date",
    )

    parser.add_argument(
        "rolling_days",
        type=int,
        help="rolling average window (in days). Default: no rolling averaging",
        required=False,
        default=30,
    )

    parser.add_argument(
        "language", type=str, help="en or ua", default="en", required=False
    )

    parser.add_argument("group_eug7_insurernorwary", type=inputs.boolean, default=True)

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

        params = VoyageResource.parser.parse_args()
        params_chart = ChartDepartureOwnership.parser.parse_args()
        format = params_chart.get("format")
        aggregate_by = params_chart.get("aggregate_by").copy()
        nest_in_data = params_chart.get("nest_in_data")
        language = params_chart.get("language")
        group_eug7_insurernorwary = params_chart.get("group_eug7_insurernorwary")

        default_aggregate_by = [
            "ship_owner_country",
            "ship_insurer_country",
            "departure_date",
            "commodity_group",
        ]

        params.update(**params_chart)
        params.update(
            **{
                # 'pivot_by': ['destination_region'],
                # 'pivot_value': 'value_tonne',
                "use_eu": True,
                "commodity_origin_iso2": "RU",
                "commodity_destination_iso2_not": "RU",
                # 'date_from': '2022-01-01',
                "pricing_scenario": [base.PRICING_DEFAULT],
                # 'sort_by': ['value_tonne'],
                "currency": "EUR",
                "keep_zeros": True,
                "format": "json",
                "nest_in_data": True,
            }
        )

        response = VoyageResource().get_from_params(params)
        data = pd.DataFrame(response.json["data"])
        data["departure_date"] = pd.to_datetime(data.departure_date)

        def recode_eug7(
            ship_owner_region, ship_owner_iso2, ship_insurer_region, ship_insurer_iso2
        ):
            g7 = ["CA", "FR", "DE", "IT", "JP", "GB", "US"]
            res = np.where(
                (ship_owner_region == "EU")
                | ship_owner_iso2.isin(g7)
                | (ship_insurer_region == "EU")
                | ship_insurer_iso2.isin(g7),
                "Owned and / or insured in EU & G7",
                np.where(
                    ship_insurer_iso2 == "NO",
                    "Insured in Norway",
                    np.where(pd.isna(ship_owner_iso2), "Unknown", "Others"),
                ),
            )
            return res

        def translate(data, language):
            if language != "en":
                file_path = "assets/language/%s.json" % (language)
                with open(file_path, "r") as file:
                    translate_dict = json.load(file)

                data = data.replace(translate_dict)
                data.columns = [translate_dict.get(x, x) for x in data.columns]

            return data

        if group_eug7_insurernorwary:
            data["region"] = recode_eug7(
                data.ship_owner_region,
                data.ship_owner_iso2,
                data.ship_insurer_region,
                data.ship_insurer_iso2,
            )
        else:
            data["region"] = data.ship_owner_region
            data["region"].fillna(base.UNKNOWN, inplace=True)
            # Do in two steps in case voyage returned base.UNKNOWN
            data.replace({base.UNKNOWN: "Unknown"}, inplace=True)

        group_by_cols = ["region", "departure_date", "commodity_group_name"] + [
            x for x in aggregate_by if x not in default_aggregate_by
        ]
        pivot_cols = ["region"]
        index_cols = [x for x in group_by_cols if x not in pivot_cols]
        result = (
            data.groupby(group_by_cols)
            .value_tonne.sum()
            .reset_index()
            .pivot_table(
                index=index_cols,
                columns=pivot_cols,
                values="value_tonne",
                sort=False,
                fill_value=0,
            )
            .reset_index()
        )

        result = translate(data=result, language=language)

        return self.build_response(
            result=result, format=format, nest_in_data=nest_in_data
        )

    def build_response(self, result, format, nest_in_data):

        result.replace({np.nan: None}, inplace=True)

        # If bulk and departure berth is coal, replace commodity with coal
        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={
                    "Content-disposition": "attachment; filename=departure_by_ownership.csv"
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
