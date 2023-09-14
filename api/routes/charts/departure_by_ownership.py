import pandas as pd
import json
import numpy as np
from flask_restx import inputs
from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse


import base
from base.logger import logger
from base.encoder import JsonEncoder
from base.utils import to_list, df_to_json, to_datetime
from .. import routes_api, ns_charts
from ..voyage import VoyageResource
from ..kpler_trade import KplerTradeResource


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
        help="end date for counter data (format 2020-01-15)",
        default=-3,
        required=False,
    )

    parser.add_argument(
        "commodity_origin_iso2",
        action="split",
        help="iso2(s) of commodity origin",
        required=False,
        default=["RU"],
    )

    parser.add_argument(
        "commodity_destination_iso2",
        help="What new destination country code(s) e.g. IT,IN",
        action="split",
        required=False,
        default=None,
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
        "status",
        help="status of shipments. Could be any or several of completed, ongoing, undetected_arrival. Default: returns all of them",
        default=[base.ONGOING, base.COMPLETED],
        action="split",
        required=False,
    )

    parser.add_argument(
        "aggregate_by",
        type=str,
        action="split",
        help="which variables to aggregate by. Could be any of commodity, type, destination_region, date",
    )

    parser.add_argument(
        "rolling_days",
        type=int,
        help="rolling average window (in days). Default: no rolling averaging",
        required=False,
        default=30,
    )

    parser.add_argument("language", type=str, help="en or ua", default="en", required=False)

    parser.add_argument("group_eug7_insurernorwary", type=inputs.boolean, default=True)

    parser.add_argument(
        "metric",
        type=str,
        help="value_tonne or count",
        required=False,
        default="value_tonne",
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

    parser.add_argument(
        "departure_port_area",
        action="split",
        help="area of departure ports to consider e.g. Baltic,Arctic,Pacific,Black Sea,Caspian Sea",
        required=False,
        default=None,
    )

    parser.add_argument("use_kpler", help="Whether to use Kpler or MT", type=bool, default=False)

    @routes_api.expect(parser)
    def get(self):
        params = VoyageResource.parser.parse_args()
        params_chart = ChartDepartureOwnership.parser.parse_args()
        format = params_chart.get("format")
        nest_in_data = params_chart.get("nest_in_data")
        language = params_chart.get("language")
        metric = params_chart.get("metric")
        group_eug7_insurernorwary = params_chart.get("group_eug7_insurernorwary")
        departure_port_area = params_chart.get("departure_port_area")
        commodity_origin_iso2 = params_chart.get("commodity_origin_iso2")
        commodity_destination_iso2 = params_chart.get("commodity_destination_iso2")
        use_kpler = params_chart.get("use_kpler")

        aggregate_by_sanction_groups = [
            "ownership_sanction_coverage",
            "departure_date",
            "commodity_group",
            "status",
        ]

        aggregate_by_country = [
            "ship_owner_country",
            "ship_insurer_country",
            "departure_date",
            "commodity_group",
            "status",
        ]

        default_aggregate_by = (
            aggregate_by_sanction_groups
            if use_kpler or group_eug7_insurernorwary
            else aggregate_by_country
        )

        aggregate_by = params_chart.get(
            "ownership_sanction_coverage",
            default_aggregate_by,
        ).copy()

        params.update(**params_chart)
        params.update(
            **{
                # 'pivot_by': ['destination_region'],
                # 'pivot_value': 'value_tonne',
                "use_eu": True,
                "commodity_origin_iso2": commodity_origin_iso2,
                "commodity_destination_iso2_not": commodity_origin_iso2,
                "commodity_destination_iso2": commodity_destination_iso2,
                # 'date_from': '2022-01-01',
                "pricing_scenario": [base.PRICING_DEFAULT],
                "departure_port_area": departure_port_area,
                # 'sort_by': ['value_tonne'],
                "currency": "EUR",
                "keep_zeros": True,
                "format": "json",
                "nest_in_data": True,
            }
        )

        data = self.get_voyages(params, use_kpler=use_kpler, aggregate_by=aggregate_by)

        def translate(data, language):
            if language != "en":
                file_path = "assets/language/%s.json" % (language)
                with open(file_path, "r") as file:
                    translate_dict = json.load(file)

                data = data.replace(translate_dict)
                data.columns = [translate_dict.get(x, x) for x in data.columns]

            return data

        if use_kpler or group_eug7_insurernorwary:
            data["region"] = data["ownership_sanction_coverage"]
        else:
            data["region"] = data.ship_owner_region
            data["region"].fillna(base.UNKNOWN, inplace=True)
            # Do in two steps in case voyage returned base.UNKNOWN
            data.replace({base.UNKNOWN: "Unknown"}, inplace=True)

        group_by_cols = ["region", "departure_date", "commodity_group_name"] + [
            x for x in aggregate_by if x not in default_aggregate_by and x in data.columns
        ]
        pivot_cols = ["region"]
        index_cols = [x for x in group_by_cols if x not in pivot_cols]

        pivot_result_cols = ["departure_date", "commodity_group_name"] + list(
            data["region"].unique()
        )
        if use_kpler or group_eug7_insurernorwary:
            # We need to add missing columns in to work across all charts
            pivot_result_cols = ["departure_date", "commodity_group_name"] + [
                "Insured in Norway",
                "Others",
                "Owned and / or insured in EU & G7",
                "Unknown",
            ]

        result = (
            data.groupby(group_by_cols, dropna=False)[metric]
            .sum()
            .reset_index()
            .pivot_table(
                index=index_cols,
                columns=pivot_cols,
                values=metric,
                sort=False,
                fill_value=0,
            )
            .reset_index()
            .reindex(columns=pivot_result_cols)
            .fillna(0)
        )

        result = translate(data=result, language=language)

        logger.info(
            "[Departures by ownership] columns returned: %s. Pandas version: %s."
            % (",".join(result.columns), pd.__version__)
        )

        # Drop pricing scenario until we find issue of why it is not present sometimes
        # TODO remove once fixed
        if "pricing_scenario" in result.columns:
            result.drop("pricing_scenario", axis=1, inplace=True)

        return self.build_response(result=result, format=format, nest_in_data=nest_in_data)

    def build_response(self, result, format, nest_in_data):
        result.replace({np.nan: None}, inplace=True)

        # If bulk and departure berth is coal, replace commodity with coal
        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=departure_by_ownership.csv"},
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

    def get_voyages(self, params, aggregate_by, use_kpler=False):
        if use_kpler:
            return self.get_voyages_kpler(params, aggregate_by)
        else:
            return self.get_voyages_mt(params, aggregate_by)

    def get_voyages_kpler(self, params, aggregate_by):
        params_kpler = params.copy()
        params_kpler["commodity_equivalent"] = params_kpler["commodity"]
        params_kpler["commodity"] = None
        corr = {
            "departure_date": "origin_date",
            "commodity_group": "commodity_equivalent_name",
        }
        params_kpler["aggregate_by"] = [corr.get(x, x) for x in aggregate_by]

        response = KplerTradeResource().get_from_params(params_kpler)
        data = pd.DataFrame(response.json["data"])
        data["departure_date"] = pd.to_datetime(data.date).dt.date
        data["commodity_group_name"] = data["commodity_equivalent_name"]
        return data

    def get_voyages_mt(self, params, aggregate_by):
        params_voyages = params.copy()
        params_voyages["aggregate_by"] = aggregate_by

        response = VoyageResource().get_from_params(params_voyages)
        data = pd.DataFrame(response.json["data"])
        data["departure_date"] = pd.to_datetime(data.departure_date).dt.date
        return data
