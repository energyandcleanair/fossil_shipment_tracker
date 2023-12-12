import pandas as pd
import json
import numpy as np
import datetime as dt
import re
from flask_restx import inputs
from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse


import base
from base import (
    CHARTS_USE_KPLER_DEFAULT,
    COMMODITY_GROUPING_DEFAULT,
    COMMODITY_GROUPING_CHOICES,
    COMMODITY_GROUPING_HELP,
)
from base.encoder import JsonEncoder
from base.utils import to_list
from .. import postcompute
from .. import routes_api, ns_charts
from ..voyage import VoyageResource
from ..overland import PipelineFlowResource

from .voyage_data_proxy import get_voyages


@ns_charts.route("/v0/chart/departure_by_destination", strict_slashes=False)
class ChartDepartureDestination(Resource):
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
        default=-2,
        required=False,
    )

    parser.add_argument(
        "add_total_commodity",
        help="Whether to add a sum of all commodities",
        type=inputs.boolean,
        default=True,
    )

    parser.add_argument(
        "country_grouping",
        type=str,
        help="How to group countries. Can be 'region' or 'top_n' (e.g. top_5)",
        default="top_5",
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
        default=["destination_country", "commodity_group_name", "departure_date", "status"],
        help="which variables to aggregate by. Could be any of commodity, type, destination_region, date",
    )

    parser.add_argument("language", type=str, help="en or ua", default="en", required=False)
    parser.add_argument(
        "postcompute",
        type=str,
        help="Post=compute function",
        required=False,
        default=None,
    )
    parser.add_argument(
        "rolling_days",
        type=int,
        help="rolling average window (in days). Default: no rolling averaging",
        required=False,
        default=30,
    )
    parser.add_argument(
        "pivot_by",
        type=str,
        help="pivot column. Default: region.",
        required=False,
        default="region",
    )
    parser.add_argument(
        "pivot_value",
        action="split",
        help="pivoted value(s). Default: value_tonne.",
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
        "use_kpler",
        help="Whether to use Kpler or MT",
        type=inputs.boolean,
        default=CHARTS_USE_KPLER_DEFAULT,
    )

    @routes_api.expect(parser)
    def get(self):
        params = VoyageResource.parser.parse_args()
        params_chart = ChartDepartureDestination.parser.parse_args()
        params_overland = PipelineFlowResource.parser.parse_args()

        format = params_chart.get("format")
        commodity = params_chart.get("commodity")
        add_total_commodity = params_chart.get("add_total_commodity")
        nest_in_data = params_chart.get("nest_in_data")
        country_grouping = params_chart.get("country_grouping")
        date_to = params_chart.get("date_to")
        pivot_by = params_chart.get("pivot_by")
        pivot_value = params_chart.get("pivot_value")
        departure_date_from = params_chart.get("departure_date_from")
        language = params_chart.get("language")
        use_kpler = params_chart.get("use_kpler")

        params.update(**params_chart)
        params.update(
            **{
                # 'pivot_by': ['destination_region'],
                # 'pivot_value': 'value_tonne',
                "use_eu": True,
                "commodity_origin_iso2": "RU",
                "commodity_destination_iso2_not": "RU",
                "destination_iso2_not": "RU",
                "date_to": None,
                "departure_date_from": departure_date_from,
                "departure_date_to": date_to,
                "commodity": None if add_total_commodity else commodity,
                # 'date_from': '2022-01-01',
                "pricing_scenario": [base.PRICING_DEFAULT],
                # 'sort_by': ['value_tonne'],
                "currency": "EUR",
                "keep_zeros": True,
                "format": "json",
                "nest_in_data": True,
                "pivot_by": None,
                "select": [
                    "departure_date",
                    "commodity_group_name",
                    "destination_country",
                    "destination_iso2",
                    "destination_region",
                    "commodity_group_name",
                    "value_tonne",
                    "value_eur",
                ],
            }
        )

        params_overland.update(
            **{
                "commodity_origin_iso2": "RU",
                "aggregate_by": ["destination_country", "commodity_group_name", "date"],
                "date_from": departure_date_from,
                "date_to": date_to,
                "commodity": None if add_total_commodity else commodity,
                "pricing_scenario": [base.PRICING_DEFAULT],
                "currency": "EUR",
                "keep_zeros": True,
                "format": "json",
                "nest_in_data": True,
                "pivot_by": None,
            }
        )

        def group_countries(data, country_grouping):
            import re

            if re.search("top_[0-9]*", country_grouping):
                # Make EU a country
                data.loc[data.destination_region == "EU", "destination_country"] = "EU"
                data.loc[data.destination_region == "EU", "destination_iso2"] = "EU"

                # When including pipeline, small countries (Europe non EU) would show up because it's mainly Europe and China
                exclude_countries = ["RS", "MK", "MD", "SM", "CH"]

                n = int(country_grouping.replace("top_", ""))
                top_n = (
                    data[
                        (data.departure_date >= max(data.departure_date) - dt.timedelta(days=30))
                        & ~data.destination_iso2.isin(exclude_countries)
                        & (data.destination_region != "Unknown")
                    ]
                    .groupby(["commodity_group_name", "destination_country"])
                    .value_tonne.sum()
                    .reset_index()
                    .sort_values("value_tonne", ascending=False)
                    .groupby(["commodity_group_name"])
                    .head(n)
                )

                top_n["region"] = top_n.destination_country
                # Keeping the same for all commodities
                # Otherwise Flourish will show empty lines
                # which might make viewer think values are actually 0
                top_n = top_n[["destination_country", "region"]].drop_duplicates()
                top_n.loc[len(top_n)] = ["Unknown", "Unknown"]
                top_n = top_n.drop_duplicates()
                data = (
                    data.fillna({"destination_country": "Unknown"})
                    .merge(top_n[["destination_country", "region"]], how="left")
                    .fillna({"region": "Others"})
                )

                # Keep for orders
                data.loc[data.destination_iso2 == base.FOR_ORDERS, "region"] = "For orders"

            else:
                data["region"] = data.destination_region

            data = (
                data.groupby(
                    [
                        "commodity_group_name",
                        "region",
                        "departure_date",
                    ],
                    dropna=False,
                )[["value_tonne", "value_eur"]]
                .sum()
                .reset_index()
                .sort_values(["departure_date"])
            )

            return data

        def pivot_data(data, pivot_by, pivot_value):
            pivot_values = to_list(pivot_value)
            if len(pivot_values) > 1:
                return pd.concat(
                    [
                        pivot_data(data=data.copy(), pivot_by=pivot_by, pivot_value=x)
                        for x in pivot_values
                    ]
                )
            else:
                pivot_value = pivot_values[0]

            data["variable"] = pivot_value
            result = (
                data.groupby(
                    ["region", "departure_date", "commodity_group_name", "variable"],
                    dropna=False,
                )[pivot_value]
                .sum()
                .reset_index()
                .pivot_table(
                    index=["commodity_group_name", "departure_date", "variable"],
                    columns=[pivot_by],
                    values=pivot_value,
                    sort=False,
                    fill_value=0,
                )
                .reset_index()
            )
            return result

        def translate(data, language):
            if language != "en":
                file_path = "assets/language/%s.json" % (language)
                with open(file_path, "r") as file:
                    translate_dict = json.load(file)

                data = data.replace(translate_dict)
                data.columns = [translate_dict.get(x, x) for x in data.columns]

            return data

        def remove_coal_to_eu(data, date_stop=dt.date(2022, 8, 11)):
            data.loc[
                (data.destination_region == "EU")
                & (data.commodity_group_name.str.lower() == "coal")
                & (pd.to_datetime(data.departure_date) >= pd.to_datetime(date_stop)),
                ["value_eur", "value_tonne"],
            ] = 0
            return data

        def add_total(data):
            groupby_cols = [c for c in data.columns if not re.match("commodity|value", c)]
            value_cols = [c for c in data.columns if re.match("value", c)]
            data_global = data.groupby(groupby_cols, dropna=False)[value_cols].sum().reset_index()

            data_global["commodity_group_name"] = "Total"

            data = pd.concat([data, data_global])
            return data

        # Get overland
        response_overland = PipelineFlowResource().get_from_params(params_overland)
        if response_overland.status_code == 200:
            data_overland = pd.DataFrame(response_overland.json["data"])
            data_overland.rename(columns={"date": "departure_date"}, inplace=True)
            data_overland["departure_date"] = pd.to_datetime(data_overland.departure_date)
        else:
            # Happens when no overland commodity selected
            data_overland = None

        # Get voyages
        data_voyage = get_voyages(params, use_kpler=use_kpler)
        if not data_voyage.empty:
            data_voyage = remove_coal_to_eu(data_voyage)
            data_voyage["departure_date"] = pd.to_datetime(data_voyage.departure_date)

            data_voyage.replace({base.UNKNOWN: "Unknown"}, inplace=True)
        data = pd.concat([data_overland, data_voyage])
        data["departure_date"] = pd.to_datetime(data["departure_date"]).dt.date

        if add_total_commodity:
            data = add_total(data)
            if commodity:
                data = data[data.commodity.isin(to_list(commodity) + ["Total"])]

        data = group_countries(data, country_grouping)
        if pivot_by is not None and pivot_by != "":
            data = pivot_data(data, pivot_by=pivot_by, pivot_value=pivot_value)
        data = self.postcompute(data, params=params)

        # Drop pricing scenario until we find issue of why it is not present sometimes
        # TODO remove once fixed
        if "pricing_scenario" in data.columns:
            data.drop("pricing_scenario", axis=1, inplace=True)

        data = translate(data=data, language=language)

        return self.build_response(result=data, format=format, nest_in_data=nest_in_data)

    def postcompute(self, result, params=None):
        postcompute_fn = postcompute.get_postcompute_fn(params.get("postcompute"))
        if postcompute_fn:
            result = postcompute_fn(result, params=params)
        return result

    def build_response(self, result, format, nest_in_data):
        result.replace({np.nan: None}, inplace=True)

        # If bulk and departure berth is coal, replace commodity with coal
        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={
                    "Content-disposition": "attachment; filename=departure_by_destination.csv"
                },
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
