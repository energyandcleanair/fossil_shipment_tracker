import pandas as pd
import json
import numpy as np
from flask_restx import inputs
from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse

from .. import routes_api, ns_charts
import base
from base.encoder import JsonEncoder
from base.utils import to_list, df_to_json, to_datetime
from ..counter import RussiaCounterResource

import datetime as dt


@ns_charts.route("/v0/chart/total_payments", strict_slashes=False)
class ChartTotalPayments(Resource):
    parser = reqparse.RequestParser()

    parser.add_argument(
        "date_to",
        type=str,
        help="start date for counter data (format 2020-01-15)",
        default=-5,
        required=False,
    )

    parser.add_argument(
        "limit",
        type=int,
        help="how many result records do you want (default: keeps all)",
        required=False,
        default=None,
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
        "version",
        help="Which counter version to use (v0=MarineTraffic/Datalastic, v1=Kpler Flows, v2=Kpler Trades)",
        type=str,
        default=base.COUNTER_VERSION_DEFAULT,
    )

    @routes_api.expect(parser)
    def get(self):
        params = RussiaCounterResource.parser.parse_args()
        params_chart = ChartTotalPayments.parser.parse_args()
        limit = params_chart.get("limit")
        format = params_chart.get("format")
        nest_in_data = params_chart.get("nest_in_data")

        date_to = to_datetime(params_chart.get("date_to"))
        if not date_to:
            date_to = dt.datetime.now()

        params.update(**params_chart)
        params.update(
            **{
                "aggregate_by": ["destination_country", "commodity_group", "month"],
                "pivot_by": ["commodity_group_name"],
                "pivot_value": "value_eur",
                "use_eu": True,
                "sort_by": ["value_eur"],
                "currency": "EUR",
                "keep_zeros": True,
                "format": "json",
                "nest_in_data": True,
                "destination_region": None,
                "limit": None,
            }
        )

        # Period 1
        params["date_from"] = "2022-02-24"
        response = RussiaCounterResource().get_from_params(params)
        data1 = pd.DataFrame(response.json["data"])
        data1["period"] = f"From beginning of the war until {date_to}"

        # Period 2
        data2 = data1.copy()
        data2 = data2[data2.month >= "2023-01-01"]
        data2["period"] = f"From 1 January 2023 until {date_to}"

        data = pd.concat([data1, data2])
        data = (
            data.groupby(["destination_country", "destination_region", "period"])
            .sum()
            .reset_index()
        )

        data = self.add_eu(data)
        data = self.limit(data, limit)

        return self.build_response(result=data, format=format, nest_in_data=nest_in_data)

    def add_eu(self, data):
        data_eu = data[data.destination_region == "EU"]
        data_eu = data_eu.groupby(["destination_region", "period"]).sum().reset_index()
        data_eu["destination_country"] = "EU"
        data = pd.concat([data, data_eu])
        return data

    def limit(self, data, limit):
        # Keep limit records within each period group
        # with the highest total value
        if limit:
            # Sum all numeric columns
            data["total"] = data.select_dtypes(include=np.number).sum(axis=1)
            # Only keep the limit largest records per group
            data = data.groupby("period", as_index=False).apply(
                lambda x: x.nlargest(limit, "total")
            )
            # Drop the total column
            data = data.drop(columns="total")

        return data

    def build_response(self, result, format, nest_in_data):
        result.replace({np.nan: None}, inplace=True)

        # If bulk and departure berth is coal, replace commodity with coal
        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=chart_monthly_payments.csv"},
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
