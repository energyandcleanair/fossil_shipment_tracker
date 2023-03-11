import pandas as pd
import json
import numpy as np
import re

from . import routes_api
from flask_restx import inputs

from base.db import session
from base.encoder import JsonEncoder
from base.utils import to_list, to_datetime
from base.logger import logger

from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse
from abc import abstractmethod


class TemplateResource(Resource):
    parser = reqparse.RequestParser()

    # Query processing
    parser.add_argument(
        "aggregate_by",
        type=str,
        action="split",
        default=None,
        help="which variables to aggregate by. Could be any of source,date,country,year",
    )
    parser.add_argument(
        "rolling_days",
        type=int,
        help="rolling average window (in days). Default: no rolling averaging",
        required=False,
        default=None,
    )
    parser.add_argument(
        "pivot_by",
        type=str,
        help="pivoting value_eur (or any other specified by pivot_value) by e.g. source,year.",
        required=False,
        action="split",
        default=None,
    )
    parser.add_argument(
        "pivot_value", type=str, help="pivoted value", required=False, default="value"
    )
    parser.add_argument(
        "pivot_fill_value",
        type=float,
        help="pivot filling value. Default: 0.",
        required=False,
        default=0,
    )
    parser.add_argument(
        "sort_by",
        type=str,
        help="sorting results e.g. asc(commodity),desc(value_eur)",
        required=False,
        action="split",
        default=None,
    )

    # Query format
    parser.add_argument(
        "format",
        type=str,
        help="format of returned results (json, geojson or csv)",
        required=False,
        default="json",
    )
    parser.add_argument(
        "nest_in_data",
        help="Whether to nest the json content in a data key.",
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
        "limit",
        type=int,
        help="how many result records do you want (default: keeps all)",
        required=False,
        default=None,
    )
    parser.add_argument(
        "limit_by",
        action="split",
        help="in which group do you want to limit to n records",
        required=False,
        default=None,
    )

    # MUST BE FILLED
    must_group_by = []
    date_cols = []
    value_cols = []
    pivot_dependencies = {}
    filename = ""

    @routes_api.expect(parser)
    def get(self):
        params = TemplateResource.parser.parse_args()
        return self.get_from_params(params)

    def get_aggregate_cols_dict(self, subquery):
        return {}

    def get_agg_value_cols(self, subquery):
        return []

    @abstractmethod
    def initial_query(self, params=None):
        return

    def filter(self, query, params=None):
        return query

    def aggregate(self, query, params=None):
        aggregate_by = params.get("aggregate_by")

        if aggregate_by and "" in aggregate_by:
            aggregate_by.remove("")

        if not aggregate_by:
            return query

        subquery = query.subquery()

        # Adding must have grouping columns
        aggregate_by.extend([x for x in self.must_group_by if x not in aggregate_by])
        if "" in aggregate_by:
            aggregate_by.remove("")

        # Aggregating
        agg_cols_dict = self.get_aggregate_cols_dict(subquery=subquery)
        agg_value_cols = self.get_agg_value_cols(subquery=subquery)

        if any([x not in agg_cols_dict for x in aggregate_by]):
            logger.warning(
                "aggregate_by can only be a selection of %s" % (",".join(agg_cols_dict.keys()))
            )
            aggregate_by = [x for x in aggregate_by if x in agg_cols_dict]

        groupby_cols = []
        for x in aggregate_by:
            groupby_cols.extend(agg_cols_dict[x])

        query = session.query(*groupby_cols, *agg_value_cols).group_by(*groupby_cols)
        return query

    def postcompute(self, result, params=None):
        return result

    def get_from_params(self, params):
        aggregate_by = params.get("aggregate_by")
        format = params.get("format", "json")
        nest_in_data = params.get("nest_in_data")
        download = params.get("download")
        rolling_days = params.get("rolling_days")
        sort_by = params.get("sort_by")
        pivot_by = params.get("pivot_by")
        pivot_value = params.get("pivot_value")
        pivot_fill_value = params.get("pivot_fill_value")
        limit = params.get("limit")
        limit_by = params.get("limit_by")

        # Create db query
        query = self.initial_query(params=params)

        query = self.filter(query=query, params=params)

        query = self.aggregate(query=query, params=params)

        # Collect
        result = pd.read_sql(query.statement, session.bind)

        if len(result) == 0:
            return Response(
                status=HTTPStatus.NO_CONTENT,
                response="empty",
                mimetype="application/json",
            )

        # Sort by date first
        # date_cols = intersection(self.date_cols, result.columns)
        # if date_cols:
        #     result = result.sort_values(date_cols)

        # Rolling average
        result = self.roll_average(
            result=result, aggregate_by=aggregate_by, rolling_days=rolling_days
        )

        # Spread currencies
        result = self.spread_currencies(result=result)

        # Sort results
        result = self.sort_result(result=result, sort_by=sort_by, aggregate_by=aggregate_by)

        # Keep only n records
        result = self.limit_result(
            result=result,
            limit=limit,
            aggregate_by=aggregate_by,
            sort_by=sort_by,
            limit_by=limit_by,
        )

        # Pivot
        result = self.pivot_result(
            result=result,
            pivot_by=pivot_by,
            pivot_value=pivot_value,
            pivot_fill_value=pivot_fill_value,
        )

        # Post compute
        result = self.postcompute(result=result, params=params)

        response = self.build_response(
            result=result,
            format=format,
            nest_in_data=nest_in_data,
            aggregate_by=aggregate_by,
            download=download,
        )
        return response

    def roll_average(self, result, aggregate_by, rolling_days):
        remove_date = False
        date_column = "date"

        if rolling_days is not None:
            if not "date" in result.columns:
                # Special case: if we did aggregate by date_without_year and year
                # Then we need to add date again, do the rolling average
                # and remove date
                if len(intersection(["date_without_year", "year"], result.columns)) == 2:
                    year = result.year.astype(str)
                    month_day = result.date_without_year.dt.strftime("%m%d")
                    result[date_column] = pd.to_datetime(
                        year + month_day, format="%Y%m%d", errors="coerce"
                    )
                    remove_date = True
                else:
                    # No date information
                    return result

            result = result[~pd.isna(result.date)]
            min_date = result["date"].min()
            max_date = result["date"].max()  # change your date here
            daterange = pd.date_range(min_date, max_date).rename("date")

            result["date"] = result["date"].dt.floor("D")  # Should have been done already
            result_rolled = (
                result.groupby(
                    [x for x in result.columns if x not in (self.date_cols + self.value_cols)]
                )[[date_column] + self.value_cols]
                .apply(
                    lambda x: x.set_index("date")
                    .resample("D")
                    .sum()
                    .reindex(daterange)
                    .fillna(0)
                    .rolling(rolling_days, min_periods=rolling_days)
                    .mean()
                )
                .reset_index()
            )

            # Add columns that may have disappeared e.g. date_without_year, year, month
            result = pd.merge(
                result_rolled,
                result[intersection(self.date_cols, result.columns)].drop_duplicates(),
            )

            if remove_date:
                result = result.drop("date", axis=1)

            # Sort by date
            result = result.sort_values(intersection(self.date_cols, result.columns))

        return result

    def pivot_result(self, result, pivot_by, pivot_value, pivot_fill_value=0):
        if pivot_by:
            pivot_by_dependencies = [
                d for x in to_list(pivot_by) for d in self.pivot_dependencies.get(x, [])
            ]
            index = [
                x
                for x in result.columns
                if not x.startswith("value")
                and x not in to_list(pivot_by)
                and x not in pivot_by_dependencies
            ]

            result["variable"] = pivot_value
            result = result.pivot_table(
                index=index + ["variable"],
                columns=to_list(pivot_by),
                values=pivot_value,
                sort=False,
                fill_value=pivot_fill_value,
            ).reset_index()

        return result

    def build_response(self, result, format, nest_in_data, aggregate_by, download):
        result.replace({np.nan: None}, inplace=True)

        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=%s.csv" % (self.filename,)},
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

    def sort_result(self, result, sort_by, aggregate_by):
        by = []
        ascending = []
        default_ascending = False

        if sort_by:
            for s in intersection(sort_by, result.columns):
                m = re.match("(.*)\\((.*)\\)", s)
                if m:
                    ascending.append(m[1] == "asc")
                    by.append(m[2])
                else:
                    # No asc(.*) or desc(.*)
                    ascending.append(default_ascending)
                    by.append(s)

            if not aggregate_by:
                sorting_groupers = ["destination_country"]

            if aggregate_by:
                dependencies = {"source_raw": ["source"]}

                aggregate_by_dependencies = [
                    d for x in to_list(aggregate_by) for d in dependencies.get(x, [])
                ]

                sorting_groupers = [
                    x
                    for x in aggregate_by
                    if not x in aggregate_by_dependencies
                    and not x in ["date", "month", "year", "currency", "date_without_year"]
                    and x in result.columns
                ]

            sorted = (
                result.groupby(sorting_groupers)[by]
                .sum()
                .reset_index()
                .sort_values(by=by, ascending=ascending)
                .drop(sort_by, axis=1)
            )

            result = pd.merge(sorted, result, how="left")

        return result

    def limit_result(self, result, limit, aggregate_by, sort_by, limit_by):
        if not limit:
            return result

        limit_by = to_list(limit_by) or []

        if not aggregate_by:
            group_by = ["destination_country"]

        if aggregate_by:
            group_by = [
                x
                for x in aggregate_by
                if not x.startswith("commodity")
                and not x in ["date", "month", "year"]
                and x in result.columns
            ]

        sort_by = sort_by or "value_eur"
        group_by = group_by + [x for x in limit_by if x not in group_by]

        # Can only take one
        sort_by = to_list(sort_by)[0]
        top = (
            result.groupby(group_by)
            .agg({sort_by: "sum"})
            .reset_index()
            .sort_values(limit_by + to_list(sort_by), ascending=False)
        )

        if limit_by:
            top = top.groupby(limit_by, as_index=False)

        top = top.head(limit).drop(sort_by, axis=1)

        result = pd.merge(result, top, how="inner")

        return result

    def spread_currencies(self, result):
        # We simply want to pivot across currencies
        # But pandas need clean non-null and hashable data, hence this whole function...
        len_before = len(result)
        n_currencies = len(result.currency.unique())
        sep = "#,#"
        old_columns = result.columns  # Used to keep the order

        result["currency"] = "value_" + result.currency.str.lower()

        # Create a hashable version
        # if "destination_names" in result.columns:
        #     result["destination_names"] = result["destination_names"].apply(
        #         lambda row: sep.join(row) if row else row
        #     )
        #     result["destination_iso2s"] = result["destination_iso2s"].apply(
        #         lambda row: sep.join([str(x) for x in row]) if row else row
        #     )
        #     result["destination_dates"] = result["destination_dates"].apply(
        #         lambda row: sep.join([x.strftime("%Y-%m-%d %H:%M:%S") for x in row])
        #         if row
        #         else row
        #     )

        index_cols = [
            x for x in result.columns if x not in ["currency", "value_currency", "value_eur"]
        ]
        # result[index_cols] = result[index_cols].replace({np.nan: na_str})
        # result[index_cols] = result[index_cols].replace({None: na_str})

        result = (
            result[index_cols + ["currency", "value_currency"]]
            .set_index(index_cols + ["currency"])["value_currency"]
            .unstack(-1)
            .reset_index()
        )

        # Recreate lists
        # if "destination_names" in result.columns:
        #     result.loc[
        #         ~result.destination_names.isnull(), "destination_names"
        #     ] = result.loc[
        #         ~result.destination_names.isnull(), "destination_names"
        #     ].apply(
        #         lambda row: row.split(sep)
        #     )
        #
        #     result.loc[
        #         ~result.destination_iso2s.isnull(), "destination_iso2s"
        #     ] = result.loc[
        #         ~result.destination_iso2s.isnull(), "destination_iso2s"
        #     ].apply(
        #         lambda row: row.split(sep)
        #     )
        #
        #     result.loc[
        #         ~result.destination_dates.isnull(), "destination_dates"
        #     ] = result.loc[
        #         ~result.destination_dates.isnull(), "destination_dates"
        #     ].apply(
        #         lambda row: row.split(sep)
        #     )  # We keep it as string

        # Quick sanity check
        len_after = len(result)
        assert len_after == len_before / n_currencies

        return result
