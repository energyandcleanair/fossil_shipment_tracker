import pandas as pd
import json
import numpy as np
import re
from collections.abc import Iterable


from . import routes_api
from flask_restx import inputs

from base.db import session
from base.encoder import JsonEncoder
from base.utils import to_list, to_datetime, intersect
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
    parser.add_argument(
        "select",
        type=str,
        help="selecting specific columns only, with the possibility to rename e.g. new_name1(var1),var2,var3",
        required=False,
        action="split",
        default=None,
    )

    parser.add_argument(
        "check_complete",
        type=inputs.boolean,
        help="whether to check if the dataset is complete",
        required=False,
        default=True,
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

    def check_complete(self, query, params):

        return True, None

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
            missing = ",".join(filter(lambda x: x not in agg_cols_dict, aggregate_by))
            selection = ",".join(agg_cols_dict.keys())
            logger.warning(
                f"aggregate_by contained {missing} but can only be a selection of {selection}"
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

        check_complete = params.get("check_complete")
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
        select = params.get("select")

        # Create db query
        query = self.initial_query(params=params)

        query = self.filter(query=query, params=params)

        if check_complete:
            check_status, incomplete_reason = self.check_complete(query=query, params=params)
            if not check_status:
                return Response(
                    status=HTTPStatus.NOT_FOUND,
                    response=f"The dataset requested is not complete. "
                    + f"Check the data and decide whether it needs updating or if you can continue "
                    + f"with incomplete data. You can continue with incomplete data by specifying check_complete=False.\n"
                    + f"The following data is incomplete:\n{incomplete_reason}",
                    mimetype="text/plain",
                )

        query = self.aggregate(query=query, params=params)

        # Collect
        result = pd.read_sql(query.statement, session.bind)

        if len(result) == 0:
            return Response(
                status=HTTPStatus.NO_CONTENT,
                response="empty",
                mimetype="application/json",
            )

        # # Sort by date first
        # date_cols = intersect(self.date_cols, result.columns)
        # if date_cols:
        #     result = result.sort_values(date_cols)

        # Hash i.e. convert list to tuples so that pandas can hash
        result, list_columns = self.hash_df(result)

        # Rolling average
        result = self.roll_average(
            result=result, aggregate_by=aggregate_by, rolling_days=rolling_days
        )

        # Spread currencies
        result = self.spread_currencies(result=result, prehashed=True)

        # Unhash
        result = self.unhash_df(result=result, list_columns=list_columns)

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

        result = self.select(result, select=select)

        response = self.build_response(
            result=result,
            format=format,
            nest_in_data=nest_in_data,
            aggregate_by=aggregate_by,
            download=download,
        )
        return response

    def roll_average(self, result, aggregate_by, rolling_days):
        # Early exit if we're not doing rolling days
        if rolling_days is None:
            return result

        found_any_date_cols = intersect(self.date_cols, result.columns)

        special_date_cols = ["date_without_year", "year"]
        found_special_date_cols = intersect(special_date_cols, result.columns)

        # If we're trying to roll and we can't find a date column, raise an error.
        if len(found_any_date_cols) == 0 and len(found_special_date_cols) != 2:
            raise RuntimeError("No matching columns for rolling average")

        # We default to the first found date column but then use an aggregated one if found
        # as it's more likely we're going to want to do the rolling on that date.
        found_aggregate_date_cols = intersect(
            intersect(self.date_cols, aggregate_by), result.columns
        )
        date_column = found_any_date_cols[0]
        if len(found_aggregate_date_cols) > 0:
            date_column = found_aggregate_date_cols[0]

        # For the special date column, we need to recreate the date.
        overwrite_date_column = "_temp_date"
        remove_date = False
        if len(found_special_date_cols) == 2:
            year = result.year.astype(str)
            month_day = result.date_without_year.dt.strftime("%m%d")
            result[overwrite_date_column] = pd.to_datetime(
                year + month_day, format="%Y%m%d", errors="coerce"
            )
            date_column = overwrite_date_column

        result = result[~pd.isna(result[date_column])]
        min_date = result[date_column].min()
        max_date = result[date_column].max()
        daterange = pd.date_range(min_date, max_date).rename(date_column)

        result[date_column] = pd.to_datetime(result[date_column]).dt.floor(
            "D"
        )  # Should have been done already
        result_rolled = (
            result.groupby(
                [x for x in result.columns if x not in (self.date_cols + self.value_cols)],
                dropna=False,
            )[[date_column] + self.value_cols]
            .apply(
                lambda x: x.set_index(date_column)
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
            result[intersect(self.date_cols, result.columns)].drop_duplicates(),
        )

        result[date_column] = pd.to_datetime(result[date_column])

        if remove_date:
            result = result.drop(date_column, axis=1)

        # Sort by date
        result = result.sort_values(intersect(self.date_cols, result.columns))

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
                and x not in self.value_cols
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

    def select(self, result, select):
        if not select:
            return result

        names = []
        variables = []

        for s in to_list(select):
            m = re.match("(.*)\\((.*)\\)", s)
            if m:
                names.append(m[1])
                variables.append(m[2])
            else:
                # No asc(.*) or desc(.*)
                names.append(s)
                variables.append(s)

        result = result[variables]
        result.columns = names
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

    def hash_df(self, df):
        # Create a hashable version
        # find columns that are list and convert them to tuple
        list_columns = [
            col
            for col in df.columns
            if any(df[col].notna()) and any(df[col].apply(lambda x: type(x) == list))
        ]

        def to_tuple_if_iterable(x):
            if x is None:
                return None
            if np.isscalar(x):
                if pd.isna(x):
                    return None
                return x
            if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
                return tuple(x)
            return x

        for col in list_columns:
            df[col] = df[col].apply(to_tuple_if_iterable)
        return df, list_columns

    def unhash_df(self, result, list_columns):
        # Unhash the dataframe
        def to_list_if_iterable(x):
            if x is None or (np.isscalar(x) and pd.isna(x)):
                return None
            if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
                return list(x)
            return x

        for col in list_columns:
            result[col] = result[col].apply(to_list_if_iterable)
        return result

    def spread_currencies(self, result, prehashed=False):

        if "currency" not in result.columns:
            return result

        # We simply want to pivot across currencies
        # But pandas need clean non-null and hashable data, hence this whole function...
        len_before = len(result)
        n_currencies = len(result.currency.unique())

        result["currency"] = "value_" + result.currency.str.lower()

        # Replace nan with None
        result.replace({np.nan: None}, inplace=True)

        if not prehashed:
            # Create a hashable version
            # find columns that are list and convert them to tuple
            list_columns = [
                col
                for col in result.columns
                if any(result[col].notna()) and any(result[col].apply(lambda x: type(x) == list))
            ]
            for col in list_columns:
                result[col] = result[col].apply(tuple)

        # Round all value_ columns to prevent pivoting error when there is an epsilon diff
        # Observed on kpler_trade once
        value_cols = [x for x in result.columns if x.startswith("value_")]
        result[value_cols] = result[value_cols].round(6)

        index_cols = [
            x for x in result.columns if x not in ["currency", "value_currency", "value_eur"]
        ]

        result = (
            result[index_cols + ["currency", "value_currency"]]
            .set_index(index_cols + ["currency"])["value_currency"]
            .unstack(-1)
            .reset_index()
        )

        # Quick sanity check
        len_after = len(result)
        assert len_after == len_before / n_currencies

        return result
