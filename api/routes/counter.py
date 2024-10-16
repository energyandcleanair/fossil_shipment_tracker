import pandas as pd
import numpy as np
import base
import re
import json
import datetime as dt
from flask import Response
from flask_restx import Resource, reqparse, inputs
from sqlalchemy import func
import sqlalchemy as sa
from sqlalchemy import case
from operator import attrgetter

from . import routes_api, postcompute
from base import (
    PRICING_DEFAULT,
    COUNTER_VERSION_DEFAULT,
    COMMODITY_GROUPING_DEFAULT,
    COMMODITY_GROUPING_CHOICES,
    COMMODITY_GROUPING_HELP,
)
from base.logger import logger
from base.db import session
from base.models import Counter, Country, Currency, PriceScenario
from base.utils import to_datetime, to_list, intersect, df_to_json, hash_df, unhash_df
from .commodity import get_subquery as get_commodity_subquery


@routes_api.route("/v0/counter", strict_slashes=False)
class RussiaCounterResource(Resource):
    @staticmethod
    def get_aggregateby_cols(subquery=None):
        aggregate_cols_dict = {
            "currency": ["currency"],
            "pricing_scenario": [
                "pricing_scenario",
                "pricing_scenario_name",
            ],
            "date": ["date"],
            "month": ["date"],
            "year": ["date"],
            "commodity": [
                "commodity",
                "commodity_group",
                "commodity_group_name",
            ],
            "commodity_group": [
                "commodity_group",
                "commodity_group_name",
            ],
            "destination_iso2": [
                "destination_iso2",
                "destination_country",
                "destination_region",
                "destination_regions",
            ],
            "destination_country": [
                "destination_iso2",
                "destination_country",
                "destination_region",
                "destination_regions",
            ],
            "destination_region": ["destination_region"],
            "version": ["version"],
        }

        if subquery is not None:
            return {
                k: to_list(attrgetter(*v)(subquery.columns), convert_tuple=True)
                for k, v in aggregate_cols_dict.items()
            }

        return aggregate_cols_dict

    parser = reqparse.RequestParser()
    parser.add_argument(
        "cumulate",
        type=inputs.boolean,
        help="whether or not to cumulate (i.e. sum) data over time",
        required=False,
        default=False,
    )
    parser.add_argument(
        "use_eu",
        type=inputs.boolean,
        help="use EU instead of EU28",
        required=False,
        default=True,
    )
    parser.add_argument(
        "aggregate_by",
        type=str,
        action="split",
        default=None,
        help="which variables to aggregate by. Can be one of {}.".format(
            ", ".join(get_aggregateby_cols.__func__().keys())
        ),
    )
    parser.add_argument(
        "rolling_days",
        type=int,
        help="rolling average window (in days). Default: no rolling averaging",
        required=False,
        default=None,
    )
    parser.add_argument(
        "format",
        type=str,
        help="format of returned results (json or csv)",
        required=False,
        default="json",
    )
    parser.add_argument(
        "date_from",
        type=str,
        help="start date for counter data (format 2020-01-15)",
        default="2022-02-24",
        required=False,
    )
    parser.add_argument(
        "date_to",
        type=str,
        help="end date for arrival (format 2020-01-15)",
        required=False,
        default=dt.datetime.today().strftime("%Y-%m-%d"),
    )
    parser.add_argument(
        "destination_iso2",
        action="split",
        help="ISO2(s) of country of interest",
        required=False,
        default=None,
    )
    parser.add_argument(
        "destination_region",
        action="split",
        help="region(s) of destination e.g. EU,Turkey",
        required=False,
        default=None,
    )
    parser.add_argument(
        "destination_region_not",
        action="split",
        help="region(s) of destination to exclude e.g. For orders",
        required=False,
        default=None,
    )
    parser.add_argument(
        "commodity",
        action="split",
        help="commodity to include e.g. crude_oil,oil_products,lng (see commodity endpoint to get the whole list). Defaults to all.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "commodity_group",
        action="split",
        help="commodity group(s) to include e.g. oil,coal,gas Defaults to all.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "commodity_grouping",
        type=str,
        help=COMMODITY_GROUPING_HELP,
        default=COMMODITY_GROUPING_DEFAULT,
        choices=COMMODITY_GROUPING_CHOICES,
    )
    parser.add_argument(
        "currency",
        action="split",
        help="currency(ies) of returned results e.g. EUR,USD,GBP",
        required=False,
        default=["EUR", "USD"],
    )
    parser.add_argument(
        "pricing_scenario",
        help="Pricing scenario (default or pricecap)",  # TODO Add list
        action="split",
        default=[PRICING_DEFAULT],
        required=False,
    )
    parser.add_argument(
        "nest_in_data",
        help="Whether to nest the json content in a data key.",
        type=inputs.boolean,
        default=True,
    )
    parser.add_argument(
        "sort_by",
        type=str,
        help="sorting results e.g. asc(commodity),desc(value_eur)",
        required=False,
        action="split",
        default=None,
    )
    parser.add_argument(
        "pivot_by",
        type=str,
        help="pivoting value_eur (or any other specified by pivot_value) by e.g. commodity_group.",
        required=False,
        action="split",
        default=None,
    )
    parser.add_argument(
        "pivot_value",
        action="split",
        help="pivoted value. Default: value_eur.",
        required=False,
        default="value_eur",
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
        "columns_order",
        action="split",
        help="order of columns. Don't need to specify all of them. Mainly useful for charts.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "keep_zeros",
        type=inputs.boolean,
        help="keep lines with zeros",
        required=False,
        default=True,
    )
    parser.add_argument(
        "postcompute",
        type=str,
        help="Post=compute function",
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
        "add_total_commodity",
        help="Whether to add a sum of all commodities",
        type=inputs.boolean,
        default=False,
    )

    parser.add_argument(
        "add_total_region",
        help="Whether to add a sum of all regions",
        type=inputs.boolean,
        default=False,
    )

    parser.add_argument("language", type=str, help="en or ua", default="en", required=False)

    parser.add_argument(
        "version",
        help="Which counter version to use. Only v2 (based on Kpler Trades) is available.",
        type=str,
        default=COUNTER_VERSION_DEFAULT,
    )

    parser.add_argument(
        "include_legacy_data",
        type=inputs.boolean,
        help="Whether to include legacy data which may not be up to date.",
        default=False,
    )

    @routes_api.expect(parser)
    def get(self):
        params = RussiaCounterResource.parser.parse_args()

        counter_version = params.get("version")
        if params.get("version") in ["v0", "v1"]:
            # Return a 401 response
            response_body = {
                "message": f"You cannot access counter {counter_version} as it has been removed."
            }
            return Response(
                response=json.dumps(response_body),
                status=401,
                mimetype="application/json",
            )

        return self.get_from_params(params)

    def get_from_params(self, params):
        format = params.get("format")
        cumulate = params.get("cumulate")
        rolling_days = params.get("rolling_days")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        aggregate_by = params.get("aggregate_by")
        destination_iso2 = params.get("destination_iso2")
        destination_region = params.get("destination_region")
        destination_region_not = params.get("destination_region_not")
        commodity = params.get("commodity")
        include_total_commodity = params.get("add_total_commodity")
        include_total_region = params.get("add_total_region")
        commodity_group = params.get("commodity_group")
        commodity_grouping = params.get("commodity_grouping")
        nest_in_data = params.get("nest_in_data")
        use_eu = params.get("use_eu")
        currency = params.get("currency")
        pricing_scenario = params.get("pricing_scenario")
        sort_by = params.get("sort_by")
        pivot_by = params.get("pivot_by")
        pivot_value = params.get("pivot_value")
        limit = params.get("limit")
        limit_by = params.get("limit_by")
        keep_zeros = params.get("keep_zeros")
        columns_order = params.get("columns_order")
        select = params.get("select")
        language = params.get("language")
        version = params.get("version")
        include_legacy_data = params.get("include_legacy_data")

        if aggregate_by and "" in aggregate_by:
            aggregate_by.remove("")

        destination_region_field = case(
            [
                (sa.and_(use_eu, Counter.destination_iso2 == "GB"), "United Kingdom"),
                (
                    sa.and_(
                        use_eu,
                        Country.region == "EU28",
                        Counter.destination_iso2 != "GB",
                    ),
                    "EU",
                ),
                (sa.and_(not use_eu, Counter.destination_iso2 == "GB"), "EU28"),
                (sa.and_(not use_eu, Country.region == "EU"), "EU28"),
                (Country.iso2 == sa.null(), "Others"),
            ],
            else_=Country.region,
        ).label("destination_region")

        destination_regions_field = Country.regions.label("destination_regions")
        destination_is_pcc_field = sa.case(
            [
                (
                    sa.and_(
                        destination_regions_field != sa.null(),
                        sa.func.array_to_string(destination_regions_field, ",").like("%PCC%"),
                    ),
                    "PCC",
                ),
            ],
            else_="NOT_PCC",
        ).label("destination_is_pcc")

        value_currency_field = (Counter.value_eur * Currency.per_eur).label("value_currency")

        commodity_subquery = get_commodity_subquery(
            session=session, grouping_name=commodity_grouping
        )

        query = (
            session.query(
                Counter.commodity,
                commodity_subquery.c.group.label("commodity_group"),
                commodity_subquery.c.group_name.label("commodity_group_name"),
                Counter.destination_iso2,
                Country.name.label("destination_country"),
                destination_region_field,
                destination_regions_field,
                destination_is_pcc_field,
                Country.regions,
                Counter.date,
                Counter.value_tonne,
                Counter.value_eur,
                Currency.currency,
                value_currency_field,
                Counter.pricing_scenario,
                PriceScenario.name.label("pricing_scenario_name"),
                Counter.version,
            )
            .outerjoin(commodity_subquery, Counter.commodity == commodity_subquery.c.id)
            .outerjoin(Country, Counter.destination_iso2 == Country.iso2)
            .outerjoin(Currency, Counter.date == Currency.date)
            .outerjoin(PriceScenario, Counter.pricing_scenario == PriceScenario.id)
            .filter(Counter.date >= to_datetime(date_from))
            .filter(Counter.version == version)
        )

        if pricing_scenario:
            query = query.filter(Counter.pricing_scenario.in_(to_list(pricing_scenario)))

        if destination_iso2:
            query = query.filter(Counter.destination_iso2.in_(to_list(destination_iso2)))

        if destination_region:
            query = query.filter(destination_region_field.in_(to_list(destination_region)))

        if destination_region_not:
            query = query.filter(destination_region_field.notin_(to_list(destination_region_not)))

        if commodity:
            query = query.filter(commodity_subquery.c.id.in_(to_list(commodity)))

        if commodity_group:
            query = query.filter(commodity_subquery.c.group.in_(to_list(commodity_group)))

        if currency is not None:
            query = query.filter(Currency.currency.in_(to_list(currency)))

        if date_to is not None:
            query = query.filter(Counter.date <= to_datetime(date_to))

        if not include_legacy_data:
            legacy_filter = Counter.commodity != "lpg"
            query = query.filter(legacy_filter)

        query = self.aggregate(query, aggregate_by)
        counter = pd.read_sql(query.statement, session.bind)

        if "id" in counter:
            counter.drop(["id"], axis=1, inplace=True)

        # Resample
        # Need to hash list columns before resampling
        counter, list_columns = hash_df(counter)

        if len(counter) == 0:
            return Response(
                response="No data available with given arguments.",
                status=204,
                mimetype="application/json",
            )

        if "date" in counter:
            daterange = pd.date_range(min(counter.date), max(counter.date)).rename("date")
            counter["date"] = pd.to_datetime(counter["date"]).dt.floor(
                "D"
            )  # Should have been done already
            cols = intersect(
                [
                    "commodity",
                    "commodity_group",
                    "commodity_group_name",
                    "destination_iso2",
                    "destination_country",
                    "destination_region",
                    "destination_is_pcc",
                    "currency",
                    "pricing_scenario",
                    "pricing_scenario_name",
                    "version",
                ],
                counter.columns,
            )

            counter = (
                counter.groupby(cols, dropna=False)
                .apply(
                    lambda x: x.set_index("date")
                    .resample("D")
                    .sum()
                    .reindex(daterange)  # .drop(cols, axis=1) \
                    .fillna(0)
                )
                .reset_index()
                .sort_values(intersect(["commodity", "date"], counter.columns))
            )

            counter["date"] = pd.to_datetime(counter.date.dt.date)

        if cumulate and "date" in counter:
            groupby_cols = [
                x
                for x in [
                    "commodity",
                    "commodity_group",
                    "commodity_group_name",
                    "destination_iso2",
                    "destination_country",
                    "destination_region",
                    "destination_regions",
                    "destination_is_pcc",
                    "currency",
                    "pricing_scenario",
                    "pricing_scenario_name",
                    "version",
                ]
                if aggregate_by is None or not aggregate_by or x in aggregate_by
            ]
            counter["value_eur"] = counter.groupby(groupby_cols, dropna=False)[
                "value_eur"
            ].transform(pd.Series.cumsum)
            counter["value_tonne"] = counter.groupby(groupby_cols, dropna=False)[
                "value_tonne"
            ].transform(pd.Series.cumsum)
            counter["value_currency"] = counter.groupby(groupby_cols, dropna=False)[
                "value_currency"
            ].transform(pd.Series.cumsum)

        if rolling_days is not None and rolling_days > 1:
            counter = (
                counter.groupby(
                    intersect(
                        [
                            "commodity",
                            "commodity_name",
                            "commodity_group",
                            "commodity_group_name",
                            "destination_iso2",
                            "destination_country",
                            "destination_region",
                            "destination_regions",
                            "destination_is_pcc",
                            "currency",
                            "pricing_scenario",
                            "pricing_scenario_name",
                            "version",
                        ],
                        counter.columns,
                    ),
                    dropna=False,
                )
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
                .replace({np.nan: None})
            )

        # Spread currencies
        counter = self.spread_currencies(result=counter)

        # Sort results
        counter = self.sort_result(result=counter, sort_by=sort_by, aggregate_by=aggregate_by)

        # Keep only n records
        counter = self.limit_result(
            result=counter,
            limit=limit,
            aggregate_by=aggregate_by,
            sort_by=sort_by,
            limit_by=limit_by,
            keep_zeros=keep_zeros,
        )

        def add_total_commodity(data):
            groupby_cols = [c for c in data.columns if not re.match("commodity|value", c)]
            value_cols = [c for c in data.columns if re.match("value", c)]
            data_global = data.groupby(groupby_cols, dropna=False)[value_cols].sum().reset_index()

            data_global["commodity_group"] = "Total"
            data_global["commodity"] = "Total"

            data = pd.concat([data, data_global])
            return data

        def add_total_region(data):
            groupby_cols = [c for c in data.columns if not re.match("destination|value", c)]
            value_cols = [c for c in data.columns if re.match("value", c)]
            data_global = data.groupby(groupby_cols, dropna=False)[value_cols].sum().reset_index()

            data_global["destination_region"] = "Total"

            data = pd.concat([data, data_global])
            return data

        if include_total_region:
            counter = add_total_region(counter)

        if include_total_commodity:
            counter = add_total_commodity(counter)
            if commodity:
                counter = counter[counter.commodity.isin(to_list(commodity) + ["Total"])]

        # Pivot
        counter = self.pivot_result(result=counter, pivot_by=pivot_by, pivot_value=pivot_value)

        # Sort columns
        counter = self.sort_columns(result=counter, columns_order=columns_order)

        # Post Compute
        counter = self.postcompute(result=counter, params=params)

        # Select, rename
        counter = self.select(counter, select=select)

        # Translate
        counter = self.translate(data=counter, language=language)

        # Unhash finally
        counter = unhash_df(counter, intersect(list_columns, counter.columns))

        if format == "csv":
            return Response(
                response=counter.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=counter.csv"},
            )

        if format == "json":
            return Response(
                response=df_to_json(counter, nest_in_data=nest_in_data),
                status=200,
                mimetype="application/json",
            )

    def aggregate(self, query, aggregate_by):
        """Perform aggregation based on user agparameters"""

        if not aggregate_by:
            return query

        subquery = query.subquery()

        # Aggregate
        value_cols = [
            func.sum(subquery.c.value_tonne).label("value_tonne"),
            func.sum(subquery.c.value_eur).label("value_eur"),
            func.sum(subquery.c.value_currency).label("value_currency"),
        ]

        optional_calculated_cols = {
            "month": [func.date_trunc("month", subquery.c.date).label("month")],
            "year": [func.date_trunc("year", subquery.c.date).label("year")],
        }

        # Adding must have grouping columns
        must_group_by = ["currency", "pricing_scenario", "version"]
        aggregate_by.extend([x for x in must_group_by if x not in aggregate_by])
        if "" in aggregate_by:
            aggregate_by.remove("")

        # Aggregating
        aggregateby_cols_dict = self.get_aggregateby_cols(subquery)

        # Update functional aggregate by options
        aggregateby_cols_dict.update(optional_calculated_cols)

        if any([x not in aggregateby_cols_dict for x in aggregate_by]):
            logger.warning(
                "aggregate_by can only be a selection of %s"
                % (",".join(aggregateby_cols_dict.keys()))
            )
            aggregate_by = [x for x in aggregate_by if x in aggregateby_cols_dict]

        groupby_cols = []
        for x in aggregate_by:
            groupby_cols.extend(aggregateby_cols_dict[x])

        query = session.query(*groupby_cols, *value_cols).group_by(*groupby_cols)
        return query

    def spread_currencies(self, result):
        len_before = len(result)
        n_currencies = len(result.currency.unique())

        result["currency"] = "value_" + result.currency.str.lower()
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
        result.replace({np.nan: None}, inplace=True)

        return result

    def sort_result(self, result, sort_by, aggregate_by):
        by = []
        ascending = []
        default_ascending = False

        if sort_by:
            for s in sort_by:
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
                dependencies = {
                    "commodity": ["commodity_group", "commodity_group_name"],
                    "commodity_group": ["commodity", "commodity_group_name"],
                    "commodity_group_name": ["commodity", "commodity_group"],
                }

                aggregate_by_dependencies = [
                    d for x in to_list(aggregate_by) for d in dependencies.get(x, [])
                ]

                sorting_groupers = [
                    x
                    for x in aggregate_by
                    if not x in aggregate_by_dependencies
                    and not x in ["date", "month", "year", "currency"]
                    and not x in by
                    and x in result.columns
                ]

            sorted = (
                result.groupby(sorting_groupers, dropna=False)[by]
                .sum()
                .reset_index()
                .sort_values(by=by, ascending=ascending)
                .drop(sort_by, axis=1)
            )

            result = pd.merge(sorted, result, how="left")

        return result

    def pivot_result(self, result, pivot_by, pivot_value):
        # Concatenate if there are several pivot_values (e.g. value_eur and value_tonne)
        if not pivot_by:
            return result

        pivot_values = to_list(pivot_value)
        if len(pivot_values) > 1:
            return pd.concat(
                [
                    self.pivot_result(result=result.copy(), pivot_by=pivot_by, pivot_value=x)
                    for x in pivot_values
                ]
            )
        else:
            pivot_value = pivot_values[0]

        dependencies = {
            "commodity": ["commodity_group", "commodity_group_name"],
            "commodity_group": ["commodity", "commodity_group_name"],
            "commodity_group_name": ["commodity", "commodity_group"],
            "destination_country": ["destination_iso2", "destination_region"],
            "destination_iso2": ["destination_country", "destination_region"],
            "pricing_scenario": ["pricing_scenario_name"],
            "pricing_scenario_name": ["pricing_scenario"],
        }

        if pivot_by:
            pivot_by_dependencies = [d for x in to_list(pivot_by) for d in dependencies.get(x, [])]
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
                fill_value=0,
            ).reset_index()

        return result

    def limit_result(self, result, limit, aggregate_by, sort_by, limit_by, keep_zeros):
        if not keep_zeros:
            result = result[(result.value_eur != 0) | (result.value_tonne != 0)]

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
            result.groupby(group_by, dropna=False)
            .agg({sort_by: "sum"})
            .reset_index()
            .sort_values(limit_by + to_list(sort_by), ascending=False)
        )

        if limit_by:
            top = top.groupby(limit_by, as_index=False, dropna=False)

        top = top.head(limit).drop(sort_by, axis=1)

        result = pd.merge(result, top, how="inner")

        return result

    def sort_columns(self, result, columns_order):
        if columns_order:
            # We keep all other columns
            cols = columns_order + [x for x in result.columns if x not in columns_order]
            result = result[cols]

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

    def postcompute(self, result, params=None):
        postcompute_fn = postcompute.get_postcompute_fn(params.get("postcompute"))
        if postcompute_fn:
            result = postcompute_fn(result, params=params)
        return result

    def translate(self, data, language):
        if language != "en":
            file_path = "assets/language/%s.json" % (language)
            with open(file_path, "r") as file:
                translate_dict = json.load(file)

            data = data.replace(translate_dict)
            data.columns = [translate_dict.get(x, x) for x in data.columns]

        return data
