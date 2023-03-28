import datetime as dt
import pandas as pd
import re
import json
import numpy as np
from http import HTTPStatus
from flask import Response
from flask_restx import Resource, reqparse
import sqlalchemy as sa
from sqlalchemy.orm import aliased
from sqlalchemy import func, case, any_

from . import routes_api
from flask_restx import inputs

import base
from base.env import get_env
from base.models import PipelineFlow, Country, Commodity, Currency, Price, PriceScenario
from base.db import session
from base.encoder import JsonEncoder
from base.utils import to_list, to_datetime, to_bool
from base.logger import logger
from base import PRICING_DEFAULT

from .commodity import get_subquery as get_commodity_subquery, get_ids as get_commodity_ids


@routes_api.route(
    "/v0/overland",
    strict_slashes=False,
    doc={"description": "Retrieve pipeline and roal/raid flows of fossil fuels."},
)
class PipelineFlowResource(Resource):
    parser = reqparse.RequestParser()

    parser.add_argument(
        "bypass_maintenance",
        help="bypass maintenance when in maintenance",
        default=False,
        required=False,
        type=inputs.boolean,
    )

    # Query content
    parser.add_argument(
        "id",
        help="id(s) of pipelineflow. Default: returns all of them",
        default=None,
        action="split",
        required=False,
    )
    parser.add_argument(
        "commodity",
        help="commodity(ies) of interest. Default: returns all of them. Options: %s"
        % (",".join(get_commodity_ids(transport=[base.PIPELINE, base.RAIL_ROAD]))),
        default=None,
        action="split",
        required=False,
    )
    parser.add_argument(
        "date_from",
        help="start date (format 2020-01-15)",
        default="2022-01-01",
        required=False,
    )
    parser.add_argument(
        "date_to",
        type=str,
        help="end date (format 2020-01-15)",
        required=False,
        default=dt.datetime.today().strftime("%Y-%m-%d"),
    )
    parser.add_argument(
        "commodity_origin_iso2",
        action="split",
        help="iso2(s) of commodity origin",
        required=False,
        default=None,
    )
    parser.add_argument(
        "commodity_grouping",
        type=str,
        help="Grouping used (e.g. coal,oil,gas ('default') vs coal,oil,lng,pipeline_gas ('split_gas')",
        default="default",
    )
    parser.add_argument(
        "departure_iso2",
        action="split",
        help="iso2(s) of departure",
        required=False,
        default=None,
    )
    parser.add_argument(
        "destination_iso2",
        action="split",
        help="iso2(s) of destination",
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
        "currency",
        action="split",
        help="currency(ies) of returned results e.g. EUR,USD,GBP",
        required=False,
        default=["EUR", "USD"],
    )
    parser.add_argument(
        "pricing_scenario",
        help="Pricing scenario (standard or pricecap)",
        action="split",
        default=[PRICING_DEFAULT],
        required=False,
    )

    # Query processing
    parser.add_argument(
        "aggregate_by",
        type=str,
        action="split",
        default=None,
        help="which variables to aggregate by. Could be any of commodity, destination_country, destination_region",
    )
    parser.add_argument(
        "rolling_days",
        type=int,
        help="rolling average window (in days). Default: no rolling averaging",
        required=False,
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

    # Misc
    parser.add_argument(
        "sort_by",
        type=str,
        help="sorting results e.g. asc(commodity),desc(value_eur)",
        required=False,
        action="split",
        default=None,
    )
    parser.add_argument(
        "limit",
        type=int,
        help="how many result records do you want (default: keeps all)",
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

    @routes_api.expect(parser, validate=True)
    def get(self):
        params = PipelineFlowResource.parser.parse_args()
        return self.get_from_params(params)

    def get_maintenance_response(self, params):
        is_in_maintenance = to_bool(get_env("MAINTENANCE", False))
        bypass_maintenance = params.get("bypass_maintenance", False)
        if is_in_maintenance and not bypass_maintenance:
            return {"message": "API is currently in maintenance mode."}, 503
        else:
            return None

    def get_from_params(self, params):
        maintenance_resp = self.get_maintenance_response(params)
        if maintenance_resp:
            return maintenance_resp

        id = params.get("id")
        commodity = params.get("commodity")
        commodity_grouping = params.get("commodity_grouping")
        date_from = params.get("date_from")
        commodity_origin_iso2 = params.get("commodity_origin_iso2")
        departure_iso2 = params.get("departure_iso2")
        destination_iso2 = params.get("destination_iso2")
        destination_region = params.get("destination_region")
        date_to = params.get("date_to")
        aggregate_by = params.get("aggregate_by")
        format = params.get("format", "json")
        nest_in_data = params.get("nest_in_data")
        download = params.get("download")
        rolling_days = params.get("rolling_days")
        currency = params.get("currency")
        sort_by = params.get("sort_by")
        limit = params.get("limit")
        keep_zeros = params.get("keep_zeros")
        pricing_scenario = params.get("pricing_scenario")

        if aggregate_by and "" in aggregate_by:
            aggregate_by.remove("")

        value_eur_field = (PipelineFlow.value_tonne * Price.eur_per_tonne).label("value_eur")

        value_currency_field = (value_eur_field * Currency.per_eur).label("value_currency")

        commodity_subquery = get_commodity_subquery(
            session=session, grouping_name=commodity_grouping
        )

        CommodityOriginCountry = aliased(Country)
        CommodityDestinationCountry = aliased(Country)

        DepartureCountry = aliased(Country)
        DestinationCountry = aliased(Country)

        # commodity_origin_iso2_field = case(
        #     [(DepartureCountry.iso2.in_(['BY', 'TR']), 'RU'),
        #      # (DepartureCountry.iso2 == 'TR' and DestinationCountry.iso2 == 'GR', 'AZ'), # Kipoi
        #      # (DepartureCountry.iso2 == 'TR' and DestinationCountry.iso2 != 'GR', 'RU'),
        #      ],
        #     else_=DepartureCountry.iso2
        # ).label('commodity_origin_iso2')
        commodity_origin_iso2_field = DepartureCountry.iso2.label("commodity_origin_iso2")

        commodity_destination_iso2_field = DestinationCountry.iso2.label(
            "commodity_destination_iso2"
        )

        # Query with joined information
        flows_rich = (
            session.query(
                PipelineFlow.id,
                PipelineFlow.commodity,
                commodity_subquery.c.group.label("commodity_group"),
                commodity_subquery.c.group_name.label("commodity_group_name"),
                # Commodity origin and destination
                commodity_origin_iso2_field,
                CommodityOriginCountry.name.label("commodity_origin_country"),
                CommodityOriginCountry.region.label("commodity_origin_region"),
                commodity_destination_iso2_field,
                CommodityDestinationCountry.name.label("commodity_destination_country"),
                CommodityDestinationCountry.region.label("commodity_destination_region"),
                PipelineFlow.date,
                PipelineFlow.departure_iso2,
                DepartureCountry.name.label("departure_country"),
                DepartureCountry.region.label("departure_region"),
                PipelineFlow.destination_iso2,
                DestinationCountry.name.label("destination_country"),
                DestinationCountry.region.label("destination_region"),
                PipelineFlow.value_tonne,
                PipelineFlow.value_m3,
                value_eur_field,
                Currency.currency,
                value_currency_field,
                Price.scenario.label("pricing_scenario"),
                PriceScenario.name.label("pricing_scenario_name"),
            )
            .join(DepartureCountry, DepartureCountry.iso2 == PipelineFlow.departure_iso2)
            .outerjoin(
                DestinationCountry,
                PipelineFlow.destination_iso2 == DestinationCountry.iso2,
            )
            .outerjoin(
                CommodityOriginCountry,
                CommodityOriginCountry.iso2 == commodity_origin_iso2_field,
            )
            .outerjoin(
                CommodityDestinationCountry,
                CommodityDestinationCountry.iso2 == commodity_destination_iso2_field,
            )
            .outerjoin(commodity_subquery, PipelineFlow.commodity == commodity_subquery.c.id)
            .outerjoin(
                Price,
                sa.and_(
                    Price.date == PipelineFlow.date,
                    Price.commodity == commodity_subquery.c.pricing_commodity,
                    sa.or_(
                        commodity_destination_iso2_field == any_(Price.destination_iso2s),
                        Price.destination_iso2s == base.PRICE_NULLARRAY_CHAR,
                    ),
                    Price.departure_port_ids == base.PRICE_NULLARRAY_INT,
                    Price.ship_owner_iso2s == base.PRICE_NULLARRAY_CHAR,
                    Price.ship_insurer_iso2s == base.PRICE_NULLARRAY_CHAR,
                ),
            )
            .outerjoin(PriceScenario, PriceScenario.id == Price.scenario)
            .outerjoin(Currency, Currency.date == PipelineFlow.date)
            .filter(PipelineFlow.destination_iso2 != "RU")
            # Very important for pricing to have a distinct statement! And to be sorted prior that
            # so that we pick those with port ids matching, then destination iso2s, then ship etc.
            .order_by(
                PipelineFlow.id,
                Price.scenario,
                Currency.currency,
                Price.departure_port_ids,
                Price.destination_iso2s,
                Price.ship_insurer_iso2s,
                Price.ship_owner_iso2s,
            )
            .distinct(PipelineFlow.id, Price.scenario, Currency.currency)
        )

        # Return only >0 values. Otherwise we hit response size limit
        flows_rich = flows_rich.filter(PipelineFlow.value_tonne > 0)

        if id is not None:
            flows_rich = flows_rich.filter(PipelineFlow.id.in_(to_list(id)))

        if commodity is not None:
            flows_rich = flows_rich.filter(PipelineFlow.commodity.in_(to_list(commodity)))

        if date_from is not None:
            flows_rich = flows_rich.filter(PipelineFlow.date >= to_datetime(date_from))

        if date_to is not None:
            flows_rich = flows_rich.filter(PipelineFlow.date <= to_datetime(date_to))

        if commodity_origin_iso2 is not None:
            flows_rich = flows_rich.filter(
                commodity_origin_iso2_field.in_(to_list(commodity_origin_iso2))
            )

        if departure_iso2 is not None:
            flows_rich = flows_rich.filter(PipelineFlow.departure_iso2.in_(to_list(departure_iso2)))

        if destination_iso2 is not None:
            flows_rich = flows_rich.filter(
                PipelineFlow.destination_iso2.in_(to_list(destination_iso2))
            )

        if destination_region is not None:
            flows_rich = flows_rich.filter(
                DestinationCountry.region.in_(to_list(destination_region))
            )

        if currency is not None:
            flows_rich = flows_rich.filter(Currency.currency.in_(to_list(currency)))

        if pricing_scenario is not None:
            flows_rich = flows_rich.filter(Price.scenario.in_(to_list(pricing_scenario)))

        if not keep_zeros:
            flows_rich = flows_rich.filter(
                sa.or_(
                    sa.and_(
                        PipelineFlow.value_tonne != sa.null(),
                        PipelineFlow.value_tonne != 0,
                    ),
                    sa.and_(PipelineFlow.value_m3 != sa.null(), PipelineFlow.value_m3 != 0),
                )
            )

        # Aggregate
        query = self.aggregate(query=flows_rich, aggregate_by=aggregate_by)

        # Query
        result = pd.read_sql(query.statement, session.bind)

        if len(result) == 0:
            return Response(
                status=HTTPStatus.NO_CONTENT,
                response="empty",
                mimetype="application/json",
            )

        result = self.spread_currencies(result)

        # Sort results
        result = self.sort_result(result=result, sort_by=sort_by)

        # Keep only n records
        if limit:
            result = result[:limit]

        # Rolling average
        result = self.roll_average(
            result=result, aggregate_by=aggregate_by, rolling_days=rolling_days
        )
        response = self.build_response(
            result=result,
            format=format,
            nest_in_data=nest_in_data,
            aggregate_by=aggregate_by,
            download=download,
        )
        return response

    def aggregate(self, query, aggregate_by):
        """Perform aggregation based on user agparameters"""

        if not aggregate_by:
            return query

        subquery = query.subquery()

        # Aggregate
        value_cols = [
            func.sum(subquery.c.value_tonne).label("value_tonne"),
            func.sum(subquery.c.value_m3).label("value_m3"),
            func.sum(subquery.c.value_eur).label("value_eur"),
            func.sum(subquery.c.value_currency).label("value_currency"),
        ]

        # Adding must have grouping columns
        must_group_by = ["currency", "pricing_scenario"]
        aggregate_by.extend([x for x in must_group_by if x not in aggregate_by])
        if "" in aggregate_by:
            aggregate_by.remove("")
        # Aggregating
        aggregateby_cols_dict = {
            "pricing_scenario": [
                subquery.c.pricing_scenario,
                subquery.c.pricing_scenario_name,
            ],
            "currency": [subquery.c.currency],
            "commodity": [subquery.c.commodity, subquery.c.commodity_group],
            "commodity_group": [
                subquery.c.commodity_group,
                subquery.c.commodity_group_name,
            ],
            "date": [subquery.c.date],
            "commodity_origin_iso2": [
                subquery.c.commodity_origin_iso2,
                subquery.c.commodity_origin_country,
                subquery.c.commodity_origin_region,
            ],
            "commodity_destination_iso2": [
                subquery.c.commodity_destination_iso2,
                subquery.c.commodity_destination_country,
                subquery.c.commodity_destination_region,
            ],
            "departure_country": [
                subquery.c.departure_iso2,
                subquery.c.departure_country,
                subquery.c.departure_region,
            ],
            "departure_iso2": [
                subquery.c.departure_iso2,
                subquery.c.departure_country,
                subquery.c.departure_region,
            ],
            "destination_country": [
                subquery.c.destination_iso2,
                subquery.c.destination_country,
                subquery.c.destination_region,
            ],
            "destination_iso2": [
                subquery.c.destination_iso2,
                subquery.c.destination_country,
                subquery.c.destination_region,
            ],
            "destination_region": [subquery.c.destination_region],
        }

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

    def roll_average(self, result, aggregate_by, rolling_days):

        date_column = "date"

        if date_column in result.columns:
            result[date_column] = pd.to_datetime(result[date_column])

        if rolling_days is not None:
            min_date = result[date_column].min()
            max_date = result[date_column].max()  # change your date here
            daterange = pd.date_range(min_date, max_date).rename(date_column)

            result[date_column] = result[date_column].dt.floor("D")  # Should have been done already
            result = (
                result.groupby(
                    [
                        x
                        for x in result.columns
                        if x not in [date_column, "ship_dwt"] and not x.startswith("value_")
                    ]
                )
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

        # TODO: inherit template
        if date_column in result.columns:
            result[date_column] = result.date.dt.date
        return result

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

    def build_response(self, result, format, nest_in_data, aggregate_by, download):
        result.replace({np.nan: None}, inplace=True)

        # If bulk and departure berth is coal, replace commodity with coal
        if format == "csv":
            return Response(
                response=result.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=pipelineflows.csv"},
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

    def sort_result(self, result, sort_by):
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

            result.sort_values(by=by, ascending=ascending, inplace=True)

        return result
