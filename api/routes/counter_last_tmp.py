import json
import pandas as pd
import numpy as np
import datetime as dt
import pytz
import datetime as dt
from flask import Response
from flask_restx import Resource, reqparse, inputs
import pymongo
from sqlalchemy import func
import sqlalchemy as sa
from sqlalchemy.orm import aliased
from sqlalchemy import case

import base
from base import PRICING_DEFAULT
from . import routes_api
from .endpoint_cache import EndpointCacher
from base.encoder import JsonEncoder
from base.logger import logger
from base.db import session
from base.models import Country, PriceScenario
from base.models import Counter
from base.utils import to_datetime, to_list, intersect
from engine.commodity import get_subquery as get_commodity_subquery


@routes_api.route("/v0/counter_last_tmp", strict_slashes=False)
class RussiaCounterLastResource(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument(
        "destination_iso2",
        type=str,
        help="ISO2 of country of interest",
        required=False,
        default=None,
    )
    parser.add_argument(
        "destination_region",
        type=str,
        help="EU28,China etc.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "date_from",
        type=str,
        help="date at which counter should start",
        required=False,
        default="2022-02-24",
    )
    parser.add_argument(
        "date_to",
        type=str,
        help="date at which counter should stop",
        required=False,
        default=None,
    )
    parser.add_argument(
        "fill_with_estimates",
        type=inputs.boolean,
        help="whether or not to fill late days with estimates",
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
        "commodity_grouping",
        type=str,
        help="Grouping used (e.g. coal,oil,gas ('default') vs coal,oil,lng,pipeline_gas ('split_gas')",
        default="default",
    )
    parser.add_argument(
        "pricing_scenario",
        help="Pricing scenario (standard or pricecap)",
        action="split",
        default=[PRICING_DEFAULT],
        required=False,
    )
    parser.add_argument(
        "aggregate_by",
        help="aggregation e.g. commodity_group,destination_region",
        required=False,
        default=["commodity", "destination_region"],
        action="split",
    )
    parser.add_argument(
        "format",
        type=str,
        help="format of returned results (json or csv)",
        required=False,
        default="json",
    )

    @routes_api.expect(parser)
    def get(self):
        params = RussiaCounterLastResource.parser.parse_args()
        counter_last = self.get_from_params(params)
        return self.build_response(
            counter_last=counter_last, format=params.get("format")
        )

    def get_from_params(self, params):
        # original parameters (before any potential modifications)
        original_params = params.copy()

        destination_iso2 = params.get("destination_iso2")
        destination_region = params.get("destination_region")
        date_from = params.get("date_from")
        date_to = params.get("date_to")
        commodity_grouping = params.get("commodity_grouping")
        aggregate_by = params.get("aggregate_by")
        pricing_scenario = params.get("pricing_scenario")
        use_eu = params.get("use_eu")

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
            ],
            else_=Country.region,
        ).label("destination_region")

        commodity_subquery = get_commodity_subquery(
            session=session, grouping_name=commodity_grouping
        )

        query = (
            session.query(
                Counter.commodity,
                commodity_subquery.c.group.label("commodity_group"),
                Counter.destination_iso2,
                Country.name.label("destination_country"),
                destination_region_field,
                Counter.date,
                # func.sum(Counter.value_tonne).label("value_tonne"),
                # func.sum(Counter.value_eur).label("value_eur"),
                Counter.value_tonne,
                Counter.value_eur,
                Counter.pricing_scenario,
                PriceScenario.name.label("pricing_scenario_name"),
            )
            .join(commodity_subquery, Counter.commodity == commodity_subquery.c.id)
            .join(Country, Country.iso2 == Counter.destination_iso2)
            .outerjoin(PriceScenario, PriceScenario.id == Counter.pricing_scenario)
            # .group_by(
            #     Counter.commodity,
            #     Counter.destination_iso2,
            #     Country.name,
            #     destination_region_field,
            #     Counter.date,
            #     commodity_subquery.c.group,
            #     Counter.pricing_scenario,
            #     PriceScenario.name,
            # )
            .filter(Counter.pricing_scenario.in_(to_list(pricing_scenario)))
        )

        if destination_region:
            query = query.filter(
                destination_region_field.in_(to_list(destination_region))
            )

        if destination_iso2:
            query = query.filter(Counter.destination_iso2 == destination_iso2)

        if date_from:
            query = query.filter(Counter.date >= str(to_datetime(date_from)))

        if date_to:
            query = query.filter(Counter.date <= str(to_datetime(date_to)))

        # Important to force this
        # so that future flows (e.g. fixed pipeline) aren't included
        query = query.filter(Counter.date <= str(dt.date.today()))

        # Aggregate
        query = self.aggregate(query=query, aggregate_by=aggregate_by)

        counter = pd.read_sql(query.statement, session.bind)
        counter.replace({np.nan: None}, inplace=True)
        counter_last = self.get_last(counter=counter)

        # Add total
        group_by_total = ["pricing_scenario", "pricing_scenario_name"]
        index_total = [x for x in counter_last.index.names if x not in group_by_total]
        total = (
            counter_last.groupby(group_by_total, dropna=False)
            .sum(numeric_only=True)
            .reset_index()
        )
        total[index_total] = "total"
        total["date"] = counter_last.date.unique()

        counter_last = pd.concat([counter_last.reset_index(), total])

        # counter_last['date'] = dt.datetime.utcnow()
        counter_last["eur_per_sec"] = counter_last["eur_per_day"] / 24 / 3600

        if "index" in counter_last.columns:
            counter_last.drop(["index"], axis=1, inplace=True)

        return counter_last

    def build_response(self, counter_last, format):
        if format == "csv":
            return Response(
                response=counter_last.to_csv(index=False),
                mimetype="text/csv",
                headers={
                    "Content-disposition": "attachment; filename=counter_last.csv"
                },
            )

        if format == "json":
            return Response(
                response=json.dumps(
                    {"data": counter_last.to_dict(orient="records")}, cls=JsonEncoder
                ),
                status=200,
                mimetype="application/json",
            )

    def get_last(self, counter):
        groupby_cols = list(
            set(counter.select_dtypes(exclude=np.number).columns.tolist())
            - set(["date"])
        )
        counter_last = (
            counter.sort_values(["date"])
            .groupby(groupby_cols)
            .agg(
                total_tonne=pd.NamedAgg(column="value_tonne", aggfunc=np.sum),
                total_eur=pd.NamedAgg(column="value_eur", aggfunc=np.sum),
                date=pd.NamedAgg(column="date", aggfunc="last"),
            )
            .reset_index()
        )

        n_days = 7
        shift_days = 2
        daterange = pd.date_range(
            dt.datetime.today() - dt.timedelta(days=n_days + 1), dt.datetime.today()
        ).rename("date")

        def resample_and_fill(x):
            x = x.set_index("date").resample("D").sum(numeric_only=True).fillna(0)
            # cut 2 last days and take the 7-day mean
            # but only on last ten days to avoid old shipments (like US)
            means = (
                x.loc[
                    x.index
                    >= dt.datetime.today() - dt.timedelta(days=shift_days + n_days + 1)
                ][["value_tonne", "value_eur"]]
                .shift(shift_days)
                .tail(n_days)
                .mean()
                .fillna(0)
            )

            x = x.reindex(daterange).fillna(means)
            return x

        counter_last_increment = (
            counter.sort_values(["date"])
            .groupby(groupby_cols)
            .apply(resample_and_fill)
            .reset_index()
            .rename(
                columns={"value_tonne": "tonne_per_day", "value_eur": "eur_per_day"}
            )
            .drop(["date"], axis=1)
            .drop_duplicates()
        )

        counter_last_merged = counter_last.merge(
            counter_last_increment, how="left", on=groupby_cols
        )

        now = dt.datetime.utcnow()
        counter_last_merged["now"] = now
        counter_last_merged["total_eur"] = (
            counter_last_merged.total_eur
            + (now - counter_last_merged.date)
            / np.timedelta64(1, "D")
            * counter_last_merged.eur_per_day
        )

        if "commodity_group" in counter_last_merged.columns:
            counter_last_merged = counter_last_merged.loc[
                ~counter_last_merged.commodity_group.isna()
            ]

        counter_last_merged = counter_last_merged.groupby(groupby_cols).sum(
            numeric_only=True
        )
        counter_last_merged["date"] = now
        return counter_last_merged

    def aggregate(self, query, aggregate_by):
        """Perform aggregation based on user agparameters"""

        if not aggregate_by:
            return query

        subquery = query.subquery()

        # Aggregate
        value_cols = [
            func.sum(subquery.c.value_tonne).label("value_tonne"),
            func.sum(subquery.c.value_eur).label("value_eur"),
        ]

        # Adding must have grouping columns
        must_group_by = ["date", "pricing_scenario"]
        aggregate_by.extend([x for x in must_group_by if x not in aggregate_by])
        if "" in aggregate_by:
            aggregate_by.remove("")

        # Aggregating
        aggregateby_cols_dict = {
            "date": [subquery.c.date],
            "pricing_scenario": [
                subquery.c.pricing_scenario,
                subquery.c.pricing_scenario_name,
            ],
            "destination_iso2": [
                subquery.c.destination_iso2,
                subquery.c.destination_country,
                subquery.c.destination_region,
            ],
            "destination_country": [
                subquery.c.destination_iso2,
                subquery.c.destination_country,
                subquery.c.destination_region,
            ],
            "destination_region": [subquery.c.destination_region],
            "commodity_group": [subquery.c.commodity_group],
            "commodity": [subquery.c.commodity, subquery.c.commodity_group],
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

    def fix_post100bn(
        self,
        counter_last,
        datetime_100bn_utc=dt.datetime(2022, 10, 4, 5, 52),
        start_slope_utc=dt.datetime(2022, 10, 3, 21, 0),
    ):
        """
        TEMPORARILY set the time at which 100bn will be reached,
        and ensure cathing up afterwards
        :param counter_last:
        :param datetime_100bn_utc: date at which we want the counter to reach 100bn
        :param n_days: number of days to catch up
        :return:
        """

        if not "destination_region" in counter_last.columns:
            counter_last["eur_per_day"] *= (
                counter_last.eur_per_day.sum() + 1139818113
            ) / counter_last.eur_per_day.sum()
            return counter_last

        if (
            counter_last[counter_last.destination_region == "EU"].total_eur.sum()
            > 100e9
        ):
            idx_eu = counter_last.destination_region == "EU"
            if counter_last[idx_eu].eur_per_day.sum() == 0:
                counter_last[idx_eu & (counter_last.commodity == "crude_oil")][
                    "eur_per_day"
                ] = (13000 * 24 * 3600)
            return counter_last

        # TODO REMOVE
        df = counter_last.reset_index().copy()
        idx_eu = df.destination_region == "EU"

        if df[idx_eu].eur_per_day.sum() == 0:
            if "commodity" in df.columns:
                df.loc[idx_eu & (df.commodity == "crude_oil"), "eur_per_day"] = 1
            if "commodity_group" in df.columns:
                df.loc[idx_eu & (df.commodity_group == "oil"), "eur_per_day"] = 1

        df["old_total_eur"] = df.total_eur - (df.now - df.date).dt.days * df.eur_per_day

        df["new_eur_per_day"] = df.eur_per_day
        df.loc[idx_eu, "new_eur_per_day"] = (
            df[idx_eu]["eur_per_day"]
            * (100e9 - df[idx_eu].total_eur.sum())
            / df[idx_eu].eur_per_day.sum()
            / ((datetime_100bn_utc - start_slope_utc).seconds / 24 / 3600)
        )

        # assert np.all(counter_last['eur_per_day'] == df['eur_per_day'])

        counter_last["eur_per_day"] = df["new_eur_per_day"]
        counter_last.replace({np.nan: 0}, inplace=True)

        # fix the total
        counter_last["total_eur"] = np.where(
            counter_last["destination_region"] != "EU",
            counter_last["total_eur"],
            counter_last["total_eur"]
            + (
                ((dt.datetime.utcnow() - start_slope_utc).seconds / (24 * 3600))
                * counter_last["eur_per_day"]
            ),
        )

        # Fixed start
        # from csv: commodity x region
        # value_now_bn = 99.2
        # date_now = dt.datetime(2022, 10, 3 ,X,Y,Z)

        # Two goals, one constraint
        # Reach EU 100bn at datetime_100bn_utc
        # Tend towards the new counter value generally
        # Prevent a huge bump
        return counter_last
