import pandas as pd
import numpy as np
import json
import datetime as dt

from base.db import session, engine
from base.models import Counter, Port, Country, Berth, Commodity
from base.models import DB_TABLE_COUNTER
from base.utils import to_datetime
from base.logger import logger_slack
from base import PRICING_DEFAULT, PRICING_ENHANCED
from base.db_utils import upsert

import base
from engines import api_client


def update(date_from="2021-01-01", version=base.COUNTER_VERSION0, force=False):
    """
    Fill counter
    :return:
    """
    logger_slack.info(f"=== Counter update {version} ===")

    # Get pipeline flows
    pipelineflows = api_client.get_overland(
        date_from=date_from,
        commodity_origin_iso2=["RU"],
        aggregate_by=["commodity_origin_iso2", "commodity_destination_iso2", "commodity", "date"],
        nest_in_data=False,
        currency="EUR",
        pricing_scenario=[PRICING_DEFAULT, PRICING_ENHANCED],
        bypass_maintenance=True,
    )

    if version == base.COUNTER_VERSION0:
        # Version 0: MT voyages for everything

        # Get shipments
        # Very important: we aggregate by ARRIVAL_DATE for counter pricing.
        voyages = (
            api_client.get_voyages(
                date_from=date_from,
                commodity_origin_iso2=["RU"],
                aggregate_by=[
                    "commodity_origin_iso2",
                    "commodity_destination_iso2",
                    "commodity",
                    "arrival_date",
                    "status",
                ],
                currency="EUR",
                status="completed",
                pricing_scenario=[PRICING_DEFAULT, PRICING_ENHANCED],
                bypass_maintenance=True,
            )
            .loc[lambda df: df.commodity_origin_iso2 == "RU"]
            .loc[lambda df: df.commodity_destination_iso2 != "RU"]
            .loc[lambda df: df.status == base.COMPLETED]
            .rename(columns={"arrival_date": "date"})
        )

        result = pd.concat([pipelineflows, voyages])

    elif version == base.COUNTER_VERSION1:
        # Version 1: MT voyages for LPG only, Kpler flows for the rest
        voyages = (
            api_client.get_voyages(
                date_from=date_from,
                commodity_origin_iso2=["RU"],
                aggregate_by=[
                    "commodity_origin_iso2",
                    "commodity_destination_iso2",
                    "commodity",
                    "arrival_date",
                    "status",
                ],
                currency="EUR",
                status="completed",
                pricing_scenario=[PRICING_DEFAULT, PRICING_ENHANCED],
                bypass_maintenance=True,
                commodity=[base.LPG],
            )
            .loc[lambda df: df.commodity_origin_iso2 == "RU"]
            .loc[lambda df: df.commodity_destination_iso2 != "RU"]
            .loc[lambda df: df.status == base.COMPLETED]
            .rename(columns={"arrival_date": "date"})
        )
        assert np.all(voyages.commodity == base.LPG)

        # Add Kpler flows
        kpler_flows = (
            api_client.get_kpler_flows(
                date_from=date_from,
                origin_iso2=["RU"],
                origin_type=["country"],
                destination_type=["country"],
                aggregate_by=[
                    "commodity_origin_iso2",
                    "destination_iso2",
                    "commodity_equivalent",
                    "date",
                ],
                currency="EUR",
                pricing_scenario=[PRICING_DEFAULT, PRICING_ENHANCED],
            )
            .loc[lambda df: df.commodity_origin_iso2 == "RU"]
            .loc[lambda df: df.destination_iso2 != "RU"]
            .loc[lambda df: df.destination_iso2 != "not found"]
            .rename(
                columns={
                    "commodity_equivalent": "commodity",
                    "commodity_equivalent_group": "commodity_group",
                    "destination_region": "commodity_destination_region",
                    "destination_iso2": "commodity_destination_iso2",
                },
                inplace=True,
            )
        )

        kpler_flows
        result = pd.concat([pipelineflows, voyages, kpler_flows])

    elif version == base.COUNTER_VERSION2:
        # Version 1: MT voyages for LPG only, Kpler TRADES for the rest
        voyages = (
            api_client.get_voyages(
                params={
                    "date_from": date_from,
                    "commodity_origin_iso2": ["RU"],
                    "aggregate_by": [
                        "commodity_origin_iso2",
                        "commodity_destination_iso2",
                        "commodity",
                        "arrival_date",
                        "status",
                    ],
                    "currency": "EUR",
                    "status": "completed",
                    "pricing_scenario": [PRICING_DEFAULT, PRICING_ENHANCED],
                    "bypass_maintenance": True,
                    "commodity": [base.LPG],
                }
            )
            .loc[lambda df: df.commodity_origin_iso2 == "RU"]
            .loc[lambda df: df.commodity_destination_iso2 != "RU"]
            .loc[lambda df: df.status == base.COMPLETED]
            .rename(columns={"arrival_date": "date"})
        )
        assert np.all(voyages.commodity == base.LPG)

        # Add Kpler trades
        kpler_trades = (
            api_client.get_kpler_trades(
                params={
                    "format": "json",
                    "download": False,
                    "date_from": date_from,
                    "origin_iso2": ["RU"],
                    "aggregate_by": [
                        "commodity_origin_iso2",
                        "commodity_destination_iso2",
                        "commodity_equivalent",
                        "destination_date",
                    ],
                    "currency": "EUR",
                    "pricing_scenario": [PRICING_DEFAULT, PRICING_ENHANCED],
                }
            )
            .loc[lambda df: df.commodity_origin_iso2 == "RU"]
            .loc[lambda df: df.commodity_destination_iso2 != "RU"]
            .loc[lambda df: df.commodity_destination_iso2 != "not found"]
            .loc[lambda df: df.status == base.COMPLETED]
            .rename(
                columns={
                    "commodity_equivalent": "commodity",
                    "commodity_equivalent_group": "commodity_group",
                    "destination_date": "date",
                }
            )
        )
        result = pd.concat([pipelineflows, voyages, kpler_trades])

    else:
        raise ValueError(f"Unknown counter version {version}")

    # Aggregate
    # Fill missing dates so that we're sure we're erasing everything
    # But only within commodity, to keep the last date available
    # daterange = pd.date_range(date_from, dt.datetime.today()).rename("date")
    result = result.sort_values(["date", "commodity"])[
        [
            "commodity",
            "commodity_group",
            "commodity_destination_region",
            "commodity_destination_iso2",
            "date",
            "value_tonne",
            "value_eur",
            "pricing_scenario",
        ]
    ]
    # TODO Check why we have some na dates
    result = result[~pd.isna(result.date)]
    result["date"] = pd.to_datetime(result["date"]).dt.floor("D")  # Should have been done already
    result = (
        result.groupby(
            [
                "commodity",
                "commodity_group",
                "commodity_destination_iso2",
                "commodity_destination_region",
                "pricing_scenario",
            ],
            dropna=False,
        )
        .apply(lambda x: x.set_index("date").resample("D").sum(numeric_only=True).fillna(0))
        .reset_index()
    )

    result = result[~pd.isna(result.pricing_scenario)]

    # Progressively phase out pipeline_lng in n days
    result = remove_pipeline_lng(result)

    # Remove EU coal shipments following coal ban
    result = remove_coal_to_eu(result)

    # Remove Ligthuanian pipeline gas
    result = remove_pipeline_gas_lt(result)

    # Add version
    result["version"] = version

    # Progressively restore new EU oil pipeline that we missed before
    # result = resume_pipeline_oil_eu(result, n_days=3)

    # Progressively resume EU shipments that have been paused to reached 100bn
    # result = resume_eu_shipments(result, n_days=3)

    # Sanity check before updating counter
    ok, global_new, global_old, eu_new, eu_old = sanity_check(
        result.loc[result.pricing_scenario == PRICING_DEFAULT], version=version
    )

    if not ok and not force:
        logger_slack.error(
            f"[COUNTER NOT UPDATED] Failed for {version}: was EUR {global_old / 1e9}B -x> now EUR {global_new / 1e9}B. Please check."
        )
    else:
        forced = " - FORCED" if force else ""
        logger_slack.info(
            f"[COUNTER UPDATE{forced}] New global counter {version}: was EUR {global_old / 1e9}B (EU: EUR {eu_old / 1e9}B) -> now EUR {global_new / 1e9}B (EU: EUR {eu_new / 1e9}B)"
        )

        result.drop(["commodity_destination_region", "commodity_group"], axis=1, inplace=True)
        result.rename(columns={"commodity_destination_iso2": "destination_iso2"}, inplace=True)

        if True:
            # Erase and replace everything
            Counter.query.filter(Counter.version == version).delete()
            session.commit()
            result.to_sql(DB_TABLE_COUNTER, con=engine, if_exists="append", index=False)
            session.commit()
        else:
            # For manual purposes
            upsert(
                df=result[result.pricing_scenario == PRICING_DEFAULT],
                table=DB_TABLE_COUNTER,
                constraint_name="unique_counter",
            )


def sanity_check(result, version):
    ok = True
    missing_price = result.loc[
        (result.value_tonne > 0)
        & (result.value_eur <= 0)
        & (result.commodity != "bulk_not_coal")
        & (result.commodity != "general_cargo")
        & (pd.to_datetime(result.date) <= dt.datetime.now())
    ]

    if len(missing_price) > 0:
        logger_slack.error("Missing prices")
        ok = ok and False

    for_orders = result[(result.commodity_destination_iso2 == base.FOR_ORDERS)]
    if len(for_orders) > 0:
        logger_slack.error("Counter has for_orders")
        ok = ok and False

    if len(result[pd.isna(result.pricing_scenario)]) > 0:
        logger_slack.error("Missing pricing scenario")
        ok = ok and False

    coal_ban = result[
        (result.commodity_destination_region == "EU")
        & (
            result.commodity.isin(["coal_rail_road", "coke_rail_road"])
            & (result.date >= "2022-08-11")
        )
    ].value_tonne.sum()

    if coal_ban > 0:
        logger_slack.error("Counter has overland coal after august 10")
        ok = ok and False

    def get_comparison_df(compared_cols):
        old_data = pd.read_sql(
            session.query(
                Counter,
                Counter.destination_iso2.label("commodity_destination_iso2"),
                Country.region.label("commodity_destination_region"),
                Commodity.group.label("commodity_group"),
            )
            .outerjoin(Country, Country.iso2 == Counter.destination_iso2)
            .join(Commodity, Commodity.id == Counter.commodity)
            .filter(Counter.pricing_scenario == PRICING_DEFAULT)
            .filter(Counter.version == version)
            .statement,
            session.bind,
        )
        old = (
            old_data.loc[old_data.date >= "2022-02-24"]
            .loc[old_data.date <= pd.to_datetime(dt.date.today())]
            .groupby(compared_cols, dropna=False)
            .agg(old_eur=("value_eur", np.nansum))
            .replace(np.nan, 0)
        )

        new = (
            result.loc[result.date >= "2022-02-24"]
            .loc[result.date <= pd.to_datetime(dt.date.today())]
            .groupby(compared_cols, dropna=False)
            .agg(new_eur=("value_eur", np.nansum))
        )

        comparison = pd.merge(
            old, new, how="outer", left_on=compared_cols, right_on=compared_cols
        ).replace(np.nan, 0)

        comparison["ok"] = (
            (comparison.new_eur >= comparison.old_eur * 0.90)
            & (comparison.new_eur <= comparison.old_eur * 1.3)
        ) | ((comparison.new_eur - comparison.old_eur).abs() < 500e6)

        comparison = comparison.reset_index()
        return comparison

    comparison = get_comparison_df(
        compared_cols=["commodity_group", "commodity_destination_region"]
    )
    ok = ok and comparison.ok.all()

    logger_slack.info(
        comparison.reset_index()
        .rename(
            columns={
                "commodity_destination_region": "region",
                "commodity_group": "com.",
            }
        )
        .to_string(col_space=10, index=False, justify="left")
    )
    if not ok:
        # Print a more detailed version
        comparison_detailed = get_comparison_df(
            compared_cols=[
                "commodity_group",
                "commodity",
                "commodity_destination_iso2",
                "commodity_destination_region",
            ]
        )
        comparison_detailed = comparison_detailed.loc[~comparison_detailed.ok]
        logger_slack.info(
            comparison_detailed.reset_index()
            .rename(
                columns={
                    "commodity_destination_region": "region",
                    "commodity_group": "com.",
                }
            )
            .to_string(col_space=10, index=False, justify="left")
        )

        # Relax a bit for crude_oil in CL, MY, US, GH, MM
        comparison_detailed["ok"] = (
            (
                (comparison_detailed.new_eur >= comparison_detailed.old_eur * 0.9)
                & (comparison_detailed.new_eur <= comparison_detailed.old_eur * 1.3)
            )
            | ((comparison_detailed.new_eur - comparison_detailed.old_eur).abs() < 100e6)
            | (
                (comparison_detailed["commodity"] == "crude_oil")
                & (
                    comparison_detailed.commodity_destination_iso2.isin(
                        ["CL", "MY", "US", "GH", "MM"]
                    )
                )
            )
            | (
                # For na destinations, it is normal to swing more
                pd.isna(comparison_detailed.commodity_destination_iso2)
                & (
                    (comparison_detailed.new_eur >= comparison_detailed.old_eur * 0.5)
                    & (comparison_detailed.new_eur <= comparison_detailed.old_eur * 2)
                )
            )
        )
        ok = ok or comparison_detailed.ok.all()

    global_old = comparison.old_eur.sum()
    global_new = comparison.new_eur.sum()

    eu_old = comparison.loc[comparison.commodity_destination_region == "EU"].old_eur.sum()
    eu_new = comparison.loc[comparison.commodity_destination_region == "EU"].new_eur.sum()

    return ok, global_new, global_old, eu_new, eu_old


def remove_pipeline_lng(result, n_days=10, date_stop=dt.date(2022, 6, 6)):
    result.loc[
        (result.commodity == "lng_pipeline")
        & (pd.to_datetime(result.date) >= pd.to_datetime(date_stop)),
        ["value_eur", "value_tonne"],
    ] = 0
    result.loc[
        (result.commodity == "lng_pipeline")
        & (pd.to_datetime(result.date) <= pd.to_datetime(date_stop)),
        ["value_eur", "value_tonne"],
    ] *= max(0, 1 - 1 / n_days * (dt.date.today() - date_stop).days)
    return result


def remove_coal_to_eu(result, date_stop=dt.date(2022, 8, 11)):
    result.loc[
        (result.commodity_destination_region == "EU")
        & (result.commodity == "coal")
        & (pd.to_datetime(result.date) >= pd.to_datetime(date_stop)),
        ["value_eur", "value_tonne"],
    ] = 0

    return result


def remove_pipeline_gas_lt(result, date_stop=dt.date(2000, 1, 1)):
    """
    We assume all gas apparently consumed by Lithuania is actually gas
    directed towards Kaliningrad.

    #TODO improve as flows from LT to Kaliningrad should already
    have been considered in our ENTSOG model
    """
    result.loc[
        (result.commodity == base.PIPELINE_GAS)
        & (result.commodity_destination_iso2 == "LT")
        & (pd.to_datetime(result.date) >= pd.to_datetime(date_stop)),
        ["value_eur", "value_tonne"],
    ] = 0

    return result


def resume_pipeline_oil_eu(
    result,
    n_days=14,
    date_start_resuming=dt.date(2022, 10, 4),
    date_break=dt.date(2022, 9, 1),
):
    """
    We missed EU pipeline oil for a couple weeks but didn't want to restore it
    in one go just before the 100 bn counter. We're adding a slow catchup
    :param result:
    :param n_days:
    :param date_stop:
    :return:
    """
    result.loc[
        (result.commodity_destination_region == "EU")
        & (result.commodity == "pipeline_oil")
        & (pd.to_datetime(result.date) >= pd.to_datetime(date_break)),
        ["value_eur", "value_tonne"],
    ] *= min(1, max(0, (dt.date.today() - date_start_resuming).seconds / 3600 / 24 / n_days))

    return result


def resume_eu_shipments(
    result,
    n_days=10,
    date_start_resuming=dt.date(2022, 10, 4),
    date_break=dt.date(2022, 9, 26),
):
    """
    We missed EU pipeline oil for a couple weeks but didn't want to restore it
    in one go just before the 100 bn counter. We're adding a slow catchup
    :param result:
    :param n_days:
    :param date_stop:
    :return:
    """
    result.loc[
        (result.commodity_destination_region == "EU")
        & (result.commodity.isin(["lng", "crude_oil", "oil_products"]))
        & (pd.to_datetime(result.date) >= pd.to_datetime(date_break)),
        ["value_eur", "value_tonne"],
    ] *= min(1, max(0, (dt.date.today() - date_start_resuming).seconds / 3600 / 24 / n_days))

    return result


def add_estimates(result):
    """
    All the commoditie infos don't stop at the same date, especially
    ENTSOG vs shipments. Plus, the latest data might not be available.
    On top of this, there is a few days lag between last info and now,
    which must be filled to have the counter working.

    BUT we need to be smart enough so that the counter doesn't jump
    down or up everytime there is an update
    :return:
    """

    import datetime as dt

    daterange = pd.date_range(min(result.date), dt.datetime.today()).rename("date")

    def resample_and_fill(x):
        x = x.set_index("date").resample("D").sum(numeric_only=True).fillna(0)
        # cut 2 last days and take the 7-day mean
        means = x[["value_tonne", "value_eur"]].shift(2).tail(7).mean()
        x = x.reindex(daterange).fillna(means)
        return x

    # TODO Get previous estimate
    result_estimated = (
        result.groupby(["commodity", "destination_region"]).apply(resample_and_fill).reset_index()
    )

    m = pd.merge(result[["commodity", "date"]], result_estimated, how="outer", indicator=True)
    result_to_upload = m[m["_merge"] == "right_only"].drop("_merge", axis=1)
    result_to_upload["type"] = base.COUNTER_ESTIMATED
    result_to_upload.to_sql(DB_TABLE_COUNTER, con=engine, if_exists="append", index=False)
