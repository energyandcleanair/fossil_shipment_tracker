import pandas as pd
import numpy as np
import json
import datetime as dt

from base.db import session, engine
from base.models import Counter, Port, Country, Berth, Commodity
from base.models import DB_TABLE_COUNTER
from base.utils import to_datetime
from base.logger import logger_slack

try:
    from api.routes.voyage import VoyageResource
    from api.routes.overland import PipelineFlowResource
except ImportError:
    from routes.voyage import VoyageResource
    from routes.overland import PipelineFlowResource

import base


def update(date_from='2021-11-01'):
    """
    Fill counter
    :return:
    """
    logger_slack.info("=== Counter update ===")
    # date_from = "2022-01-01"

    # Get pipeline flows
    params_pipelineflows = {
        "format": "json",
        "download": False,
        "date_from": date_from,
        "departure_iso2": ["RU", "BY", "TR"],
        "aggregate_by": ["departure_iso2", "destination_iso2", "commodity", "date"],
        "nest_in_data": False,
        "currency": "EUR"
    }
    pipelineflows_resp = PipelineFlowResource().get_from_params(params=params_pipelineflows)
    pipelineflows = json.loads(pipelineflows_resp.response[0])
    pipelineflows = pd.DataFrame(pipelineflows)

    # Bruegel: Finally, on Turkey, our assumption was to attribute:
    # • All of Kipi to Azerbaijan,
    # • All of Strandzha to Russia.
    # -> we remove TR -> GR
    # pipelineflows = remove_kipi_flows(pipelineflows, n_days=1)

    # Get shipments
    # Very important: we aggregate by ARRIVAL_DATE for counter pricing.
    params_voyage = {
        "format": "json",
        "download": False,
        "date_from": date_from,
        "departure_iso2": ['RU'],
        "aggregate_by": ['departure_iso2', "destination_iso2", "commodity", "arrival_date", "status"],
        "nest_in_data": False,
        "currency": 'EUR'
    }
    voyages_resp = VoyageResource().get_from_params(params=params_voyage)
    voyages = json.loads(voyages_resp.response[0])
    voyages = pd.DataFrame(voyages)
    voyages = voyages.loc[voyages.departure_iso2=='RU'] # Just to confirm
    voyages = voyages.loc[voyages.status==base.COMPLETED]
    voyages.rename(columns={'arrival_date': 'date'}, inplace=True)

    # Aggregate
    # Fill missing dates so that we're sure we're erasing everything
    # But only within commodity, to keep the last date available
    # daterange = pd.date_range(date_from, dt.datetime.today()).rename("date")
    result = pd.concat([pipelineflows, voyages]) \
        .sort_values(['date', 'commodity']) \
        [["commodity", 'commodity_group', 'destination_region', "destination_iso2", "date", "value_tonne", "value_eur"]]
    result["date"] = pd.to_datetime(result["date"]).dt.floor('D')  # Should have been done already
    result = result \
        .groupby(["commodity", 'commodity_group', "destination_iso2", 'destination_region']) \
        .apply(lambda x: x.set_index("date") \
               .resample("D").sum() \
               .fillna(0)) \
        .reset_index()

    result["type"] = base.COUNTER_OBSERVED

    # Progressively phase out pipeline_lng in n days
    result = remove_pipeline_lng(result)

    # Sanity check before updating counter
    ok, global_new, global_old = sanity_check(result)

    if not ok:
        logger_slack.error("[ERROR] New global counter: EUR %.1fB vs EUR %.1fB. Counter not updated. Please check." % (global_new / 1e9, global_old / 1e9))
    else:
        logger_slack.info("[COUNTER UPDATE] New global counter: EUR %.1fB vs EUR %.1fB." % (global_new / 1e9, global_old / 1e9))

        # Erase and replace everything
        result.drop(['destination_region', 'commodity_group'], axis=1, inplace=True)
        Counter.query.delete()
        session.commit()
        result.to_sql(DB_TABLE_COUNTER,
                  con=engine,
                  if_exists="append",
                  index=False)
        session.commit()

    # Add estimates
    # add_estimates(result)


def sanity_check(result):

    ok = True

    missing_price = result.loc[
        (result.value_tonne > 0) &
        (result.value_eur <= 0) &
        (result.commodity != 'bulk_not_coal') &
        (result.commodity != 'general_cargo') &
        (result.commodity != 'lpg') &
        (pd.to_datetime(result.date) <= dt.datetime.now())]

    if len(missing_price) > 0:
        logger_slack.error("Missing prices")
        ok = False

    def get_comparison_df(compared_cols):
        old_data = pd.read_sql(session.query(Counter, Country.region.label('destination_region'), Commodity.group.label('commodity_group')) \
                               .join(Country, Country.iso2 == Counter.destination_iso2) \
                               .join(Commodity, Commodity.id == Counter.commodity).statement,
                               session.bind)
        old = old_data \
            .loc[old_data.date >= '2022-02-24'] \
            .loc[old_data.date <= pd.to_datetime(dt.date.today())] \
            .groupby(compared_cols) \
            .agg(old_eur=('value_eur', np.nansum)) \
            .replace(np.nan, 0)

        new = result \
            .loc[result.date >= '2022-02-24'] \
            .loc[result.date <= pd.to_datetime(dt.date.today())] \
            .groupby(compared_cols) \
            .agg(new_eur=('value_eur', np.nansum))


        comparison = pd.merge(old, new,
                 how='outer',
                 left_on=compared_cols,
                 right_on=compared_cols) \
            .replace(np.nan, 0)

        comparison['ok'] = comparison.new_eur >= comparison.old_eur
        return comparison

    comparison = get_comparison_df(compared_cols=['commodity_group', 'destination_region'])
    ok = comparison.ok.all()

    logger_slack.info(comparison.reset_index() \
                      .rename(columns={'destination_region': 'region',
                                       'commodity_group': 'com.'}) \
                      .to_string(col_space=10, index=False,
                                 justify='left'))
    if not ok:
        # Print a more detailed version
        comparison_detailed = get_comparison_df(compared_cols=['commodity_group', 'destination_iso2'])
        logger_slack.info(comparison_detailed.reset_index() \
                          .rename(columns={'destination_region': 'region',
                                           'commodity_group': 'com.'}) \
                          .to_string(col_space=10, index=False,
                                     justify='left'))

    global_old = comparison.old_eur.sum()
    global_new = comparison.new_eur.sum()
    return ok, global_new, global_old


def remove_pipeline_lng(result, n_days=10,
                        date_stop=dt.date(2022, 6, 6)):
    result.loc[(result.commodity == 'lng_pipeline') & (pd.to_datetime(result.date) >= pd.to_datetime(date_stop)),
               ["value_eur", "value_tonne"]] = 0
    result.loc[(result.commodity == 'lng_pipeline') & (pd.to_datetime(result.date) <= pd.to_datetime(date_stop)),
               ["value_eur", "value_tonne"]] *= max(0, 1 - 1 / n_days * (dt.date.today() - date_stop).days)
    return result


# def remove_kipi_flows(pipelineflows,
#                       date_stop=dt.datetime(2022, 6, 16),
#                       n_days=10):
#     """
#         Assuming gas transiting from Turkey through Kipi point
#         is originating in Azerbaidjan.
#
#         n_days: number of days to phase it out to avoid jumps in counter
#         date_stop: date of immediate cut (everything before will be progressively removed,
#                                           everything after will be removed immediately)
#         :return:
#         """
#
#     idx = (pipelineflows.departure_iso2 == 'TR') & (pipelineflows.destination_iso2 == 'GR')
#
#     idx_after = idx & (pd.to_datetime(pipelineflows.date) >= date_stop)
#     idx_before = idx & (pd.to_datetime(pipelineflows.date) <= date_stop)
#
#     factor_after = 0
#     factor_before = max(0, 1 - (1 / n_days * (dt.date.today() - date_stop.date()).days))
#
#     pipelineflows.loc[idx_after, 'value_tonne'] = pipelineflows.loc[idx_after, 'value_tonne'] * factor_after
#     pipelineflows.loc[idx_after, 'value_m3'] = pipelineflows.loc[idx_after, 'value_m3'] * factor_after
#     pipelineflows.loc[idx_after, 'value_eur'] = pipelineflows.loc[idx_after, 'value_eur'] * factor_after
#
#     pipelineflows.loc[idx_before, 'value_tonne'] = pipelineflows.loc[idx_before, 'value_tonne'] * factor_before
#     pipelineflows.loc[idx_before, 'value_m3'] = pipelineflows.loc[idx_before, 'value_m3'] * factor_before
#     pipelineflows.loc[idx_before, 'value_eur'] = pipelineflows.loc[idx_before, 'value_eur'] * factor_before
#
#     return pipelineflows



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
        x = x.set_index("date") \
            .resample("D").sum() \
            .fillna(0)
        # cut 2 last days and take the 7-day mean
        means = x[["value_tonne", "value_eur"]].shift(2).tail(7).mean()
        x = x.reindex(daterange) \
            .fillna(means)
        return x

    # TODO Get previous estimate
    result_estimated = result \
        .groupby(["commodity", "destination_region"]) \
        .apply(resample_and_fill) \
        .reset_index()

    m = pd.merge(result[["commodity", "date"]], result_estimated, how='outer', indicator=True)
    result_to_upload = m[m['_merge'] == 'right_only'].drop('_merge', axis=1)
    result_to_upload["type"] = base.COUNTER_ESTIMATED
    result_to_upload.to_sql(DB_TABLE_COUNTER,
                  con=engine,
                  if_exists="append",
                  index=False)