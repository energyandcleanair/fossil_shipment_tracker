import pandas as pd
import numpy as np
import json
import datetime as dt

from base.db import session, engine
from base.models import Counter, Port, Country, Berth, Commodity
from base.models import DB_TABLE_COUNTER
from base.utils import to_datetime
from base.logger import logger_slack
from base import PRICING_DEFAULT, PRICING_PRICECAP
from base.db_utils import upsert

try:
    from api.routes.voyage import VoyageResource
    from api.routes.overland import PipelineFlowResource
except ImportError:
    from routes.voyage import VoyageResource
    from routes.overland import PipelineFlowResource

import base


def update(date_from='2021-01-01'):
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
        "commodity_origin_iso2": ["RU"],
        "aggregate_by": ["commodity_origin_iso2", "commodity_destination_iso2", "commodity", "date"],
        "nest_in_data": False,
        "currency": "EUR",
        "pricing_scenario": [PRICING_DEFAULT, PRICING_PRICECAP]
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
        "commodity_origin_iso2": ['RU'],
        "aggregate_by": ['commodity_origin_iso2', "commodity_destination_iso2", "commodity", "arrival_date", "status"],
        "nest_in_data": False,
        "currency": 'EUR',
        "pricing_scenario": [PRICING_DEFAULT, PRICING_PRICECAP]
    }
    voyages_resp = VoyageResource().get_from_params(params=params_voyage)
    voyages = json.loads(voyages_resp.response[0])
    voyages = pd.DataFrame(voyages)
    voyages = voyages.loc[voyages.commodity_origin_iso2 == 'RU'] # Just to confirm
    voyages = voyages.loc[voyages.status == base.COMPLETED]
    voyages.rename(columns={'arrival_date': 'date'}, inplace=True)

    # Aggregate
    # Fill missing dates so that we're sure we're erasing everything
    # But only within commodity, to keep the last date available
    # daterange = pd.date_range(date_from, dt.datetime.today()).rename("date")
    result = pd.concat([pipelineflows, voyages]) \
        .sort_values(['date', 'commodity']) \
        [["commodity", 'commodity_group', 'commodity_destination_region', "commodity_destination_iso2", "date", "value_tonne", "value_eur",
          "pricing_scenario"]]
    result["date"] = pd.to_datetime(result["date"]).dt.floor('D')  # Should have been done already
    result = result \
        .groupby(["commodity", 'commodity_group', "commodity_destination_iso2", 'commodity_destination_region', 'pricing_scenario']) \
        .apply(lambda x: x.set_index("date") \
               .resample("D").sum() \
               .fillna(0)) \
        .reset_index()

    result = result[~pd.isna(result.pricing_scenario)]

    # Progressively phase out pipeline_lng in n days
    result = remove_pipeline_lng(result)

    # Remove EU coal shipments following coal ban
    result = remove_coal_to_eu(result)

    # Remove new EU oil pipeline that we missed before
    # After 100bn release, we'll need to reinclude it progressively
    result = remove_pipeline_oil_eu(result)

    # Sanity check before updating counter
    ok, global_new, global_old, eu_new, eu_old = sanity_check(result.loc[result.pricing_scenario == PRICING_DEFAULT])

    if not ok:
        logger_slack.error("[ERROR] New global counter: EUR %.1fB vs EUR %.1fB. Counter not updated. Please check." % (global_new / 1e9, global_old / 1e9))
    else:
        logger_slack.info("[COUNTER UPDATE] New global counter: EUR %.1fB vs EUR %.1fB. (EU: EUR %.1fB vs EUR %.1fB)" %
                          (global_new / 1e9, global_old / 1e9, eu_new / 1e9, eu_old / 1e9))

        result.drop(['commodity_destination_region', 'commodity_group'], axis=1, inplace=True)
        result.rename(columns={'commodity_destination_iso2': 'destination_iso2'}, inplace=True)

        if True:
            # Erase and replace everything
            Counter.query.delete()
            session.commit()
            result.to_sql(DB_TABLE_COUNTER,
                      con=engine,
                      if_exists="append",
                      index=False)
            session.commit()
        else:
            # For manual purposes
            upsert(df=result[result.pricing_scenario == PRICING_PRICECAP],
                   table=DB_TABLE_COUNTER,
                   constraint_name="unique_counter")


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
        ok = ok and False

    for_orders = result[(result.commodity_destination_iso2 == base.FOR_ORDERS)]
    if len(for_orders) > 0:
        logger_slack.error("Counter has for_orders")
        ok = ok and False

    if len(result[pd.isna(result.pricing_scenario)]) > 0:
        logger_slack.error("Missing pricing scenario")
        ok = ok and False

    coal_ban = result[(result.commodity_destination_region == 'EU') & \
        (result.commodity.isin(['coal_rail_road', 'coke_rail_road']) &
         (result.date >= '2022-08-11'))].value_tonne.sum()

    if coal_ban > 0:
        logger_slack.error("Counter has overland coal after august 10")
        ok = ok and False

    def get_comparison_df(compared_cols):
        old_data = pd.read_sql(session.query(Counter,
                                             Counter.destination_iso2.label('commodity_destination_iso2'),
                                             Country.region.label('commodity_destination_region'),
                                             Commodity.group.label('commodity_group')) \
                               .join(Country, Country.iso2 == Counter.destination_iso2) \
                               .join(Commodity, Commodity.id == Counter.commodity) \
                               .filter(Counter.pricing_scenario == PRICING_DEFAULT).statement,
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

        comparison['ok'] = (comparison.new_eur >= comparison.old_eur * 0.95) \
                           & (comparison.new_eur <= comparison.old_eur * 1.1)
        comparison = comparison.reset_index()
        return comparison

    comparison = get_comparison_df(compared_cols=['commodity_group', 'commodity_destination_region'])
    ok = ok and comparison.ok.all()

    logger_slack.info(comparison.reset_index() \
                      .rename(columns={'commodity_destination_region': 'region',
                                       'commodity_group': 'com.'}) \
                      .to_string(col_space=10, index=False,
                                 justify='left'))
    if not ok:
        # Print a more detailed version
        comparison_detailed = get_comparison_df(compared_cols=['commodity_group', 'commodity', 'commodity_destination_iso2', 'commodity_destination_region'])
        comparison_detailed = comparison_detailed.loc[~comparison_detailed.ok]
        logger_slack.info(comparison_detailed.reset_index() \
                          .rename(columns={'commodity_destination_region': 'region',
                                           'commodity_group': 'com.'}) \
                          .to_string(col_space=10, index=False,
                                     justify='left'))

    global_old = comparison.old_eur.sum()
    global_new = comparison.new_eur.sum()

    eu_old = comparison.loc[comparison.commodity_destination_region == 'EU'].old_eur.sum()
    eu_new = comparison.loc[comparison.commodity_destination_region == 'EU'].new_eur.sum()

    return ok, global_new, global_old, eu_new, eu_old


def remove_pipeline_lng(result, n_days=10,
                        date_stop=dt.date(2022, 6, 6)):
    result.loc[(result.commodity == 'lng_pipeline') & (pd.to_datetime(result.date) >= pd.to_datetime(date_stop)),
               ["value_eur", "value_tonne"]] = 0
    result.loc[(result.commodity == 'lng_pipeline') & (pd.to_datetime(result.date) <= pd.to_datetime(date_stop)),
               ["value_eur", "value_tonne"]] *= max(0, 1 - 1 / n_days * (dt.date.today() - date_stop).days)
    return result


def remove_coal_to_eu(result, date_stop=dt.date(2022,8,11)):
    result.loc[(result.commodity_destination_region == 'EU') & (result.commodity == 'coal')
               & (pd.to_datetime(result.date) >= pd.to_datetime(date_stop)),
               ["value_eur", "value_tonne"]] = 0

    return result


def remove_pipeline_oil_eu(result, date_stop=dt.date(2022, 9, 1)):
    #TODO restore progressively
    result.loc[(result.commodity_destination_region == 'EU') & (result.commodity == 'pipeline_oil')
               & (pd.to_datetime(result.date) >= pd.to_datetime(date_stop)),
               ["value_eur", "value_tonne"]] = 0
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