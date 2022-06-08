import pandas as pd
import json
import datetime as dt

from base.db import session, engine
from base.models import Counter
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
        "nest_in_data": False}
    pipelineflows_resp = PipelineFlowResource().get_from_params(params=params_pipelineflows)
    pipelineflows = json.loads(pipelineflows_resp.response[0])
    pipelineflows = pd.DataFrame(pipelineflows)


    # Get shipments
    # Very important: we aggregate by ARRIVAL_DATE for counter pricing.
    params_voyage = {
        "format": "json",
        "download": False,
        "date_from": date_from,
        "aggregate_by": ["destination_iso2", "commodity", "arrival_date", "status"],
        "nest_in_data": False}
    voyages_resp = VoyageResource().get_from_params(params=params_voyage)
    voyages = json.loads(voyages_resp.response[0])
    voyages = pd.DataFrame(voyages)
    voyages = voyages.loc[voyages.status==base.COMPLETED]
    voyages.rename(columns={'arrival_date': 'date'}, inplace=True)

    result = pd.concat([pipelineflows, voyages]) \
        .sort_values(['date', 'commodity']) \
        [["commodity", "destination_iso2", "date", "value_tonne", "value_eur"]]

    # Fill missing dates so that we're sure we're erasing everything
    # But only within commodity, to keep the last date available
    # daterange = pd.date_range(date_from, dt.datetime.today()).rename("date")
    result["date"] = pd.to_datetime(result["date"]).dt.floor('D')  # Should have been done already
    result = result \
        .groupby(["commodity", "destination_iso2"]) \
        .apply(lambda x: x.set_index("date") \
               .resample("D").sum() \
               .fillna(0)) \
    .reset_index()

    result["type"] = base.COUNTER_OBSERVED


    # Progressively phase out pipeline_lng in n days
    n_days = 10
    date_stop = dt.date(2022, 6, 6)
    result.loc[(result.commodity=='lng_pipeline') & (pd.to_datetime(result.date) >= pd.to_datetime(date_stop)),
                                    ["value_eur", "value_tonne"]] = 0
    result.loc[(result.commodity == 'lng_pipeline') & (pd.to_datetime(result.date) <= pd.to_datetime(date_stop)),
               ["value_eur", "value_tonne"]] *= max(0, 1 - 1/n_days * (dt.date.today()-date_stop).days)


    # Some sanity checking before updating the counter
    old_data = pd.read_sql(Counter.query.statement, session.bind)
    global_old = old_data.loc[(old_data.date >= to_datetime('2022-02-24')) &
                                (old_data.date <= pd.to_datetime(dt.date.today()))] \
                    .value_eur.sum()

    global_new = result.loc[(result.date >= to_datetime('2022-02-24')) &
                            (result.date <= pd.to_datetime(dt.date.today()))] \
                    .value_eur.sum()

    de_old = old_data.loc[(old_data.date >= to_datetime('2022-02-24')) &
                          (old_data.date <= pd.to_datetime(dt.date.today())) &
                          (old_data.destination_iso2=='DE')] \
                    .value_eur.sum()

    de_new = result.loc[(result.date >= to_datetime('2022-02-24')) &
                        (result.date <= pd.to_datetime(dt.date.today())) &
                        (result.destination_iso2 == 'DE')] \
        .value_eur.sum()


    ok = (global_new >= global_old - 0.4e9) and (global_new < global_old + 2e9)
    ok = ok and (de_new >= de_old - 0.4e9) and (de_new < de_old + 2e9)

    if not ok:
        logger_slack.error("[ERROR] New global counter: EUR %.1fB vs EUR %.1fB. Counter not updated. Please check." % (global_new / 1e9, global_old / 1e9))
    else:
        logger_slack.info("[COUNTER UPDATE] New global counter: EUR %.1fB vs EUR %.1fB." % (global_new / 1e9, global_old / 1e9))

        # Erase and replace everything
        Counter.query.delete()
        session.commit()
        result.to_sql(DB_TABLE_COUNTER,
                  con=engine,
                  if_exists="append",
                  index=False)
        session.commit()

    # Add estimates
    # add_estimates(result)


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