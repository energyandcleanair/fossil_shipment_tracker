import pandas as pd
import geopandas as gpd

from base.logger import logger
from base.db import session, engine
from base.db_utils import upsert
from base.models import Counter, PipelineFlow, Price, Counter
from base.models import DB_TABLE_COUNTER
from api.routes.voyage import VoyageResource
from sqlalchemy import func
import sqlalchemy as sa
import json


def update():
    """
    Fill counter
    :return:
    """
    print("=== Counter update ===")
    date_from = "2022-01-01"

    # Get voyage
    pipelineflows = session.query(
        PipelineFlow.date,
        PipelineFlow.commodity,
        func.sum(PipelineFlow.value_tonne * Price.eur_per_tonne) \
            .label('value_eur')) \
        .join(Price, sa.and_(Price.date == PipelineFlow.date,
                                  Price.commodity == PipelineFlow.commodity)) \
        .filter(PipelineFlow.date >= date_from) \
        .group_by(PipelineFlow.commodity, PipelineFlow.date)
    pipelineflows = pd.read_sql(pipelineflows.statement, session.bind)
    pipelineflows["destination_region"] = "EU"

    # Get shipments
    params_voyage = {
        "format": "json",
        "download": False,
        "date_from": date_from,
        "aggregate_by": ["destination_region", "commodity", "departure_date"],
        "destination_region": "EU",
        "nest_in_data": False}
    voyages_resp = VoyageResource().get_from_params(params=params_voyage)
    voyages = json.loads(voyages_resp.response[0])
    voyages = pd.DataFrame(voyages)
    voyages.rename(columns={'departure_date': 'date'}, inplace=True)

    result = pd.concat([pipelineflows, voyages]) \
        .sort_values(['date', 'commodity']) \
        [["commodity", "destination_region", "date", "value_tonne", "value_eur"]]

    # Fill missing dates so that we're sure we're erasing everything
    # But only within commodity, to keep the last date available
    import datetime as dt
    # daterange = pd.date_range(date_from, dt.datetime.today()).rename("date")
    result["date"] = pd.to_datetime(result["date"]).dt.floor('D')  # Should have been done already
    result = result \
        .groupby(["commodity", "destination_region"]) \
        .apply(lambda x: x.set_index("date") \
               .resample("D").sum() \
               .fillna(0)) \
    .reset_index()

    result["type"] = "observed"

    # Erase and replace everything
    Counter.query.delete()
    session.commit()
    result.to_sql(DB_TABLE_COUNTER,
              con=engine,
              if_exists="append",
              index=False)
    session.commit()

    # Add estimates
    add_estimates(result)


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

    m = pd.merge(result[["commodity","date"]], result_estimated, how='outer', indicator=True)
    result_to_upload = m[m['_merge'] == 'right_only'].drop('_merge', axis=1)
    result_to_upload["type"] = "estimated"
    result_to_upload.to_sql(DB_TABLE_COUNTER,
                  con=engine,
                  if_exists="append",
                  index=False)