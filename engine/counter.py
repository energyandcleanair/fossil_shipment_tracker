import pandas as pd
import geopandas as gpd

from base.logger import logger
from base.db import session
from base.db_utils import upsert
from base.models import Counter, PipelineFlow, Price
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

    date_from = "2022-02-24"

    # Get voyage
    pipelineflows = session.query(
        PipelineFlow.date,
        PipelineFlow.commodity,
        func.sum(PipelineFlow.value_tonne * Price.eur_per_tonne) \
            .label('value_eur')) \
        .outerjoin(Price, sa.and_(Price.date == PipelineFlow.date,
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
        [["commodity","destination_region","date","value_tonne","value_eur"]]

    # Fill missing dates so that we're sure we're erasing everything
    import datetime as dt
    daterange = pd.date_range(date_from, dt.datetime.today()).rename("date")
    result["date"] = pd.to_datetime(result["date"]).dt.floor('D')  # Should have been done already
    result = result \
        .groupby(["commodity","destination_region"]) \
        .apply(lambda x: x.set_index("date") \
               .resample("D").sum() \
               .reindex(daterange) \
               .fillna(0)) \
    .reset_index()

    upsert(result, DB_TABLE_COUNTER, constraint_name='unique_counter')
