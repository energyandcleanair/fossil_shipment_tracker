from sqlalchemy.orm import load_only
from sqlalchemy.inspection import inspect
import json
import pandas as pd
import seaborn as sns
import tempfile
import os

from engines import fossil_tracker_api_client, departure
from base.db_utils import execute_statement
from base.utils import to_list, to_datetime
from base.logger import logger_slack, slacker
from slack_sdk.errors import SlackApiError
from base.models import Shipment, ShipmentWithSTS
from base import PRICING_DEFAULT


def rebuild(date_from="2021-01-01"):
    logger_slack.info("=== Shipment rebuild ===")
    with open("engines/shipment_rebuild.sql", "r") as file:
        sql_rebuild = file.read()

    execute_statement(sql_rebuild, print_result=True)

    departure.update()
    update(date_from=date_from)


def update(date_from="2021-01-01", skip_chart=False):
    logger_slack.info("=== Shipment update ===")

    with open("engines/shipment_refresh_sts.sql", "r") as file:
        sql_content = file.read()

    with open("engines/shipment_refresh.sql", "r") as file:
        sql_content += file.read()

    sql_content = sql_content.replace(
        "date_utc >= '2021-01-01'",
        "date_utc >= '%s'" % (to_datetime(date_from).strftime("%Y-%m-%d")),
    )
    execute_statement(sql_content, print_result=True, slack_result=True)

    if not skip_chart:
        send_diagnostic_chart()


def return_combined_shipments(session, columns=None):
    """
    Combine sts and non sts shipment tables and return the union subquery

    :param session:
    :param columns:
    :return:
    """
    if not columns:
        columns = [column.name for column in inspect(Shipment).c]

    non_sts_shipments = session.query(Shipment).options(load_only(*columns))

    sts_shipments = session.query(ShipmentWithSTS).options(load_only(*columns))

    return non_sts_shipments.union(sts_shipments).subquery()


def send_diagnostic_chart():

    params = {
        "aggregate_by": ["departure_date", "status", "commodity"],
        "commodity": ["crude_oil", "oil_products", "lng", "coal"],
        "departure_iso2": "RU",
        "departure_date_from": "2022-01-01",
        "rolling_days": 14,
        "currency": "EUR",
        "pricing_scenario": [PRICING_DEFAULT],
        "commodity_grouping": "default",
    }

    v = fossil_tracker_api_client.get_voyages(**params)

    v["value_tonne"] = pd.to_numeric(v.value_tonne)
    v["departure_date"] = pd.to_datetime(v.departure_date)

    shipment_diagnostic = sns.relplot(
        data=v,
        kind="line",
        x="departure_date",
        y="value_tonne",
        hue="status",
        errorbar=None,
        col="commodity",
        col_wrap=2,
        facet_kws={"sharey": False, "sharex": True},
        height=8,
    )

    folder = "misc/diagnostics/"
    if not os.path.exists(folder):
        os.makedirs(folder)
    shipment_diagnostic.figure.savefig(os.path.join(folder, "shipment_diagnostics.png"))

    try:
        filepath = "./misc/diagnostics/shipment_diagnostics.png"
        response = slacker.files_upload(channels="#log-russia-counter", file=filepath)
        assert response["file"]  # the uploaded file
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        assert e.response["ok"] is False
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
        logger_slack.error(f"Got an error: {e.response['error']}", stack_info=True, exc_info=True)
