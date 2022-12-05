from sqlalchemy.orm import load_only
from sqlalchemy.inspection import inspect
import json
import pandas as pd
import seaborn as sns
import tempfile

from engine import departure
from base.db_utils import execute_statement
from base.utils import to_list, to_datetime
from base.logger import logger_slack, slacker
from slack_sdk.errors import SlackApiError
from base.models import Shipment, ShipmentWithSTS
from base import PRICING_DEFAULT

def rebuild(date_from="2021-01-01"):
    logger_slack.info("=== Shipment rebuild ===")
    with open('engine/shipment_rebuild.sql', 'r') as file:
        sql_rebuild = file.read()

    execute_statement(sql_rebuild, print_result=True)

    departure.update()
    update(date_from=date_from)


def update(date_from="2021-01-01"):
    logger_slack.info("=== Shipment update ===")

    with open('engine/shipment_refresh_sts.sql', 'r') as file:
        sql_content = file.read()

    with open('engine/shipment_refresh.sql', 'r') as file:
        sql_content += file.read()

    sql_content = sql_content.replace("date_utc >= '2021-01-01'",
                                      "date_utc >= '%s'" % (to_datetime(date_from).strftime('%Y-%m-%d')))
    execute_statement(sql_content, print_result=True, slack_result=True)


    # # Create a shipment for each dangling departure
    # # and complete it for each arrival
    # dangling_departures = get_departures_without_shipment(date_from=date_from)
    # dangling_arrivals = get_dangling_arrivals()
    #
    # arrival_departures = [x.departure_id for x in dangling_arrivals]
    # ongoing_departures = [x for x in dangling_departures
    #                         if x.id not in arrival_departures]
    #
    # for d in tqdm(dangling_arrivals):
    #     existing_shipment = Shipment.query.filter(Shipment.departure_id == d.departure_id).first()
    #     if existing_shipment:
    #         existing_shipment.arrival_id = d.id
    #         existing_shipment.status = base.COMPLETED
    #         session.commit()
    #     else:
    #         new_shipment = Shipment(**{
    #             "departure_id": d.departure_id,
    #             "arrival_id": d.id,
    #             "status": base.COMPLETED
    #         })
    #         session.add(new_shipment)
    #         session.commit()
    #
    # for d in tqdm(ongoing_departures):
    #     new_shipment = Shipment(**{
    #         "departure_id": d.id,
    #         "arrival_id": None,
    #         "status": base.ONGOING
    #     })
    #     session.add(new_shipment)
    # session.commit()

def return_combined_shipments(session, columns=None):
    """
    Combine sts and non sts shipment tables and return the union subquery

    :param session:
    :param columns:
    :return:
    """
    if not columns:
        columns = [column.name for column in inspect(Shipment).c]

    non_sts_shipments = session.query(Shipment) \
        .options(load_only(*columns))

    sts_shipments = session.query(ShipmentWithSTS) \
        .options(load_only(*columns))

    return non_sts_shipments.union(sts_shipments).subquery()


def send_diagnostic_chart():
    from api.routes.voyage import VoyageResource

    params = {'aggregate_by': ['departure_date', 'status', 'commodity'],
              'commodity': ['crude_oil', 'oil_products', 'lng', 'coal'],
              # 'commodity_origin_iso2': 'RU',
              'departure_date_from': '2021-01-01',
              'rolling_days': 14,
              'currency': 'EUR',
              'pricing_scenario' : [PRICING_DEFAULT],
              'commodity_grouping': 'default'
              }

    v = VoyageResource().get_from_params(params=params)
    v = pd.DataFrame(json.loads(v.response[0]))

    v['value_tonne'] = pd.to_numeric(v.value_tonne)
    v['departure_date'] = pd.to_datetime(v.departure_date)

    shipment_diagnostic = sns.relplot(data=v,
                kind='line',
                x='departure_date',
                y='value_tonne',
                hue='status',
                errorbar=None,
                col="commodity",
                col_wrap=2,
                facet_kws={'sharey': False, 'sharex': True},
                height=8)

    shipment_diagnostic.figure.savefig('./misc/diagnostics/shipment_diagnostics.png')

    try:
        filepath = './misc/diagnostics/shipment_diagnostics.png'
        response = slacker.files_upload(channels='#log-russia-counter', file=filepath)
        assert response["file"]  # the uploaded file
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        assert e.response["ok"] is False
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
        logger_slack.error(f"Got an error: {e.response['error']}")


