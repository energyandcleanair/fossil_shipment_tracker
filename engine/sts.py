from base.db_utils import execute_statement
from base.utils import to_list, to_datetime
from base.logger import logger_slack

def update(date_from="2022-01-01", clean_up=True, rebuild=False):
    logger_slack.info("=== Event shipment update ===")

    if rebuild:
        with open('engine/event_shipment_rebuild.sql', 'r') as file:
            sql_rebuild = file.read()
    else:
        sql_rebuild = ''

    with open('engine/event_shipment_refresh.sql', 'r') as file:
        sql_refresh = file.read()
    sql_refresh = sql_refresh.replace("departure.date_utc >= '2021-11-01'",
                                      "date_utc >= '%s'" % (to_datetime(date_from).strftime('%Y-%m-%d')))

    if clean_up:
        with open('engine/event_shipment_clean_up.sql', 'r') as file:
            sql_content_cleanup = file.read()
        sql_refresh += sql_content_cleanup

    execute_statement(sql_rebuild+sql_refresh, print_result=True)