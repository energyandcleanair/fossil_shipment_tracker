from base.db_utils import execute_statement
from base.utils import to_list, to_datetime
from base.logger import logger_slack

def update(date_from="2022-01-01"):
    logger_slack.info("=== Event shipment update ===")

    with open('engine/event_shipment_refresh.sql', 'r') as file:
        sql_content = file.read()
    sql_content = sql_content.replace("departure.date_utc >= '2021-11-01'",
                                      "date_utc >= '%s'" % (to_datetime(date_from).strftime('%Y-%m-%d')))
    execute_statement(sql_content, print_result=True)