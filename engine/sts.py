from base.db_utils import execute_statement
from base.utils import to_list, to_datetime
from base.logger import logger_slack

def update(date_from="2022-01-01", max_distance_between_ships = 5000, clean_up=True, rebuild=False):
    """
    Update Event_Shipment table

    Parameters
    ----------
    date_from : date to use to filter events from a specific time
    max_distance_between_ships : the maximum distance between meters the ships were at the [closest] time of the event
    clean_up : remove any rows with null event_id
    rebuild : rebuild table

    Returns
    -------

    """
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
    sql_refresh = sql_refresh.replace("(event.interacting_ship_details->>'distance_meters')::int < 5000",
                                      "(event.interacting_ship_details->>'distance_meters')::int < {}".format(max_distance_between_ships))

    if clean_up:
        with open('engine/event_shipment_clean_up.sql', 'r') as file:
            sql_content_cleanup = file.read()
        sql_refresh += sql_content_cleanup

    execute_statement(sql_rebuild+sql_refresh, print_result=True)