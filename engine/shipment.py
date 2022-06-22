from base.db_utils import execute_statement
from base.utils import to_list, to_datetime
from base.logger import logger_slack


def rebuild(date_from="2022-01-01"):
    logger_slack.info("=== Shipment rebuild ===")
    with open('engine/shipment_rebuild.sql', 'r') as file:
        sql_content1 = file.read()
    with open('engine/shipment_refresh.sql', 'r') as file:
        sql_content2 = file.read()

    sql_content = sql_content1 + sql_content2
    sql_content = sql_content.replace("date_utc >= '2022-01-01'",
                                      "date_utc >= '%s'" % (to_datetime(date_from).strftime('%Y-%m-%d')))
    execute_statement(sql_content, print_result=True)


def update(date_from="2022-01-01"):
    logger_slack.info("=== Shipment update ===")

    with open('engine/shipment_refresh.sql', 'r') as file:
        sql_content = file.read()
    sql_content = sql_content.replace("date_utc >= '2021-11-01'",
                                      "date_utc >= '%s'" % (to_datetime(date_from).strftime('%Y-%m-%d')))
    execute_statement(sql_content, print_result=True)


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

