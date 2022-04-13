import datetime as dt

import base
from base.db import session
from base.models import Ship, Departure, Flow, Position, Arrival
from engine.arrival import get_dangling_arrivals
from engine.departure import get_departures_without_flow
from engine import position
from tqdm import tqdm
from base.db import engine
from base.utils import to_list, to_datetime


def rebuild():
    print("=== Flow rebuild ===")
    with engine.connect() as con:
        con = con.execution_options(isolation_level="AUTOCOMMIT")
        with open('engine/flow_rebuild.sql', 'r') as file:
            sql_content1 = file.read()
        with open('engine/flow_refresh.sql', 'r') as file:
            sql_content2 = file.read()
        rs = con.execute(sql_content1 + sql_content2)
        for row in rs:
            print(row)


def update(date_from="2022-01-01"):
    print("=== Flow update ===")

    with engine.connect() as con:
        con = con.execution_options(isolation_level="AUTOCOMMIT")
        with open('engine/flow_refresh.sql', 'r') as file:
            sql_content = file.read()
        sql_content=sql_content.replace("date_utc >= '2022-01-01'",
                            "date_utc >= '%s'" % (to_datetime(date_from).strftime('%Y-%m-%d')))
        rs = con.execute(sql_content)
        for row in rs:
            print(row)




    # # Create a flow for each dangling departure
    # # and complete it for each arrival
    # dangling_departures = get_departures_without_flow(date_from=date_from)
    # dangling_arrivals = get_dangling_arrivals()
    #
    # arrival_departures = [x.departure_id for x in dangling_arrivals]
    # ongoing_departures = [x for x in dangling_departures
    #                         if x.id not in arrival_departures]
    #
    # for d in tqdm(dangling_arrivals):
    #     existing_flow = Flow.query.filter(Flow.departure_id == d.departure_id).first()
    #     if existing_flow:
    #         existing_flow.arrival_id = d.id
    #         existing_flow.status = base.COMPLETED
    #         session.commit()
    #     else:
    #         new_flow = Flow(**{
    #             "departure_id": d.departure_id,
    #             "arrival_id": d.id,
    #             "status": base.COMPLETED
    #         })
    #         session.add(new_flow)
    #         session.commit()
    #
    # for d in tqdm(ongoing_departures):
    #     new_flow = Flow(**{
    #         "departure_id": d.id,
    #         "arrival_id": None,
    #         "status": base.ONGOING
    #     })
    #     session.add(new_flow)
    # session.commit()

