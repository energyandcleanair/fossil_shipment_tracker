from engine import port
from engine import portcall
from engine import departure
from engine import arrival
from engine import flow
from engine import berth


# def test_port(test_db_empty):
#     port.fill()
#     berth.fill()
#     return
#
# def test_portcall(test_db):
#     portcall.fill(limit=20)
#     return
#
# def test_departure(test_db):
#     departure.update()
#     return
#
#
# def test_arrival(test_db):
#     arrival.update()
#     return


def test_flow(test_db):
    # flow.update()
    # flow.update_positions()
    berth.detect_arrival_berth()
    return