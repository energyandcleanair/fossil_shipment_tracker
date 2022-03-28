from engine import port
from engine import portcall
from engine import departure
from engine import arrival
from engine import flow

def test_port(test_db_empty):
    port.fill()
    return

# def test_portcall(test_db):
#     portcall.fill(limit=10)
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
#
#
# def test_flow(test_db):
#     flow.update()
#     return