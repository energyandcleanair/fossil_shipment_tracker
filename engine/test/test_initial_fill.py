from engine import port
from engine import portcall
from engine import departure

# def test_port(test_db_empty):
#     port.initial_fill()
#     return
#
# def test_portcall(test_db):
#     # port.initial_fill()
#     portcall.fill()
#     return

def test_departure(test_db):
    departure.update()
    return