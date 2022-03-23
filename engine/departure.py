"This fills departure table using MarineTraffic PortCall data"
from base.db import session

from models import PortCall, Departure


def update_from_marinetraffic():

    # Look for dangling PortCalls
    subquery = session.query(Departure.portcall_id)
    dangling_portcalls = PortCall.query.filter(~PortCall.id.in_(subquery)).all()

    for portcall in dangling_portcalls:
        pass







def update_from_datalastic():
    pass