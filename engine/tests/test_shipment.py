from engines.shipment import rebuild, update
from base.db import session, check_if_table_exists
from base.models import Shipment, Arrival, Departure, ShipmentWithSTS
import sqlalchemy as sa


def test_update():
    update()


def test_shipment_rebuild():
    """
    Test rebuilding shipment table
    """

    rebuild(date_from="2021-11-01")

    shipments = (
        session.query(Shipment.arrival_id, Departure.id)
        .join(Departure, Shipment.departure_id == Departure.id)
        .filter(Shipment.arrival_id != sa.null())
    )

    arrivals, departures = [s.arrival_id for s in shipments], [s.id for s in shipments]

    assert len(arrivals) == len(set(arrivals)) and len(departures) == len(set(departures))
