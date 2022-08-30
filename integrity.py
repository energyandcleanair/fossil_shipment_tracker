from base.db import session, check_if_table_exists
from base.models import Trajectory, ShipmentWithSTS, Shipment, ShipmentArrivalBerth, ShipmentDepartureBerth, Departure, Arrival
from api.tests import test_counter
from app import app
from base.logger import logger, logger_slack

def check():

    logger_slack.info("Checking integrity: shipment, portcall and berth relationship.")
    test_shipment_table()
    test_portcall_relationship()
    test_berths()

    logger_slack.info("Checking integrity: counter, voyage and pricing")
    test_counter.test_counter_against_voyage(app)
    test_counter.test_pricing_gt0(app)
def test_shipment_table():

    # check that the shipment table respect unique departures and arrivals

    shipments = session.query(Shipment.id.label("shipment_id"),
                              Shipment.arrival_id,
                              Departure.id.label("departure_id")) \
        .join(Departure, Shipment.departure_id == Departure.id)

    arrivals, departures, shipment_ids = [s.arrival_id for s in shipments if s.arrival_id is not None], [s.departure_id for s in shipments], [s.shipment_id for s in shipments.all()]

    assert len(arrivals) == len(set(arrivals)) and len(departures) == len(set(departures))

    # check that no departure/arrival is references in STS shipments and non-STS shipments

    shipments_sts = session.query(ShipmentWithSTS.id.label("shipment_id"),
                                  ShipmentWithSTS.arrival_id,
                                  Departure.id.label("departure_id")) \
        .join(Departure, ShipmentWithSTS.departure_id == Departure.id)

    arrivals_sts, departures_sts, shipment_ids_sts = [s.arrival_id for s in shipments_sts if s.arrival_id is not None], [s.departure_id for s in shipments_sts], [s.shipment_id for s in shipments_sts.all()]

    assert not list(set(departures_sts) & set(departures)) and not list(set(arrivals_sts) & set(arrivals))

    assert not (list(set(shipment_ids) & set(shipment_ids_sts)))

def test_berths():

    # make sure we respect that all shipments in departure and arrival berth have a matching shipment_id

    berths = session.query(ShipmentDepartureBerth.shipment_id).union(session.query(ShipmentArrivalBerth.shipment_id))
    shipments = session.query(Shipment.id).union(session.query(ShipmentWithSTS.id))

    berth_shipment_ids, shipment_ids = [b.shipment_id for b in berths.all()], [s.id for s in shipments.all()]

    assert len(set(berth_shipment_ids) & set(shipment_ids)) == len(berth_shipment_ids)

def test_portcall_relationship():

    # verify we have a 1:1 relationship with departures/arrivals and portcall
    # note - departure/arrivals can appear multiple times in the shipment with sts table, but only one portcall should
    # always be linked with departure/arrival

    non_sts_shipments = session.query(
        Shipment.id,
        Departure.portcall_id.label('departure_portcall_id'),
        Arrival.portcall_id.label('arrival_portcall_id')
    ) \
    .join(Departure, Departure.id == Shipment.departure_id) \
    .join(Arrival, Arrival.id == Shipment.arrival_id)

    departure_portcall_ids, arrival_portcall_ids = [d.departure_portcall_id for d in non_sts_shipments if d.departure_portcall_id is not None],  \
                                                   [a.arrival_portcall_id for a in non_sts_shipments if a.arrival_portcall_id is not None]

    assert len(departure_portcall_ids) == len(set(departure_portcall_ids)) and len(arrival_portcall_ids) == len(set(arrival_portcall_ids))

    sts_shipments = session.query(
        ShipmentWithSTS.id,
        Departure.portcall_id.label('departure_portcall_id'),
        Arrival.portcall_id.label('arrival_portcall_id')
    ) \
    .join(Departure, Departure.id == ShipmentWithSTS.departure_id) \
    .join(Arrival, Arrival.id == ShipmentWithSTS.arrival_id)

    departure_portcall_ids_sts, arrival_portcall_ids_sts = [d.departure_portcall_id for d in sts_shipments if d.departure_portcall_id is not None], \
                                                   [a.arrival_portcall_id for a in sts_shipments if a.arrival_portcall_id is not None]

    assert not len(set(departure_portcall_ids_sts) & set(departure_portcall_ids)) and not len(set(arrival_portcall_ids_sts) & set(arrival_portcall_ids))