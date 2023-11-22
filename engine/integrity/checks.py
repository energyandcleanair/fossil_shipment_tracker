import sqlalchemy as sa
import pandas as pd

from base.db import session
from base.models import (
    ShipmentWithSTS,
    Shipment,
    ShipmentArrivalBerth,
    ShipmentDepartureBerth,
    Departure,
    Arrival,
)
from base.logger import logger_slack

from .check_kpler_trade import test_kpler_trades


def test_shipment_portcall_integrity():
    # check that shipments exist with some expected/hardcoded portcalls
    shipments = (
        session.query(Shipment.id)
        .join(Departure, Departure.id == Shipment.departure_id)
        .join(Arrival, Arrival.id == Shipment.arrival_id)
        .filter(
            sa.or_(
                sa.and_(Departure.portcall_id == 121521, Arrival.portcall_id == 129614),
                sa.and_(Departure.portcall_id == 121840, Arrival.portcall_id == 122068),
                sa.and_(Departure.portcall_id == 627232, Arrival.portcall_id == 643501),
                sa.and_(Departure.portcall_id == 143588, Arrival.portcall_id == 318245),
                sa.and_(Departure.portcall_id == 170033, Arrival.portcall_id == 497229),
            )
        )
        .all()
    )

    assert len(shipments) == 5


def test_shipment_table():
    # check that the shipment table respect unique departures and arrivals

    shipments = session.query(
        Shipment.id.label("shipment_id"),
        Shipment.arrival_id,
        Departure.id.label("departure_id"),
    ).join(Departure, Shipment.departure_id == Departure.id)

    arrivals, departures, shipment_ids = (
        [s.arrival_id for s in shipments if s.arrival_id is not None],
        [s.departure_id for s in shipments],
        [s.shipment_id for s in shipments.all()],
    )

    assert len(arrivals) == len(set(arrivals)) and len(departures) == len(set(departures))

    # check that no departure/arrival is references in STS shipments and non-STS shipments

    shipments_sts = session.query(
        ShipmentWithSTS.id.label("shipment_id"),
        ShipmentWithSTS.arrival_id,
        Departure.id.label("departure_id"),
    ).join(Departure, ShipmentWithSTS.departure_id == Departure.id)

    arrivals_sts, departures_sts, shipment_ids_sts = (
        [s.arrival_id for s in shipments_sts if s.arrival_id is not None],
        [s.departure_id for s in shipments_sts],
        [s.shipment_id for s in shipments_sts.all()],
    )

    assert not list(set(departures_sts) & set(departures)) and not list(
        set(arrivals_sts) & set(arrivals)
    )

    assert not (list(set(shipment_ids) & set(shipment_ids_sts)))


def test_berths():
    # make sure we respect that all shipments in departure and arrival berth have a matching shipment_id
    berths = session.query(ShipmentDepartureBerth.shipment_id).union(
        session.query(ShipmentArrivalBerth.shipment_id)
    )
    shipments = session.query(Shipment.id).union(session.query(ShipmentWithSTS.id))

    berth_shipment_ids, shipment_ids = [b.shipment_id for b in berths.all()], [
        s.id for s in shipments.all()
    ]

    assert len(set(berth_shipment_ids) & set(shipment_ids)) == len(berth_shipment_ids)


def test_portcall_relationship():
    # verify we have a 1:1 relationship with departures/arrivals and portcall
    # note - departure/arrivals can appear multiple times in the shipment with sts table, but only one portcall should
    # always be linked with departure/arrival

    non_sts_shipments = (
        session.query(
            Shipment.id,
            Departure.portcall_id.label("departure_portcall_id"),
            Arrival.portcall_id.label("arrival_portcall_id"),
        )
        .join(Departure, Departure.id == Shipment.departure_id)
        .join(Arrival, Arrival.id == Shipment.arrival_id)
    )

    departure_portcall_ids, arrival_portcall_ids = [
        d.departure_portcall_id for d in non_sts_shipments if d.departure_portcall_id is not None
    ], [a.arrival_portcall_id for a in non_sts_shipments if a.arrival_portcall_id is not None]

    assert len(departure_portcall_ids) == len(set(departure_portcall_ids)) and len(
        arrival_portcall_ids
    ) == len(set(arrival_portcall_ids))

    sts_shipments = (
        session.query(
            ShipmentWithSTS.id,
            Departure.portcall_id.label("departure_portcall_id"),
            Arrival.portcall_id.label("arrival_portcall_id"),
        )
        .join(Departure, Departure.id == ShipmentWithSTS.departure_id)
        .join(Arrival, Arrival.id == ShipmentWithSTS.arrival_id)
    )

    departure_portcall_ids_sts, arrival_portcall_ids_sts = [
        d.departure_portcall_id for d in sts_shipments if d.departure_portcall_id is not None
    ], [a.arrival_portcall_id for a in sts_shipments if a.arrival_portcall_id is not None]

    assert not len(set(departure_portcall_ids_sts) & set(departure_portcall_ids)) and not len(
        set(arrival_portcall_ids_sts) & set(arrival_portcall_ids)
    )


def test_insurer():
    """ " If scraping fails and our code doesn't detect it, it adds a false 'unknown'.
    We try to detect these by detecting patterns where an insurer was known, then unknown,
    and at a later date, then found again.
    Note that this will only work after we had a successful scraping
    """

    raw_sql = """
    WITH unknown
     AS (SELECT *
         FROM   ship_insurer
         WHERE  company_raw_name = 'unknown'),
     known
     AS (SELECT *
         FROM   ship_insurer
         WHERE  company_raw_name != 'unknown'),
     problematic
     AS (SELECT s.commodity,
                u.updated_on - u.date_from_equasis,
                u.*,
                k.date_from_equasis,
                k.updated_on
         FROM   unknown u
                LEFT JOIN known k
                       ON u.ship_imo = k.ship_imo
                LEFT JOIN ship s
                       ON s.imo = u.ship_imo
         WHERE  ( k.date_from_equasis < u.date_from_equasis
                   OR k.date_from_equasis IS NULL )
                AND ( k.updated_on > u.updated_on )
                AND u.updated_on - u.date_from_equasis < '100 days'
            )
        SELECT *
        FROM   problematic
        WHERE commodity != 'bulk' and commodity != 'general_cargo' and commodity != 'unknown';
    """

    result = session.execute(raw_sql)
    if result.rowcount > 0:
        missing_types = [row[0] for row in result]
        count = pd.Series(missing_types).value_counts()
        logger_slack.error(
            "There are ships marked with Unknown insurers that most likely shouldn't be:\n%s."
            % count.to_string()
        )
        logger_slack.info(f"Unknown insurers query:\n {raw_sql}")

    # Test that those will only one insurer have a null date_from
    raw_sql = """
        WITH count as (select ship_imo, min(date_from_equasis), count(*) from ship_insurer group by 1)
        SELECT * from count where count = 1 and min is not null
        """

    wrong_date_from = session.execute(raw_sql)
    # Count how many rows there are
    if wrong_date_from.rowcount > 0:
        logger_slack.error(
            "There are insurers with date_from_equasis = NULL even though these are the only ones identified: %s"
            % ", ".join([row[0] for row in wrong_date_from])
        )
    assert wrong_date_from.rowcount == 0


def test_trade_platform():
    """I've found trades with Crude/Co as a commodity
    but LNG as a platform. Just ensuring this isn't happening anymore
    """

    raw_sql = """
    select * from kpler_trade kt
    left join kpler_product kp on kp.id = kt.product_id
    where kp.platform != kt.platform;
    """

    result = session.execute(raw_sql)
    if result.rowcount > 0:
        logger_slack.error(
            "Some kpler trades have a platform field not matching the platform of their products."
        )
