import sqlalchemy as sa
import pandas as pd
import numpy as np

import base
from base.db import session
from base.models import (
    ShipmentWithSTS,
    Shipment,
    ShipmentArrivalBerth,
    ShipmentDepartureBerth,
    Departure,
    Arrival,
)
from api.tests import test_counter
from api.app import app
from base.logger import logger_slack, slacker, notify_engineers
from api.routes.voyage import VoyageResource
from api.routes.overland import PipelineFlowResource
from api.routes.counter_last import RussiaCounterLastResource


class IntegrityFailure:
    def __init__(self, message: str, info: object):
        self.message = message
        self.info = info

    def tostring(self):
        return f"{self.message}: {self.info}"


def check():
    failures = []

    logger_slack.info("Checking integrity: shipment, portcall and berth relationship.")
    failures += test_shipment_table()
    failures += test_shipment_portcall_integrity()
    failures += test_portcall_relationship()
    failures += test_berths()

    logger_slack.info("Checking integrity: counter, voyage and pricing")
    try:
        test_counter.test_counter_against_voyage(app)
        test_counter.test_pricing_gt0(app)
    except AssertionError as e:
        failures += IntegrityFailure(message="Counter tests failed", info={"error": e})

    logger_slack.info("Checking integrity: insurer data")
    failures += test_insurer()

    if len(failures) > 0:
        failure_info = "".join(
            map(lambda failure: f"\n - {failure.message}: {failure.info}", failures)
        )
        logger_slack.error(f"Failed integrity check: {failure_info}")
        notify_engineers("Please check error")


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

    if len(shipments) != 5:
        return [
            IntegrityFailure(
                "Expected 5 shipments but got a different number", {"n_shipments": len(shipments)}
            )
        ]
    else:
        return []


def test_shipment_table():
    failures = []

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

    if len(arrivals) != len(set(arrivals)):
        failures.append(
            IntegrityFailure(
                message="Duplicate arrivals found",
                info={"all_arrivals": len(arrivals), "arrivals_set": len(set(arrivals))},
            )
        )

    if len(departures) != len(set(departures)):
        failures.append(
            IntegrityFailure(
                message="Duplicate departures found",
                info={"all_departures": len(departures), "departures_set": len(set(departures))},
            )
        )

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

    if list(set(arrivals_sts) & set(arrivals)):
        failures.append(
            IntegrityFailure(
                message="Overlap in arrivals between arrivals and arrivals with STS",
                info={"overlap": len(list(set(arrivals_sts) & set(arrivals)))},
            )
        )

    if list(set(departures_sts) & set(departures)):
        failures.append(
            IntegrityFailure(
                message="Overlap in departures between departures and departures with STS",
                info={"overlap": len(list(set(departures_sts) & set(departures)))},
            )
        )

    if list(set(shipment_ids) & set(shipment_ids_sts)):
        failures.append(
            IntegrityFailure(
                message="Overlap in shipments between shipments and shipments with STS",
                info={"overlap": len(list(set(shipment_ids) & set(shipment_ids_sts)))},
            )
        )

    return failures


def test_berths():
    # make sure we respect that all shipments in departure and arrival berth have a matching shipment_id
    berths = session.query(ShipmentDepartureBerth.shipment_id).union(
        session.query(ShipmentArrivalBerth.shipment_id)
    )
    shipments = session.query(Shipment.id).union(session.query(ShipmentWithSTS.id))

    berth_shipment_ids, shipment_ids = [b.shipment_id for b in berths.all()], [
        s.id for s in shipments.all()
    ]

    if len(set(berth_shipment_ids) & set(shipment_ids)) != len(berth_shipment_ids):
        return [
            IntegrityFailure(
                message="Wrong number of departure/arrival berths found",
                info={
                    "set_of_all_births_and_shipments": len(
                        set(berth_shipment_ids) & set(shipment_ids)
                    ),
                    "all_berth_shipment_ids": len(berth_shipment_ids),
                },
            )
        ]
    else:
        return []


def test_portcall_relationship():
    # verify we have a 1:1 relationship with departures/arrivals and portcall
    # note - departure/arrivals can appear multiple times in the shipment with sts table, but only one portcall should
    # always be linked with departure/arrival

    failures = []

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

    if len(departure_portcall_ids) != len(set(departure_portcall_ids)):
        failures.append(
            IntegrityFailure(
                message="Duplicate departure portcalls found for non-STS shipments",
                info={
                    "n_duplicates": len(departure_portcall_ids) - len(set(departure_portcall_ids))
                },
            )
        )

    if len(arrival_portcall_ids) != len(set(arrival_portcall_ids)):
        failures.append(
            IntegrityFailure(
                message="Duplicate arrival portcalls found for non-STS shipments",
                info={"n_duplicates": len(arrival_portcall_ids) - len(set(arrival_portcall_ids))},
            )
        )

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

    if len(set(departure_portcall_ids_sts) & set(departure_portcall_ids)):
        failures.append(
            [
                IntegrityFailure(
                    message="Duplicate departure portcalls found for STS shipments",
                    info={
                        "n_duplicates": len(
                            set(departure_portcall_ids_sts) & set(departure_portcall_ids)
                        )
                    },
                )
            ]
        )

    if len(set(arrival_portcall_ids_sts) & set(arrival_portcall_ids)):
        failures.append(
            IntegrityFailure(
                message="Duplicate arrival portcalls found for STS shipments",
                info={
                    "n_duplicates": len(set(arrival_portcall_ids_sts) & set(arrival_portcall_ids))
                },
            )
        )

    return failures


def test_insurer():
    """ " If scraping fails and our code doesn't detect it, it adds a false 'unknown'.
    We try to detect these by detecting patterns where an insurer was known, then unknown,
    and at a later date, then found again.
    Note that this will only work after we had a successful scraping
    """

    failures = []

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
                u.updated_on - u.date_from,
                u.*,
                k.date_from,
                k.updated_on
         FROM   unknown u
                LEFT JOIN known k
                       ON u.ship_imo = k.ship_imo
                LEFT JOIN ship s
                       ON s.imo = u.ship_imo
         WHERE  ( k.date_from < u.date_from
                   OR k.date_from IS NULL )
                AND ( k.updated_on > u.updated_on )
                AND u.updated_on - u.date_from < '21 days'
            )
        SELECT *
        FROM   problematic
        WHERE commodity != 'bulk' and commodity != 'general_cargo' and commodity != 'unknown';
    """

    result = session.execute(raw_sql)
    if result.rowcount > 0:
        missing_types = [row[0] for row in result]
        counts = pd.Series(missing_types).value_counts()
        counts_info = ", ".join(pd.DataFrame(counts).apply(lambda a: f"{a.name}: {a[0]}", axis=1))
        failures.append(
            IntegrityFailure(
                message="There are ships marked with Unknown insurers that most likely shouldn't be",
                info={"count": counts_info},
            )
        )

    # Test that those will only one insurer have a null date_from
    raw_sql = """
        WITH count as (select ship_imo, min(date_from), count(*) from ship_insurer group by 1)
        SELECT * from count where count = 1 and min is not null
        """

    wrong_date_from = session.execute(raw_sql)
    # Count how many rows there are
    if wrong_date_from.rowcount > 0:
        failures.append(
            IntegrityFailure(
                message="There are insurers with date_from = NULL even though these are the only ones identified",
                info={"wrong_date_from": ", ".join([row[0] for row in wrong_date_from])},
            )
        )
    return failures


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
