import sys

import sqlalchemy as sa
import pandas as pd
import numpy as np

import base
from base.db import session, check_if_table_exists
from base.models import (
    Trajectory,
    ShipmentWithSTS,
    Shipment,
    ShipmentArrivalBerth,
    ShipmentDepartureBerth,
    Departure,
    Arrival,
)
from api.tests import test_counter
from app import app
from base.logger import logger, logger_slack
from api.routes.voyage import VoyageResource
from api.routes.overland import PipelineFlowResource
from api.routes.counter_last import RussiaCounterLastResource
from api.routes.counter import RussiaCounterResource


def check():
    logger_slack.info("Checking integrity: shipment, portcall and berth relationship.")

    try:
        test_shipment_table()
        test_shipment_portcall_integrity()
        test_portcall_relationship()
        test_berths()
    except AssertionError:
        logger_slack.error(
            "Failed integrity: shipment, portcall and berth relationship."
        )
        raise

    try:
        logger_slack.info("Checking integrity: counter, voyage and pricing")
        test_counter.test_counter_against_voyage(app)
        test_counter.test_pricing_gt0(app)
    except AssertionError:
        logger_slack.error("Failed integrity: counter, voyage and pricing")
        raise


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


def test_counter_against_voyage():
    params = {
        "date_from": "2022-02-24",
        "format": "json",
        "pricing_scenario": "default",
    }

    response = RussiaCounterLastResource().get_from_params(params=params)
    assert response.status_code == 200
    data = response.json["data"]
    counter_df = pd.DataFrame(data).sort_values(["date"], ascending=False)

    params = {"date_from": "2022-02-24", "format": "json"}

    response = PipelineFlowResource().get_from_params(params=params)
    assert response.status_code == 200
    data = response.json["data"]
    pipeline_df = pd.DataFrame(data).sort_values(["date"], ascending=False)

    params = {
        "date_from": "2022-02-24",
        "commodity_grouping": "default",
        "currency": ["EUR"],
        "pricing_scenario": base.PRICING_DEFAULT,
        "format": "json",
    }

    response = VoyageResource().get_from_params(params=params)
    assert response.status_code == 200
    data = response.json["data"]
    voyage_df = pd.DataFrame(data)

    counter2 = (
        pd.concat(
            [
                voyage_df.loc[
                    (voyage_df.arrival_date_utc >= "2022-02-24")
                    & (voyage_df.departure_iso2 == "RU")
                ][["destination_region", "commodity_group", "value_eur"]],
                pipeline_df.loc[
                    (pipeline_df.date >= "2022-02-24")
                    & (pipeline_df.departure_iso2.isin(["TR", "RU", "BY"]))
                ][["destination_region", "commodity_group", "value_eur"]],
            ]
        )
        .groupby(["destination_region", "commodity_group"])
        .agg(value_eur=("value_eur", lambda x: np.nansum(x) / 1e9))
    )

    counter1 = counter_df.groupby(["destination_region", "commodity_group"]).agg(
        value_eur=("value_eur", lambda x: np.nansum(x) / 1e9)
    )

    assert counter1 == counter2


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

    assert len(arrivals) == len(set(arrivals)) and len(departures) == len(
        set(departures)
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
        d.departure_portcall_id
        for d in non_sts_shipments
        if d.departure_portcall_id is not None
    ], [
        a.arrival_portcall_id
        for a in non_sts_shipments
        if a.arrival_portcall_id is not None
    ]

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
        d.departure_portcall_id
        for d in sts_shipments
        if d.departure_portcall_id is not None
    ], [
        a.arrival_portcall_id
        for a in sts_shipments
        if a.arrival_portcall_id is not None
    ]

    assert not len(
        set(departure_portcall_ids_sts) & set(departure_portcall_ids)
    ) and not len(set(arrival_portcall_ids_sts) & set(arrival_portcall_ids))


def test_insurer():
    """ " If scraping fails and our code doesn't detect it, it adds a false 'unknown'.
    We try to detect these by detecting patterns where an insurer was known, then unknown,
    and at a later date, then found again.
    Note that this will only work after we had a successful scraping
    """

    raw_sql = """
    WITH count
     AS (SELECT ship_imo,
                Count(*) AS count
         FROM   ship_insurer
         GROUP  BY 1),
     max_updated_date_from_null
     AS (SELECT ship_imo,
                Max(updated_on) AS known_updated_on
         FROM   ship_insurer
         WHERE  date_from IS NULL
         GROUP  BY 1),
     date_from_unknown
     AS (SELECT ship_imo,
                Max(date_from)  AS unknown_date_from,
                Max(updated_on) AS unknown_updated_on
         FROM   ship_insurer
         WHERE  company_raw_name = 'unknown'
         GROUP  BY 1),
     problematic
     AS (SELECT count.*,
                m.known_updated_on,
                d.unknown_date_from,
                d.unknown_updated_on
         FROM   count
                LEFT JOIN max_updated_date_from_null m
                       ON count.ship_imo = m.ship_imo
                LEFT JOIN date_from_unknown d
                       ON count.ship_imo = d.ship_imo
         WHERE  count.count >= 2
                AND m.known_updated_on > d.unknown_date_from)
    SELECT *
    FROM   problematic
    """

    result = session.execute(raw_sql)
    if result.rowcount > 0:
        logger_slack.error(
            "There are ships marked with Unknown insurers that most likely shouldn't be: %s."
            % ", ".join([row[0] for row in result])
        )

    # Test that those will only one insurer have a null date_from
    raw_sql = """
        WITH count as (select ship_imo, min(date_from), count(*) from ship_insurer group by 1)
        SELECT * from count where count = 1 and min is not null
        """

    wrong_date_from = session.execute(raw_sql)
    # Count how many rows there are
    if wrong_date_from.rowcount > 0:
        logger_slack.error(
            "There are insurers with date_from = NULL even though these are the only ones identified: %s"
            % ", ".join([row[0] for row in wrong_date_from])
        )
    assert wrong_date_from.rowcount == 0