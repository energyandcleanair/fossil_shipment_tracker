import tqdm
from geoalchemy2 import Geometry, func
import geopandas as gpd
import pandas as pd
import shapely
import sqlalchemy as sa
from sqlalchemy.orm import aliased
import datetime as dt
from fiona.drvsupport import supported_drivers

import base
from base.logger import logger, logger_slack
from base.db import session
from base.models import (
    ShipmentWithSTS,
    PortCall,
    Departure,
    ShipmentDepartureLocationSTS,
    ShipmentArrivalLocationSTS,
    Event,
    STSLocation,
    Arrival,
    Ship,
)
from base.models import (
    DB_TABLE_STS_LOCATIONS,
    DB_TABLE_STSDEPARTURELOCATION,
    DB_TABLE_STSARRIVALLOCATION,
)

from base.utils import update_geometry_from_wkb, to_list
from base.db_utils import upsert

from engines import portcall, mtevents


def update(date_from="2021-01-01"):
    """
    This function collects the before/after portcall for STS events so we can verify draught change

    :return:
    """

    logger_slack.info("=== Updating STS information ===")

    fill_portcalls_around_sts(date_from=date_from, go_backward=True)
    fill_portcalls_around_sts(date_from=date_from, go_backward=False)

    update_sts_locations()


def check_multi_stage_sts(date_from="2022-01-01", ship_imo=None):
    """
    This function checks existing sts shipments for further sts performed by the interacting ship to generate
    multistage sts shipments

    Returns
    -------

    """
    shipment_sts_departures = (
        session.query(
            ShipmentWithSTS,
            Event.date_utc.label("event_date_utc"),
            Event.id.label("event_id"),
            Departure.event_id.label("departure_event_id"),
            Departure.ship_imo,
        )
        .join(Departure, Departure.id == ShipmentWithSTS.departure_id)
        .join(Event, Event.id == Departure.event_id)
        .filter(Departure.event_id != sa.null())
    )

    if date_from:
        shipment_sts_departures = shipment_sts_departures.filter(Event.date_utc >= date_from)
    if ship_imo:
        shipment_sts_departures = shipment_sts_departures.filter(
            Departure.ship_imo.in_(to_list(ship_imo))
        )

    shipment_sts_departures = shipment_sts_departures.all()

    def check_events(ship_imo, date_from):
        # force check to see if we have next portcall to limit our event query later
        next_portcall = portcall.get_next_portcall(
            imo=ship_imo, date_from=date_from, arrival_or_departure=None, go_backward=False
        )

        # if we do not have a next portcall yet, we leave for now
        if not next_portcall:
            return

        # check if other events exist further in the journey until the next portcall
        mtevents.get_and_process_ship_events_between_dates(
            date_from=date_from,
            date_to=next_portcall.date_utc,
            ship_imo=ship_imo,
            force_rebuild=True,
            between_existing_only=True,
        )

        # collapse potentially found events
        unique_events = return_unique_events(
            date_from=date_from + dt.timedelta(minutes=1),
            date_to=next_portcall.date_utc - dt.timedelta(minutes=1),
            ship_imo=ship_imo,
            collapse_events=True,
        )

        if not unique_events:
            return

        for e in unique_events:
            check_events(ship_imo=e.interacting_ship_imo, date_from=e.date_utc)

    for shipment in tqdm.tqdm(shipment_sts_departures):
        check_events(ship_imo=shipment.ship_imo, date_from=shipment.event_date_utc)


def return_unique_events(
    date_from="2022-01-01", date_to=None, ship_imo=None, event_id=None, collapse_events=False
):
    """
    This function returns unique events based on our assumptions of STS interactions:
        - the commodities of hte two ships have to soft-match; see below
        - we must have confirmed distance at time of event between the two ships
        - if we collapse, we remove any events which point to the same next portcall (i.e. two same ships interact together multiple
        times before a portcall)

    Parameters
    ----------
    date_from : date from which to get events
    ship_imo : ship imo to filter for
    event_id : event id to filter for
    collapse_events : whether to collapse events based on next portcall
    """

    MainShip = aliased(Ship)
    IntShip = aliased(Ship)

    unique_events = (
        session.query(Event.id, Event.ship_imo, Event.interacting_ship_imo, Event.date_utc)
        .join(MainShip, MainShip.imo == Event.ship_imo)
        .join(IntShip, IntShip.imo == Event.interacting_ship_imo)
        .filter(
            Event.interacting_ship_details["distance_meters"] != sa.null(),
            sa.or_(
                MainShip.commodity == IntShip.commodity,
                sa.and_(
                    MainShip.commodity.in_([base.OIL_OR_CHEMICAL, base.OIL_PRODUCTS]),
                    IntShip.commodity.in_([base.OIL_OR_CHEMICAL, base.OIL_PRODUCTS]),
                ),
            ),
        )
    )

    if date_from:
        unique_events = unique_events.filter(Event.date_utc >= date_from)

    if date_to:
        unique_events = unique_events.filter(Event.date_utc <= date_to)

    if ship_imo:
        unique_events = unique_events.filter(MainShip.imo.in_(to_list(ship_imo)))

    if event_id:
        unique_events = unique_events.filter(Event.id.in_(to_list(event_id)))

    unique_events = unique_events.all()

    if collapse_events:
        # get the next portcall date for each event
        next_portcall = (
            session.query(Event.id, PortCall.date_utc.label("next_portcall_date_utc"))
            .outerjoin(PortCall, PortCall.ship_imo == Event.ship_imo)
            .filter(Event.id.in_([e.id for e in unique_events]))
            .filter(PortCall.date_utc > Event.date_utc)
            .order_by(Event.id, PortCall.date_utc.asc())
            .distinct(Event.id)
            .subquery()
        )

        unique_events = (
            session.query(
                Event.id,
                Event.ship_imo,
                Event.interacting_ship_imo,
                Event.date_utc,
                next_portcall.c.next_portcall_date_utc,
            )
            .join(next_portcall, next_portcall.c.id == Event.id)
            .order_by(
                Event.ship_imo,
                Event.interacting_ship_imo,
                next_portcall.c.next_portcall_date_utc.desc(),
            )
            .distinct(
                Event.ship_imo, Event.interacting_ship_imo, next_portcall.c.next_portcall_date_utc
            )
            .all()
        )

    return unique_events


def fill_portcalls_around_sts(
    date_from="2022-01-01",
    ship_imo=None,
    event_id=None,
    collapse_events=False,
    go_backward=True,
    for_departing=True,
    for_arriving=True,
):
    """
    The purpose of this function is to find the first preceeding and proceeding portcall for sts events

    :param date_from: Date from which to check events
    :param for_arriving: Whether to fill portcalls around arriving ship
    :param collapse_events: whether to collapse events between existing portcalls
    :param go_backward: whether to check portcall backwards as well
    :param for_departing: whether to fill portcalls around departing [interacting] ship
    :param event_id: event id to filter for
    :param ship_imo: ship imo to filter for
    :return:
    """

    unique_events = return_unique_events(
        date_from=date_from, ship_imo=ship_imo, event_id=event_id, collapse_events=collapse_events
    )

    for event in tqdm.tqdm(unique_events):
        if for_arriving:
            logger.info(
                "Finding portcalls for arriving ship_imo: {}, date_from: {}, go_backward: {}.".format(
                    event.ship_imo, event.date_utc, go_backward
                )
            )
            portcall.get_next_portcall(
                imo=event.ship_imo,
                date_from=event.date_utc,
                arrival_or_departure=None,
                go_backward=go_backward,
            )

        if for_departing:
            logger.info(
                "Finding portcalls for departing ship_imo: {}, date_from: {}, go_backward: {}.".format(
                    event.interacting_ship_imo, event.date_utc, go_backward
                )
            )
            portcall.get_next_portcall(
                imo=event.interacting_ship_imo,
                date_from=event.date_utc,
                arrival_or_departure=None,
                go_backward=go_backward,
            )


def update_sts_locations():
    """
    Update departure and arrival STS locations

    Returns
    -------

    """
    detect_sts_departure_location()
    detect_sts_arrival_location()


def generate_geojson():
    """
    Convert KML to geojson file for filling db

    Returns
    -------

    """

    supported_drivers["KML"] = "rw"
    # gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'

    sts_gdf = gpd.read_file("assets/sts_locations/STS Areas.kml", driver="KML")
    sts_gdf.insert(0, "id", range(0, len(sts_gdf)))

    sts_gdf.rename(columns={"Name": "name"}, inplace=True)

    sts_gdf.to_file("assets/sts_locations/sts_areas.geojson", driver="GeoJSON")


def fill():
    """
    Fill sts_locations data with prepared locations map

    Returns
    -------

    """

    sts_gdf = gpd.read_file("assets/sts_locations/sts_areas.geojson")

    sts_gdf = sts_gdf[["id", "name", "geometry"]]

    # Remove z dimension
    def remove_z(geom):
        return shapely.wkb.loads(shapely.wkb.dumps(geom, output_dimension=2))

    sts_gdf["geometry"] = sts_gdf.geometry.apply(remove_z)

    sts_df = pd.DataFrame(sts_gdf)
    sts_df = update_geometry_from_wkb(sts_df, to="wkt")

    upsert(
        df=sts_df,
        table=DB_TABLE_STS_LOCATIONS,
        constraint_name="sts_locations_pkey",
        dtype={"geometry": Geometry("GEOMETRY", 4326)},
    )
    return


def detect_sts_departure_location(shipment_id=None):
    """
    Find sts departure locations based on pre-defined areas we have created (i.e. Gibraltar...)

    Parameters
    ----------
    shipment_id :

    Returns
    -------

    """

    # Look for shipments to update
    shipments_to_update = session.query(ShipmentWithSTS.id).filter(
        ShipmentWithSTS.id.notin_(session.query(ShipmentDepartureLocationSTS.shipment_id))
    )

    if shipment_id is not None:
        shipment_id = to_list(shipment_id)
        shipments_to_update = shipments_to_update.filter(ShipmentWithSTS.id.in_(shipment_id))

    locations = (
        session.query(
            ShipmentWithSTS.id.label("shipment_id"),
            STSLocation.id.label("sts_location_id"),
            Event.id.label("event_id"),
        )
        .filter(ShipmentWithSTS.id.in_(shipments_to_update))
        .join(Departure, Departure.id == ShipmentWithSTS.departure_id)
        .filter(Departure.event_id != sa.null())
        .join(Event, Event.id == Departure.event_id)
        .join(STSLocation, func.ST_Contains(STSLocation.geometry, Event.ship_closest_position))
        .order_by(ShipmentWithSTS.id, STSLocation.id, Event.date_utc)
        .distinct(ShipmentWithSTS.id, STSLocation.id, Event.date_utc)
    )

    locations_df = pd.read_sql(locations.statement, session.bind)

    # Maximum one location per shipment - in the future we can relax this with meta levels (i.e. laconian gulf -> med)
    locations_count = locations_df.groupby(["shipment_id"])["sts_location_id"].count().reset_index()
    if locations_count.sts_location_id.max() > 1:
        logger.warning("Found more than one departure location for a shipment. Keeping latest one")
        locations_df = locations_df.sort_values(
            ["shipment_id", "sts_location_id"], ascending=True
        ).drop_duplicates(["shipment_id"], keep="last")

    locations_df["method_id"] = "simple_overlapping"
    locations_df = locations_df[["shipment_id", "sts_location_id", "event_id", "method_id"]]
    upsert(
        df=locations_df,
        table=DB_TABLE_STSDEPARTURELOCATION,
        constraint_name="unique_shipmentstsdeparturelocation",
    )
    return


def detect_sts_arrival_location(shipment_id=None):
    """

    Parameters
    ----------
    shipment_id :

    Returns
    -------

    """

    # Look for shipments to update
    shipments_to_update = session.query(ShipmentWithSTS.id).filter(
        ShipmentWithSTS.id.notin_(session.query(ShipmentArrivalLocationSTS.shipment_id))
    )

    if shipment_id is not None:
        shipment_id = to_list(shipment_id)
        shipments_to_update = shipments_to_update.filter(ShipmentWithSTS.id.in_(shipment_id))

    locations = (
        session.query(
            ShipmentWithSTS.id.label("shipment_id"),
            STSLocation.id.label("sts_location_id"),
            Event.id.label("event_id"),
        )
        .filter(ShipmentWithSTS.id.in_(shipments_to_update))
        .join(Arrival, Arrival.id == ShipmentWithSTS.arrival_id)
        .filter(Arrival.event_id != sa.null())
        .join(Event, Event.id == Arrival.event_id)
        .join(STSLocation, func.ST_Contains(STSLocation.geometry, Event.ship_closest_position))
        .order_by(ShipmentWithSTS.id, STSLocation.id, Event.date_utc)
        .distinct(ShipmentWithSTS.id, STSLocation.id, Event.date_utc)
    )

    locations_df = pd.read_sql(locations.statement, session.bind)

    # Maximum one location per shipment - in the future we can relax this with meta levels (i.e. laconian gulf -> med)
    locations_count = locations_df.groupby(["shipment_id"])["sts_location_id"].count().reset_index()
    if locations_count.sts_location_id.max() > 1:
        logger.warning("Found more than one departure location for a shipment. Keeping latest one")
        locations_df = locations_df.sort_values(
            ["shipment_id", "sts_location_id"], ascending=True
        ).drop_duplicates(["shipment_id"], keep="last")

    locations_df["method_id"] = "simple_overlapping"
    locations_df = locations_df[["shipment_id", "sts_location_id", "event_id", "method_id"]]
    upsert(
        df=locations_df,
        table=DB_TABLE_STSARRIVALLOCATION,
        constraint_name="unique_shipmentstsarrivallocation",
    )
    return
