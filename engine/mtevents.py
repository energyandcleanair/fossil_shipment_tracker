import datetime

import pandas as pd
import numpy as np
import sqlalchemy
from tqdm import tqdm
from shapely import wkb

import base
from base.logger import logger, logger_slack
from base.db import session
from base.db_utils import upsert
from base.db import check_if_table_exists
from base.models import DB_TABLE_MTEVENT_TYPE
from base.utils import distance_between_points, to_list, to_datetime

from engine.datalastic import Datalastic
from engine.marinetraffic import Marinetraffic
from engine.ship import fill

import datetime as dt

import re

from base.models import MarineTrafficEventType, Shipment, Departure, Ship, MarineTrafficCall, Port, Event, \
Arrival, Position
from sqlalchemy import func
import sqlalchemy as sa


def update(
        date_from="2022-01-01",
        date_to=dt.date.today() + dt.timedelta(days=1),
        ship_imo=None,
        commodities=[base.LNG,
                     base.CRUDE_OIL,
                     base.OIL_PRODUCTS,
                     base.OIL_OR_CHEMICAL],
        min_dwt=base.DWT_MIN,
        between_existing_only=False,
        between_shipments_only=False,
        use_cache=False,
        cache_objects=False,
        upload_unprocessed_events=True,
        limit=None):
    """
    This function retrieves the events for a specific ship imo

    Parameters
    ----------
    between_shipments_only : will only query events between existing shipments and ignore them otherwise
    between_existing_only : will only query between existing events
    date_from : date we want to query from
    date_to : date we want to query to
    ship_imo : the ship imo(s)s
    use_cache : whether we want to check cache
    cache_objects : whether we want to cache objects
    force_rebuild : force rebuild ignoring previous calls
    upload_unprocessed_events : whether to upload events where we failed to get some date
    limit : the limit of the number of ships we want to process

    Returns
    -------

    """
    logger_slack.info("=== Updating events for ships ===")

    ships = session.query(
        Departure.ship_imo.distinct().label("ship_imo"),
    ) \
        .filter(Departure.date_utc > date_from) \
        .join(Port, Port.id == Departure.port_id) \
        .join(Ship, Departure.ship_imo == Ship.imo)

    if ship_imo:
        ships = ships.filter(Ship.imo.in_(to_list(ship_imo)))
    if commodities:
        ships = ships.filter(Ship.commodity.in_(to_list(commodities)))
    if min_dwt:
        ships = ships.filter(Ship.dwt >= min_dwt)
    if limit:
        ships = ships.limit(limit)

    processed_ships = []

    logger.info("Will process {} ships.".format(len(ships.all())))

    date_to, date_from = to_datetime(date_to), to_datetime(date_from)

    for ship in tqdm(ships.all()):

        # convert SQLAlchemy.row object
        ship = ship._asdict()

        # get ship imo
        ship_imo = ship["ship_imo"]

        if ship_imo in processed_ships:
            continue

        # add ship to processed
        processed_ships.append(ship_imo)

        # dates to query
        date_bounds = []

        if not force_rebuild:

            # check whether we called this ship imo in the MTCall table and get latest date
            last_event_call = session.query(MarineTrafficCall) \
                .filter(MarineTrafficCall.method == base.VESSEL_EVENTS,
                        MarineTrafficCall.params['imo'].astext == ship_imo,
                        MarineTrafficCall.status == base.HTTP_OK) \
                .order_by(MarineTrafficCall.params['todate'].desc()) \
                .first()

            # if we did check this ship before and force rebuild is false, only query since last time
            if last_event_call and last_event_call is not None:
                date_from = to_datetime(last_event_call.params['todate']) + dt.timedelta(minutes=1)

            date_bounds = [(date_from, date_to)]

        if force_rebuild and not between_existing_only and not between_shipments_only:
            date_bounds = [(date_from, date_to)]

        if force_rebuild and between_existing_only and not between_shipments_only:
            event_calls = session.query(MarineTrafficCall) \
                .filter(MarineTrafficCall.method == base.VESSEL_EVENTS,
                        MarineTrafficCall.params['imo'].astext == ship_imo,
                        MarineTrafficCall.status == base.HTTP_OK) \
                .order_by(MarineTrafficCall.params['todate'].desc()) \
                .all()

            event_date_froms = [to_datetime(date_from)] + [x.date_utc + dt.timedelta(minutes=1) for x in event_calls]
            event_date_tos = [x.date_utc - dt.timedelta(minutes=1) for x in event_calls] + [to_datetime(date_to)]
            date_bounds = list(zip(event_date_froms, event_date_tos))

        if force_rebuild and between_shipments_only:
            # to reduce the amount of credits/unneeded events - we can query only between existing departures / arrivals
            # for shipments in our db - if we do not have an arrival, we use the next departure date for undeteced
            # arrival shipments, and otherwise todays date
            shipments = session.query(
                Shipment.id.label('shipment_id'),
                Departure.date_utc.label('departure_date_utc'),
                Arrival.date_utc.label('arrival_date_utc'),
                sa.func.lag(Departure.date_utc).over(Departure.ship_imo, order_by=Departure.date_utc.desc()).label(
                    'next_departure_date')
            ) \
                .join(Departure, Departure.id == Shipment.departure_id) \
                .outerjoin(Arrival, Arrival.id == Shipment.arrival_id) \
                .filter(Departure.ship_imo == ship_imo,
                        Departure.date_utc >= to_datetime(date_from),
                        Departure.date_utc <= to_datetime(date_to)) \
                .order_by(Departure.date_utc.asc()).subquery()

            shipment_dates = session.query(
                shipments.c.departure_date_utc.label('date_from'),
                sa.case(
                    [
                        (shipments.c.arrival_date_utc != sa.null(), shipments.c.arrival_date_utc),
                        (sa.and_(shipments.c.arrival_date_utc == sa.null(),
                                 shipments.c.next_departure_date != sa.null()), shipments.c.next_departure_date)
                    ], else_=datetime.date.today()
                ).label('date_to')
            )

            date_bounds = shipment_dates.all()

        # there is a max query date for marinetraffic, so we need to possibly break down dates to <180 day difference
        query_dates = []

        for dates in date_bounds:
            query_date_from = dates[0]
            query_date_to = dates[1]

            if query_date_to <= query_date_from:
                continue

            day_delta = (query_date_to - query_date_from).days

            if day_delta < 180:
                query_dates.append(dates)
                continue

            polling_limit_days = 179
            polling_limit = datetime.timedelta(polling_limit_days)

            for _d in range(0, int(day_delta / polling_limit_days)):
                query_dates.append([query_date_from, query_date_from + polling_limit])
                query_date_from = query_date_from + polling_limit + datetime.timedelta(minutes=1)

            query_dates.append([query_date_from, query_date_to])

        for dates in query_dates:
            query_date_from = dates[0]
            query_date_to = dates[1]

            if query_date_to <= query_date_from:
                continue

            events = Marinetraffic.get_ship_events_between_dates(
                imo=ship_imo,
                date_from=to_datetime(query_date_from),
                date_to=to_datetime(query_date_to),
                use_cache=use_cache,
                cache_objects=cache_objects
            )

            if not events:
                print("No vessel events found for ship_imo: {}.".format(ship_imo))
                continue

            event_process_state = [add_interacting_ship_details_to_event(e) for e in events]

            if event_process_state.count(False) > 0:
                print("Failed to process {} events out of {}".format(event_process_state.count(False),
                                                                     len(event_process_state)))

            # Store them in db so that we won't query them
            for event in events:

                if not upload_unprocessed_events and event.interacting_ship_imo is None:
                    continue

                try:
                    session.add(event)
                    session.commit()
                    if force_rebuild:
                        print("Found a missing event")
                except sqlalchemy.exc.IntegrityError as e:
                    if "psycopg2.errors.UniqueViolation" in str(e):
                        print("Failed to upload event: duplicated event")
                    else:
                        print("Failed to upload event: %s" % (str(e),))
                    session.rollback()
                    continue


def check_distance_between_ships(ship_one_imo, ship_two_imo, event_time, window_hours = 2, use_cache = True, cache_only= False):
    """
    Calculates the distance closest to the event time within a specific margin

    Parameters
    ----------
    ship_one_imo : ship imo
    ship_two_imo : ship imo
    event_time : time of the event

    Returns
    -------
    position of ship1, position of ship2, distance between them, time difference of the reporting time of the 2 positions

    """

    ship_position, intship_position = None, None

    # if we are using check db, look in the databse first
    if use_cache:
        ship_position = session.query(Position).filter(Position.ship_imo == ship_one_imo,
                                                       func.abs(func.extract('epoch', Position.date_utc - event_time)) <= (3600.*2)) \
                                                        .order_by(func.abs(func.extract('epoch', Position.date_utc - event_time)).desc()).first()
        intship_position = session.query(Position).filter(Position.ship_imo == ship_two_imo,
                                                       func.abs(func.extract('epoch', Position.date_utc - event_time)) <= (3600.*2)) \
                                                        .order_by(func.abs(func.extract('epoch', Position.date_utc - event_time)).desc()).first()

    if use_cache and cache_only and (ship_position is None or intship_position is None):
        return ship_position, intship_position, None, None

    # get closest position in time and add to event
    if ship_position is None and not cache_only:
        ship_position = Datalastic.get_position(imo=ship_one_imo, date=event_time, window=24)
    if intship_position is None and not cache_only:
        intship_position = Datalastic.get_position(imo=ship_two_imo, date=event_time, window=24)


    if ship_position is None and not cache_only:
        ship_position = Marinetraffic.get_closest_position(imo=ship_one_imo, date=event_time, window_hours=window_hours, interval_mins=5)
    if intship_position is None and ship_position is not None and not cache_only:
        intship_position = Marinetraffic.get_closest_position(imo=ship_two_imo, date=event_time, window_hours=window_hours, interval_mins=5)

    if ship_position is None or intship_position is None:
        print("Failed to find ship positions. try increasing time window...")
        return ship_position, intship_position, None, None

    ship_position_geom, intship_position_geom = ship_position.geometry, intship_position.geometry

    # TODO: is there a better way to handle the SRID section?
    d = distance_between_points(ship_position_geom, intship_position_geom)

    if d:
        print("Distance between ships was {} at {}".format(d, event_time))

    # we calculate the time difference at the point the position is taken with an extra 0.5 hour buffer
    time_difference = 0.5 + abs((ship_position.date_utc - intship_position.date_utc).total_seconds() / 3600.)

    return ship_position_geom, \
           intship_position_geom, \
           d, \
           time_difference


def find_ships_in_db(interacting_ship_name):
    ships = session.query(Ship) \
        .filter(sa.or_(Ship.name.any(interacting_ship_name),
                       Ship.name.any(func.lower(interacting_ship_name))),
                Ship.dwt > base.DWT_MIN,
                ~Ship.imo.contains('NOTFOUND')).all()

    return ships


def add_interacting_ship_details_to_event(event, distance_check=30000):
    """
    This function adds the interacting ship details to an mt event by:
        - finding the vessel using fuzzy search on datalastic
        - getting imo from MT if imo is missing from datalastic content
        - finding the closest position by time to the event that happened
        - checking the two ships where within distance_check at closest time position

    Parameters
    ----------
    event : Event object with data from MT
    distance_check : distance in meters that 2 ships have to be within each other to satisfy event condition

    Returns
    -------
    Bool : True if all conditions are successful and event was modified, or False if any failed

    """

    assert event.ship_imo is not None and event.content is not None

    ship_imo, event_content, ship_name, event_time = event.ship_imo, event.content, event.ship_name, event.date_utc

    intship_name = re.findall(r"\b([A-Z][A-Z\s]*[A-Z]|[A-Z])\b", event_content)

    if len(intship_name) != 1:
        print("Error in parsing ship name for event: {}".format(event_content))
        return False

    intship_name = intship_name[0]
    event.interacting_ship_name = intship_name

    print("{} vessel interacting with {}".format(ship_name, intship_name))

    int_ships = find_ships_in_db(intship_name)

    # first, try and find the ship in our database and check if position is satisfied
    if int_ships:
        for intship in int_ships:
            ship_position, intship_position, d, position_time_diff = check_distance_between_ships(ship_imo, intship.imo,
                                                                                                  event_time)

            # check if two ships are within a distance of each other based on avg speed and time difference of positions
            # we multiply by 2 in the max case of them moving opposite to each other
            if d is not None and d < (position_time_diff * base.AVG_TANKER_SPEED_KMH * 1000 * 2):
                event.interacting_ship_imo = intship.imo
                event.ship_closest_position = ship_position
                event.interacting_ship_closest_position = intship_position
                event.interacting_ship_details = {"distance_meters": int(d)}

                return True

    # since we did not find satisfactory ship in db, let's try datalastic

    int_ships = Datalastic.find_ship(intship_name, fuzzy=True, return_closest=5)

    if int_ships is None:

        # before returning false, let's try and add in imo locally
        int_ship_imo_local = find_ship_imo_locally(intship_name)
        if int_ship_imo_local is not None: event.interacting_ship_imo = int_ship_imo_local

        return False

    for intship in int_ships:
        if not intship:
            print("Error in finding ship in Datalastic for event: {}".format(event_content))
            continue

        # fill imo where necessary from MT
        if intship.imo is None:
            if intship.mmsi is not None:
                mt_intship_check = fill(mmsis=[intship.mmsi[-1]])

                if not mt_intship_check:
                    # add unknown ship to db, so we don't repeatedly query MT
                    unknown_ship = Ship(imo='NOTFOUND_' + intship.mmsi[-1], mmsi=intship.mmsi[-1], type=intship.type,
                                        name=intship.name[-1])
                    session.add(unknown_ship)
                    session.commit()

                    continue

                mt_ship = session.query(Ship).filter(Ship.mmsi.any(intship.mmsi[-1])).all()

                # check if we find more than 1 ship
                if len(mt_ship) > 1:
                    continue

                # if we don't find any in db by mmsi we failed to upload...
                if not mt_ship:
                    print("Failed to find imo in MT for event: {}".format(event_content))
                    continue

                mt_ship = mt_ship[0]

                if intship.name[-1] in mt_ship.name:
                    intship.imo = mt_ship.imo
                else:
                    print("Found match for ship with mmsi, but names do not match for event {}".format(event_content))
                    continue
            else:
                print("No ship imo found and we do not have an mmsi for event: {}".format(event_content))
                continue

        # check if interacting ship is in db
        found = fill(imos=[intship.imo])
        if not found:
            print("Failed to upload missing ships")
            continue

        # get closest position in time and add to event
        ship_position, intship_position, d, position_time_diff = check_distance_between_ships(ship_imo, intship.imo,
                                                                                              event_time)

        if d is not None and d < (position_time_diff * base.AVG_TANKER_SPEED_KMH * 2 * 1000):
            event.interacting_ship_imo = intship.imo
            event.ship_closest_position = ship_position
            event.interacting_ship_closest_position = intship_position
            event.interacting_ship_details = {"distance_meters": int(d)}
            return True

    # before returning false, let's try and add in imo locally
    int_ship_imo_local = find_ship_imo_locally(intship_name)
    if int_ship_imo_local is not None: event.interacting_ship_imo = int_ship_imo_local

    return False


def find_ship_imo_locally(ship_name):
    '''
    Finds the imo of a ship using its name either in previous events or in the database using exact match

    :param ship_name:
    :return:
    '''
    int_ship_found = session.query(
        Event
    ) \
        .filter(Event.interacting_ship_name == ship_name) \
        .filter(Event.interacting_ship_imo != sa.null()).first()

    if int_ship_found is not None:
        logger.info("Found match for ship name in previous events.")
        return int_ship_found.interacting_ship_imo

    else:
        # take exact match in db
        int_ships = find_ships_in_db(ship_name)
        if len(int_ships) > 0:
            logger.info("Found match for ship name in db.")
            return int_ships[0].imo

        return None


def back_fill_ship_position(force_rebuild=True):
    """
    Function to try and find ship positions using both Datalastic and MT for older events

    Returns
    -------

    """
    events = session.query(
        Event
    ) \
        .filter(Event.interacting_ship_imo != sa.null(),
                Event.ship_closest_position == sa.null())

    mtcalls = session.query(
        MarineTrafficCall.params['imo'].astext.label('ship_imo'),
        MarineTrafficCall.params['fromdate'].label('fromdate'),
        MarineTrafficCall.params['todate'].label('todate')
    ) \
        .filter(MarineTrafficCall.status == base.HTTP_OK) \
        .filter(MarineTrafficCall.method == base.VESSEL_POSITION) \
        .filter(MarineTrafficCall.date_utc > '2022-10-05 20:50')

    for e in tqdm(events.all()):

        if not force_rebuild:
            previous_calls = mtcalls.filter(MarineTrafficCall.params['imo'].astext == e.ship_imo).all()
            matches = [q for q in previous_calls if to_datetime(q.fromdate) <= to_datetime(e.date_utc) <= to_datetime(q.todate)]

            # We have queried this event before
            if len(matches) > 0:
                continue

        logger.info("Processing event id {}.".format(e.id))
        # Attempt to get the closest position in time and add to event
        ship_position, intship_position, d, position_time_diff = check_distance_between_ships(e.ship_imo, e.interacting_ship_imo,
                                                                                              e.date_utc, use_cache=True, cache_only=False)

        if d is not None and d < (position_time_diff * base.AVG_TANKER_SPEED_KMH * 2 * 1000):
            e.ship_closest_position = ship_position
            e.interacting_ship_closest_position = intship_position
            e.interacting_ship_details = {"distance_meters": int(d)}
            session.commit()
def back_fill_ship_imo():
    '''
    Function to find and add ship_imo for ships which did not meet legacy distance check logic or which we were
    unable to find at the time

    :return:
    '''
    events = session.query(
        Event
    ) \
        .filter(Event.interacting_ship_imo == sa.null())

    for e in tqdm(events.all()):
        # first try and find a ship has been seen in STS events and we have confirmed imo for
        interacting_ship_name = e.interacting_ship_name

        interacting_ship_imo = find_ship_imo_locally(interacting_ship_name)

        if interacting_ship_imo is not None:
            e.interacting_ship_imo = interacting_ship_imo
            session.commit()


def create_mtevent_type_table(force_rebuild=False):
    """
    This function creates the mtevent_type table which stores information
    about different event types, ids and descriptions; by default if table exists
    and force_rebuild=False will only append new rows

    Parameters
    ----------
    force_rebuild : if table already exists, delete data and refill

    Returns
    -------

    """

    # create table if it doesn't exist already
    if not check_if_table_exists(MarineTrafficEventType, create_table=True):
        logger.error("Table does not exist. Create table manually or set create_table=True.")
        return

    mtevent_type_df = pd.read_csv('assets/mtevent_type.csv')

    assert mtevent_type_df['id'].notnull().values.all()

    mtevent_type_df['id'] = mtevent_type_df['id'].apply(str)
    mtevent_type_df['name'] = mtevent_type_df['name'].apply(str)
    mtevent_type_df['description'] = mtevent_type_df['description'].apply(str)
    mtevent_type_df['availability'] = mtevent_type_df['availability'].apply(str)

    # cna have empty descriptions
    mtevent_type_df.replace({np.nan: None}, inplace=True)

    if force_rebuild:
        try:
            session.query(MarineTrafficEventType).delete()
            session.commit()
        except:
            session.rollback()

    # upsert event types
    upsert(df=mtevent_type_df, table=DB_TABLE_MTEVENT_TYPE, constraint_name="unique_event_type_id")
    session.commit()
    return
