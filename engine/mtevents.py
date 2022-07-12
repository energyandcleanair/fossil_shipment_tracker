import pandas as pd
import numpy as np
import sqlalchemy
from tqdm import tqdm

import base
from base.logger import logger, logger_slack
from base.db import session
from base.db_utils import upsert
from base.db import check_if_table_exists
from base.models import DB_TABLE_MTEVENT_TYPE
from base.utils import distance_between_points, to_list, to_datetime

from engine.datalastic import Datalastic
from engine.marinetraffic import Marinetraffic, load_cache
from engine.ship import fill

import datetime as dt

import re
import json

from base.models import MarineTrafficEventType, Event, EventShipment, Shipment, Departure, Ship, MarineTrafficCall

def update(
        date_from="2022-02-01",
        date_to=dt.date.today() + dt.timedelta(days=1),
        ship_imo=None,
        use_cache=False,
        cache_objects=True,
        only_ongoing=False,
        force_rebuild=False,
        upload_unprocessed_events=True,
        limit=None):
    """
    This function retrieves the events for a specific ship imo

    Parameters
    ----------
    date_from : date we want to query from
    date_to : date we want to query to
    ship_imo : the ship imo(s)
    use_cache : whether we want to check cache
    cache_objects : whether we want to cache objects
    only_ongoing : only query ongoing shipments
    force_rebuild : force rebuild ignoring previous calls
    upload_unprocessed_events : whether to upload events where we failed to get some date
    limit : the limit of the number of ships we want to process

    Returns
    -------

    """
    logger_slack.info("=== Updating events for ships ===")

    ships = session.query(
            Departure.ship_imo.distinct().label("ship_imo"),
            Shipment.status
         ) \
        .join(Departure, Shipment.departure_id == Departure.id)

    if ship_imo:
        ships = ships.filter(Departure.ship_imo.in_(to_list(ship_imo)))
    if only_ongoing:
        ships = ships.filter(Shipment.status != base.COMPLETED)
    if limit:
        ships = ships.limit(limit)

    processed_ships = []

    for ship in tqdm(ships.all()):

        # convert SQLAlchemy.row object
        ship = ship._asdict()

        # get ship imo
        ship_imo = ship["ship_imo"]

        if ship_imo in processed_ships:
            continue

        # add ship to processed
        processed_ships.append(ship_imo)

        if not force_rebuild:

            # check whether we called this ship imo in the MTCall table and get latest date
            last_event_call = session.query(MarineTrafficCall) \
                .filter(MarineTrafficCall.method == base.VESSEL_EVENTS,
                        MarineTrafficCall.params['imo'].astext == ship_imo,
                        MarineTrafficCall.status == base.HTTP_OK) \
                .order_by(MarineTrafficCall.params['todate'].desc()) \
                .first()

            # if we did check this ship before and force rebuild is false, only query since last time
            if last_event_call is not None:
                es_date_from = to_datetime(last_event_call.params['todate']) + dt.timedelta(minutes=1)
            else:
                es_date_from = date_from

            date_bounds = [(es_date_from, to_datetime(date_to))]

        if force_rebuild:
            date_bounds = [(date_from, date_to)]

        for dates in date_bounds:
            query_date_from = dates[0]
            query_date_to = dates[1]

            events = Marinetraffic.get_ship_events_between_dates(
                imo=ship_imo,
                date_from=to_datetime(query_date_from),
                date_to=to_datetime(query_date_to),
                use_cache=use_cache,
                cache_objects=cache_objects
            )

            if not events:
                print("No vessel events found for ship_imo: {}.".format(ship_imo))

            event_process_state = [add_interacting_ship_details_to_event(e) for e in events]

            if event_process_state.count(False) > 0:
                print("Failed to process {} events out of {}".format(event_process_state.count(False), len(event_process_state)))

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


def add_interacting_ship_details_to_event(event, distance_check = 10000):
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

    print("{} vessel interacting with {}".format(ship_name, intship_name))
    intship = Datalastic.find_ship(intship_name, fuzzy=True, return_closest=True)

    if not intship:
        print("Error in finding ship in Datalastic for event: {}".format(event_content))
        return False

    event.interacting_ship_name = intship.name

    # fill imo where necessary from MT
    if intship.imo is None:
        if intship.mmsi is not None:
            mt_intship_check = fill(mmsis=[intship.mmsi])

            if not mt_intship_check:

                # add unknown ship to db so we don't repeatedly query MT
                unknown_ship = Ship(imo='NOTFOUND_' + intship.mmsi, mmsi=intship.mmsi, type=intship.type,
                                    name=intship.name)
                session.add(unknown_ship)
                session.commit()

                return False

            mt_ship = session.query(Ship).filter(Ship.mmsi == intship.mmsi).all()

            # check if we find more than 1 ship
            if len(mt_ship) > 1:
                return False

            # if we don't find any in db by mmsi we failed to upload...
            if not mt_ship:
                print("Failed to find imo in MT for event: {}".format(event_content))
                return False

            mt_ship = mt_ship[0]

            if intship.name == mt_ship.name:
                intship.imo = mt_ship.imo
            else:
                print("Found match for ship with mmsi, but names do not match for event {}".format(event_content))
        else:
            print("No ship imo found and we do not have an mmsi for event: {}".format(event_content))
            return False

    # check if interacting ship is in db
    found = fill(imos=[intship.imo])
    if not found:
        print("Failed to upload missing ships")
        return False

    ship_position = Datalastic.get_position(imo=ship_imo, date=event_time)
    intship_position = Datalastic.get_position(imo=intship.imo, date=event_time)

    if not ship_position or not intship_position:
        print("Failed to find ship positions. try increasing time window...")
        return False

    ship_position, intship_position = ship_position.geometry, intship_position.geometry

    # add positions to details
    event.ship_closest_position, event.interacting_ship_closest_position = ship_position, intship_position

    # TODO: is there a better way to handle the SRID section?
    d = distance_between_points(ship_position.replace("SRID=4326;",""), intship_position.replace("SRID=4326;",""))

    if d:
        print("Distance between ships was {} at {}".format(d, event_time))
        if d < distance_check:
            event.interacting_ship_imo = intship.imo
            event.interacting_ship_details = {"distance_meters": int(d)}
            return True

    return False

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