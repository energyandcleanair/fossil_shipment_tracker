import pandas as pd
import numpy as np
import sqlalchemy
from tqdm import tqdm

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

from base.models import MarineTrafficEventType, Event, EventShipment, Shipment, Departure, Ship

def update(
        date_from="2022-02-01",
        date_to=dt.date.today() + dt.timedelta(days=1),
        ship_imo=None,
        use_cache=False,
        cache_objects=True,
        only_ongoing=True,
        force_rebuild=False,
        upload_unprocessed_events=True):
    logger_slack.info("=== Updating events for ships ===")

    ships = session.query(
            Departure.ship_imo.distinct().label("ship_imo"),
            Shipment.status
         ) \
        .join(Departure, Shipment.departure_id == Departure.id)

    if ship_imo:
        ships = ships.filter(Departure.ship_imo.in_(to_list(ship_imo)))
    if only_ongoing:
        ships = ships.filter(Shipment.status != "completed")

    for ship in tqdm(ships.all()):

        # convert SQLAlchemy.row object
        ship = ship._asdict()

        # we do not want to iterate twice over same ship imo if there are completed AND ongoing shipments
        if not only_ongoing and ship["status"] != 'completed':
            continue

        if not force_rebuild:
            last_event_shipment = session.query(EventShipment) \
                .filter(Departure.ship_imo == ship["ship_imo"],
                        EventShipment.created_at <= to_datetime(date_to)) \
                .join(Shipment, EventShipment.shipment_id == Shipment.id) \
                .join(Departure, Shipment.departure_id == Departure.id) \
                .order_by(EventShipment.created_at.desc()) \
                .first()

            if last_event_shipment is not None:
                es_date_from = last_event_shipment.created_at + dt.timedelta(minutes=1)
            else:
                es_date_from = date_from

            date_bounds = [(es_date_from, to_datetime(date_to))]

        if force_rebuild:
            date_bounds = [(date_from, date_to)]

        for dates in date_bounds:
            query_date_from = dates[0]
            query_date_to = dates[1]

            events = Marinetraffic.get_ship_events_between_dates(
                date_from=to_datetime(query_date_from),
                date_to=to_datetime(query_date_to),
                imo=ship["ship_imo"],
                use_cache=use_cache,
                cache_objects=cache_objects
            )

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

def add_interacting_ship_details_to_event(event, distance_check = 5000):
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

    # fill imo where necessary from MT
    if intship.imo is None:
        if intship.mmsi is not None:
            mt_intship = Marinetraffic.get_ship(mmsi=intship.mmsi)
            if not mt_intship:
                print("Failed to find imo in MT for event: {}".format(event_content))
                return False
            if intship.name == mt_intship.name:
                intship.imo = mt_intship.imo
            else:
                print("Found match for ship with mmsi, but names do not match for event {}".format(event_content))
        else:
            print("No ship imo found and we do not have an mmsi for event: {}".format(event_content))
            return False

    # check if interacting ship exists already
    # TODO: add ship input option to .fill so we do not have to request twice?
    found = fill(imos=[intship.imo])
    if not found:
        print("Failed to upload misisng ships")
        return False

    ship_position = Datalastic.get_position(imo=ship_imo, date=event_time)
    intship_position = Datalastic.get_position(imo=intship.imo, date=event_time)

    if not ship_position or not intship_position:
        print("Failed to find ship positions. try increasing time window...")
        return False

    # TODO: is there a better way to handle the SRID section?
    d = distance_between_points(ship_position.geometry.replace("SRID=4326;",""), intship_position.geometry.replace("SRID=4326;",""))

    if d:
        print("Distance between ships was {} at {}".format(d, event_time))
        if d < distance_check:
            event.interacting_ship_name = intship.name
            event.interacting_ship_imo = intship.imo
            event.interacting_ship_details = json.dumps(dict((col, getattr(intship, col)) for col in intship.__table__.columns.keys()))
            return True

    return False

def create_mtevent_table(force_rebuild=False):
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