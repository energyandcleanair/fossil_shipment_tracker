from base.models import Event, Ship
from engine import ship, port
from engine.mtevents import create_mtevent_type_table, add_interacting_ship_details_to_event, update, find_ships_in_db
from engine.marinetraffic import Marinetraffic
from base.db import check_if_table_exists


def test_find_ships_by_name():
    find_ships_in_db('BLUEFISH')


def test_mtevent_type():
    create_mtevent_type_table(force_rebuild=False)
    return


def test_upload_events():
    update(ship_imo="9417177",use_cache=True, cache_objects=False,
           force_rebuild=True, upload_unprocessed_events=False)
    return


def test_check_tables():
    assert check_if_table_exists(Event, create_table=True) is True


def test_process_ship_events():
    events = Marinetraffic.get_ship_events_between_dates(imo="9417177", date_from='2022-05-18', date_to='2022-05-24',
                                                         use_cache=True, cache_objects=False)
    for e in events:
        assert e.ship_imo is not None and e.ship_name is not None and e.content is not None

    event_status = [add_interacting_ship_details_to_event(e) for e in events]

    assert event_status.count(True) == len(event_status)
