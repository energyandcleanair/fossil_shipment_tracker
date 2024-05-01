from base.models import Event, Ship
import pytest
from engines import ship, port
from engines.mtevents import (
    create_mtevent_type_table,
    add_interacting_ship_details_to_event,
    update,
    find_ships_in_db,
)
from engines.marinetraffic import Marinetraffic
from base.db import check_if_table_exists


@pytest.mark.system
def test_find_ships_by_name():
    ships = find_ships_in_db("BLUEFISH")

    assert ships is not None


@pytest.mark.system
def test_upload_events():
    update(
        ship_imo="9417177",
        use_cache=True,
        cache_objects=False,
        force_rebuild=False,
        upload_unprocessed_events=True,
    )
    return


@pytest.mark.system
def test_process_ship_events():
    events = Marinetraffic.get_ship_events_between_dates(
        imo="9417177",
        date_from="2022-05-18",
        date_to="2022-05-24",
        use_cache=True,
        cache_objects=False,
    )
    for e in events:
        assert e.ship_imo is not None and e.ship_name is not None and e.content is not None

    event_status = [add_interacting_ship_details_to_event(e) for e in events]

    assert event_status.count(True) == len(event_status)
