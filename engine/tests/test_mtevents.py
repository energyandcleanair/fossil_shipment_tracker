from engine import mtevents

def test_mtevent_type():
    mtevents.create_mtevent_table(force_rebuild=False)
    return

def test_events():
    mtevents.initialise_events_from_cache()
    return