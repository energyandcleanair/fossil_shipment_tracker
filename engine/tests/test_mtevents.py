from engine import mtevents

def test_mtevent_type():
    mtevents.create_mtevent_table(force_rebuild=True)
    return