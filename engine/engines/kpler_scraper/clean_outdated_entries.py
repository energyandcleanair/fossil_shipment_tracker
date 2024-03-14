import os
from base.db import session


def clean_outdated_entries():
    # Read sql from 'clean_outdated_entries.sql'
    with open(os.path.join(os.path.dirname(__file__), "clean_outdated_entries.sql")) as f:
        sql = f.read()
    session.execute(sql)
    session.commit()
    return
