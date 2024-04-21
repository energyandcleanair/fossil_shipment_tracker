from base.db import session
from base.models.kpler import KplerSyncHistory

from base.db_utils import upsert

from tqdm import tqdm

import pandas as pd


def update_sync_history_with_status(
    *,
    origin_iso2,
    date_from,
    date_to,
    comparison,
    checked_time,
):
    query_for_history_entries = session.query(KplerSyncHistory).filter(
        KplerSyncHistory.country_iso2 == origin_iso2,
        KplerSyncHistory.date >= date_from,
        KplerSyncHistory.date <= date_to,
    )

    history_entries = query_for_history_entries.all()

    # Comparison to dict of date to ok
    comparison = comparison.set_index("departure_day").to_dict(orient="index")

    for entry in tqdm(history_entries, unit=f"history-entry", leave=False):
        is_valid = comparison[entry.date]["ok"] if entry.date in comparison else True
        entry.last_checked = checked_time
        entry.is_valid = is_valid

    session.commit()


def mark_updated(from_iso2, period, update_time):
    days = [day for day in get_days_in_month(period)]

    records = [
        {"date": day, "country_iso2": from_iso2, "last_updated": update_time, "is_valid": False}
        for day in days
    ]

    df = pd.DataFrame.from_records(records)

    upsert(
        df,
        table=KplerSyncHistory.__tablename__,
        constraint_name="kpler_sync_history_unique",
        show_progress=False,
    )


def get_days_in_month(period_as_str):
    period = pd.Period(period_as_str, freq="M")
    days = pd.date_range(start=period.start_time.date(), end=period.end_time.date()).to_list()
    return days
