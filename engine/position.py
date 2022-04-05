from engine.datalastic import Datalastic
from base.db import session
from sqlalchemy import func
from base.models import Position


def get(imo, date_from, date_to):
    positions = Datalastic.get_positions(imo=imo, date_from=date_from, date_to=date_to)
    return positions


def update(ship_imo, date_from, date_to):
        first_date, last_date = session.query(
            func.min(Position.date_utc).label('first_date'),
            func.max(Position.date_utc).label('last_date')
            ).filter(Position.ship_imo==ship_imo).all()[0]

        dates = []

        if first_date is None:
            # No position found, we query the whole voyage
            dates.append({"date_from": date_from, "date_to": date_to})
        else:
            # We only query head or tail or both
            if first_date > date_from:
                dates.append({"date_from": date_from, "date_to": first_date})
            if last_date < date_to:
                dates.append({"date_from": last_date, "date_to": date_to})

        for date in dates:
            positions = get(imo=ship_imo, **date)
            if positions:
                for p in positions:
                    session.add(p)
        session.commit()
