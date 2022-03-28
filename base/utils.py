from shapely import geometry
import datetime as dt
import pandas as pd

def latlon_to_point(lat, lon, wkt=True):
    return "SRID=4326;" + geometry.Point(float(lon), lat).wkt


def to_datetime(d):
    if isinstance(d, str):
        return dt.datetime.strptime(d, "%Y-%m-%d")
    if isinstance(d, dt.datetime):
        return d
    if isinstance(d, dt.date):
        return dt.datetime.combine(d, dt.datetime.min.time())
    if isinstance(d, pd.Timestamp):
        return d.to_pydatetime()
    if d is None:
        return None

    raise TypeError("d is not a date or datetime")

