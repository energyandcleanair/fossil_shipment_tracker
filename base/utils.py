from shapely import geometry
import shapely
import datetime as dt
import pandas as pd
from geoalchemy2 import WKTElement

def latlon_to_point(lat, lon, wkt=True):
    return "SRID=4326;" + geometry.Point(float(lon), lat).wkt


def to_list(d):
    if not isinstance(d, list):
        return [d]
    else:
        return d


def to_datetime(d):
    if isinstance(d, str):
        try:
            return dt.datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            return dt.datetime.strptime(d, "%Y-%m-%dT%H:%M:%S")
    if isinstance(d, dt.datetime):
        return d
    if isinstance(d, dt.date):
        return dt.datetime.combine(d, dt.datetime.min.time())
    if isinstance(d, pd.Timestamp):
        return d.to_pydatetime()
    if d is None:
        return None

    raise TypeError("d is not a date or datetime")


def wkb_to_shape(geom):
    from shapely import wkb
    try:
        return wkb.loads(bytes(geom.data))
    except (shapely.errors.WKBReadingError,AttributeError):
        return None


def update_geometry_from_wkb(df, to="shape"):
    if to == "shape":
        df["geometry"] = df.geometry.apply(wkb_to_shape)
    if to == "wkt":
        def to_wkt(x):
            shape = wkb_to_shape(x)
            if shape is not None:
                return WKTElement(shape.wkt, srid=4326)
            else:
                return None
        df["geometry"] = df.geometry.apply(to_wkt)
    return df