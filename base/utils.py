from shapely import geometry
import shapely
import datetime as dt
import pandas as pd
from geoalchemy2 import WKTElement
from base.encoder import JsonEncoder
import json
import numpy as np

def distance_between_points(p1, p2, wkt=True):
    """
    Returns distance in meters between two points; if wkt=False assumed to be shapely Point objects

    Parameters
    ----------
    p1 : point 1
    p2 : point 2
    wkt : whether p1/p2 are wkt, if false assumed to be shapely Points

    Returns
    -------
    Distance in meters

    """
    try:
        if wkt:
            p1, p2 = shapely.wkt.loads(p1), shapely.wkt.loads(p2)
        return p1.distance(p2)
    except TypeError:
        return None

def latlon_to_point(lat, lon, wkt=True):
    try:
        return "SRID=4326;" + geometry.Point(float(lon), lat).wkt
    except TypeError:
        return None


def to_list(d):
    if not isinstance(d, list):
        return [d]
    else:
        return d


def to_datetime(d, date_ref=None):
    if not date_ref:
        date_ref = dt.datetime.now()
    if isinstance(d, str):
        try:
            return dt.datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            try:
                return to_datetime(int(d), date_ref=date_ref)
            except ValueError:
                return dt.datetime.strptime(d, "%Y-%m-%dT%H:%M:%S")
    if isinstance(d, dt.datetime):
        return d
    if isinstance(d, dt.date):
        return dt.datetime.combine(d, dt.datetime.min.time())
    if isinstance(d, pd.Timestamp):
        return d.to_pydatetime()
    if isinstance(d, int):
        return to_datetime(date_ref) + dt.timedelta(days=d)
    if d is None:
        return None

    raise TypeError("d is not an int, date or datetime")


def wkb_to_shape(geom):
    from shapely import wkb
    if isinstance(geom, shapely.geometry.base.BaseGeometry):
        return geom
    try:
        return wkb.loads(bytes(geom.data))
    except (shapely.errors.WKBReadingError, AttributeError):
        return None


def to_wkt(x):
    shape = wkb_to_shape(x)
    if shape is not None:
        return WKTElement(shape.wkt, srid=4326)
    else:
        return None


def update_geometry_from_wkb(df, to="shape"):
    if to == "shape":
        df["geometry"] = df.geometry.apply(wkb_to_shape)
    if to == "wkt":
        df["geometry"] = df.geometry.apply(to_wkt)
    return df


def intersect(lst1, lst2):
    return list(set(lst1) & set(lst2))


def df_to_json(df, nest_in_data=False):
    # To be parsable by JS
    df.replace({np.nan: None}, inplace=True)

    if nest_in_data:
        return json.dumps({"data": df.to_dict(orient="records")}, cls=JsonEncoder)
    else:
        return json.dumps(df.to_dict(orient="records"), cls=JsonEncoder)
