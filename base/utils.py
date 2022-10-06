from shapely import geometry
import shapely
import pyproj
import datetime as dt
import pandas as pd
from geoalchemy2 import WKTElement, WKBElement
from base.encoder import JsonEncoder
import json
import numpy as np

def distance_between_points(p1, p2, ellps = 'WGS84'):
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
    geod = pyproj.Geod(ellps=ellps)

    try:
        if isinstance(p1, WKBElement):
            p1, p2 = wkb_to_shape(p1), wkb_to_shape(p2)
        else:
            p1, p2 = shapely.wkt.loads(p1.replace("SRID=4326;", "")), shapely.wkt.loads(p2.replace("SRID=4326;", ""))
        angle1, angle2, distance = geod.inv(p1.x, p1.y, p2.x, p2.y)
        return distance
    except TypeError:
        return None

def latlon_to_point(lat, lon, wkt=True):
    try:
        return "SRID=4326;" + geometry.Point(float(lon), float(lat)).wkt
    except TypeError:
        return None


def to_list(d):
    if d is None:
        return []

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
                try:
                    return dt.datetime.strptime(d, "%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    return dt.datetime.strptime(d, "%Y-%m-%d %H:%M")
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
    """
    converts wkb to wkt or shapely geometry object

    Parameters
    ----------
    df : dataframe to convert
    to : conversion, either shape or wkt

    Returns
    -------

    """
    if to == "shape":
        df["geometry"] = df.geometry.apply(wkb_to_shape)
    if to == "wkt":
        df["geometry"] = df.geometry.apply(to_wkt)
    return df


def intersect(lst1, lst2):
    return list(set(lst1) & set(lst2))


def df_to_json(df, nest_in_data=False):
    # To be parsable by JS
    df = df.where(pd.notnull(df), None)

    if nest_in_data:
        return json.dumps({"data": df.to_dict(orient="records")}, cls=JsonEncoder)
    else:
        return json.dumps(df.to_dict(orient="records"), cls=JsonEncoder)


def split(list_, chunk_size):

  for i in range(0, len(list_), chunk_size):
    yield list_[i:i + chunk_size]
