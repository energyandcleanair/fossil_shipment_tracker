from typing import Any, Optional
from shapely import geometry
import shapely
import pyproj
import datetime as dt
import pandas as pd
from geoalchemy2 import WKTElement, WKBElement
from base.encoder import JsonEncoder
import json
import numpy as np
from collections.abc import Iterable


def daterange_intersection(daterange1, daterange2):
    """
    Returns the intersection of two date ranges

    :param daterange1:
    :param datarange2:
    :return:
    Intersection of the two date ranges, otherwise None
    """

    latest_start = max(daterange1[0], daterange2[0])
    earliest_end = min(daterange1[1], daterange2[1])

    if latest_start <= earliest_end:
        return (latest_start, earliest_end)
    else:
        return None


def remove_dates(base_daterange, dateranges, go_backward=False):
    """
    This function takes a date range and a list of dateranges and removes the list of dateranges from daterange

    Parameters
    ----------
    base_daterange :
    dateranges :

    Returns
    -------

    """

    # if going backward reverse input for intersection checking
    if go_backward:
        base_daterange = base_daterange[::-1]

    current_date, valid_dateranges = base_daterange[0], []

    for daterange in dateranges:
        intersection = daterange_intersection(base_daterange, daterange)

        # our remove date does not intersect with base date
        if intersection is None:
            continue

        # our remove date contains all of our base date range
        if intersection == base_daterange:
            return []

        # if we are not at the starting point of the date range we add valid range
        if intersection[0] != base_daterange[0]:
            valid_dateranges.append((current_date, intersection[0]))

        # otherwise we move our pointer to the next end point
        current_date = intersection[1]

    # add final section
    if current_date != base_daterange[1]:
        valid_dateranges.append((current_date, base_daterange[1]))

    # if we switched order, now reverse back to return in correct order
    if go_backward:
        return [(a, b) for b, a in valid_dateranges]

    return valid_dateranges


def subtract_daterange_from_other(base_daterange, subtract_daterange):
    """
    Remove one date range from another - this could maybe be done more nicely geomatrically with the idea of
        lines

    :param base_daterange: the daterange we want to subtract from
    :param subtract_daterange: the daterange we want to remove from another
    :return:

    """

    intersection = daterange_intersection(base_daterange, subtract_daterange)

    if intersection is None:
        return [base_daterange]

    # base contains all of subtract daterange
    if intersection == base_daterange:
        return [
            (base_daterange[0], subtract_daterange[0]),
            (subtract_daterange[1], base_daterange[1]),
        ]
    # our base is fully contained within our subtraction
    if intersection == subtract_daterange:
        return []

    # else return the daterange with the removal of the unioned section
    if intersection[0] == base_daterange[0]:
        return [(intersection[1], base_daterange[1])]
    else:
        return [(base_daterange[0]), intersection[0]]


def collapse_dates(date_list, buffer_seconds=120):
    """
    Takes a list of datefrom/tos and collapses any that are overlapping within a certain buffer limit

    :param date_list: list of date tuples to collapse
    :param buffer_seconds: the maximum time difference between date ranges to collapse. if set to 0 will only collapse
        date ranges which fully overlap
    :return:
    Returns a list of date tuples
    """

    if len(date_list) == 0:
        return []

    # force sort
    date_list.sort(key=lambda date_pair: date_pair[0])

    collapsed, current = [], date_list[0]

    for d in date_list[1:]:
        if (d[0] - current[1]).total_seconds() <= buffer_seconds:
            current = (min(current[0], d[0]), max(current[1], d[1]))
        else:
            collapsed.append(current)
            current = d
    else:
        collapsed.append(current)

    return collapsed


def distance_between_points(p1, p2, ellps="WGS84"):
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
            p1 = wkb_to_shape(p1)
        else:
            p1 = shapely.wkt.loads(p1.replace("SRID=4326;", ""))
        if isinstance(p2, WKBElement):
            p2 = wkb_to_shape(p2)
        else:
            p2 = shapely.wkt.loads(p2.replace("SRID=4326;", ""))

        try:
            angle1, angle2, distance = geod.inv(p1.x, p1.y, p2.x, p2.y)
            return distance
        except AttributeError:
            return None
    except TypeError:
        return None


def latlon_to_point(lat, lon, wkt=True):
    try:
        return "SRID=4326;" + geometry.Point(float(lon), float(lat)).wkt
    except TypeError:
        return None


def to_list(d, convert_tuple=False):
    if d is None:
        return []
    if convert_tuple and isinstance(d, tuple):
        return list(d)
    if not isinstance(d, list):
        return [d]
    else:
        return d


class DateTimeParseError(ValueError):
    pass


def to_datetime(d: Any, date_ref: Optional[dt.datetime] = None) -> Optional[dt.datetime]:
    if d is None:
        return None
    if isinstance(d, dt.datetime):
        return d

    if not date_ref:
        date_ref = dt.datetime.now()

    if isinstance(d, str):
        if represents_int(d):
            return to_datetime(int(d), date_ref=date_ref)
        return try_parse_date(d, formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"])

    if isinstance(d, dt.date):
        return dt.datetime.combine(d, dt.datetime.min.time())
    if isinstance(d, pd.Timestamp):
        return d.to_pydatetime()
    if isinstance(d, int):
        return to_datetime(date_ref) + dt.timedelta(days=d)

    raise TypeError("d is not an int, date or datetime")


def represents_int(s: str) -> bool:
    return s.isdigit() or (s[0] in ["-", "+"] and s[1:].isdigit())


def try_parse_date(date_str: str, *, formats: list[str]) -> dt.datetime:
    for fmt in formats:
        try:
            return dt.datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    raise DateTimeParseError(f"Unable to convert {date_str} to a valid date. Is it a valid date?")


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

    # Sometimes keys are dates (when pivoting)
    df.columns = [str(x) for x in df.columns]

    if nest_in_data:
        return json.dumps({"data": df.to_dict(orient="records")}, cls=JsonEncoder)
    else:
        return json.dumps(df.to_dict(orient="records"), cls=JsonEncoder)


def split(list_, chunk_size):
    for i in range(0, len(list_), chunk_size):
        yield list_[i : i + chunk_size]


def to_bool(v):
    if isinstance(v, bool):
        return v
    else:
        return str(v).lower() in ("yes", "true", "t", "1")


def read_json(path):
    with open(path) as f:
        return json.load(f)


def hash_df(df):
    # Create a hashable version
    # find columns that are list and convert them to tuple
    list_columns = [
        col
        for col in df.columns
        if any(df[col].notna()) and any(df[col].apply(lambda x: type(x) == list))
    ]

    def to_tuple_if_iterable(x):
        if x is None:
            return None
        if np.isscalar(x):
            if pd.isna(x):
                return None
            return x
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            return tuple(x)
        return x

    for col in list_columns:
        df[col] = df[col].apply(to_tuple_if_iterable)
    return df, list_columns


def unhash_df(df, list_columns):
    # Unhash the dataframe
    def to_list_if_iterable(x):
        if x is None or (np.isscalar(x) and pd.isna(x)):
            return None
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            return list(x)
        return x

    for col in list_columns:
        df[col] = df[col].apply(to_list_if_iterable)
    return df
