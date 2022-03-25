from shapely import geometry


def latlon_to_point(lat, lon, wkt=True):
    return "SRID=4326;" + geometry.Point(float(lon), lat).wkt