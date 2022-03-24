import pandas as pd
import fiona #required to prevent circular import
import pyproj
import geopandas as gpd

from base.db import engine
from base.db import session
from base.db_utils import upsert
from models import Port
from models import DB_TABLE_PORT


def count():
    return session.query(Port).count()


def fill():
    """
    Fill port data from prepared file
    :return:
    """
    # Was originally created in another repo: https://github.com/energyandcleanair/shipment_tracking
    ports_df = pd.read_csv("assets/ports.csv")
    if not "check_arrival" in ports_df.columns:
        ports_df["check_arrival"] = False

    ports_gdf = gpd.GeoDataFrame(ports_df, geometry=gpd.points_from_xy(ports_df.lon, ports_df.lat), crs="EPSG:4326")
    ports_gdf = ports_gdf[["unlocode", "name", "iso2", "check_departure", "check_arrival", "geometry"]]

    upsert(df=ports_gdf, table=DB_TABLE_PORT, constraint_name="unique_port")
    return