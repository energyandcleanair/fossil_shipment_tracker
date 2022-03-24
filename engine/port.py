import pandas as pd
import fiona #required to prevent circular import
import pyproj

import geopandas as gpd
from base.db import engine

from models import DB_TABLE_PORT

import os
os.environ["PROJ_LIB"]
pyproj.datadir.get_data_dir()


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
    ports_gdf.to_postgis(DB_TABLE_PORT, con=engine, if_exists="append")

    #TODO Make an upsert version that would update existing table if ports (pkey="unlocode") already exist
    # Can use something like this: https://stackoverflow.com/questions/55187884/insert-into-postgresql-table-from-pandas-with-on-conflict-update
    return

