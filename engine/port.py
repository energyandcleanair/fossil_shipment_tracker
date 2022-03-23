import pandas as pd
import fiona
import geopandas as gpd

from models import DB_TABLE_PORT
def fill():
    """
    Fill port data from assets
    :return:
    """
    # Take ports from UM directly
    ports_df = pd.read_csv("https://raw.githubusercontent.com/datasets/un-locode/master/data/code-list.csv")
    ports_df["unlocode"] = ports_df.Country + ports_df.Location

    def coords_to_lonlat(c):
        # c="5554N 03749E"
        try:
            lon = (float(c[6:9]) + float(c[9:11]) / 60) * (-1 if c[11]=='W' else 1)
            lat = (float(c[0:2]) + float(c[2:4]) / 60 ) * (-1 if c[4]=='S' else 1)
            return lon, lat
        except TypeError:
            return None, None

    ports_df[["lon", "lat"]] = ports_df.Coordinates.apply(coords_to_lonlat)
    ports_df.rename(columns={"Country": "iso2", "Name": "name"}, inplace=True)
    ports_df = gpd.GeoDataFrame(ports_df, geometry=gpd.points_from_xy(ports_df.lon, ports_df.lat))
    ports_df.to_sql(DB_TABLE_PORT,  con=engine)








    return

