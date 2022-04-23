import pandas as pd
import geopandas as gpd

from base.logger import logger
from base.db import session
from base.db_utils import upsert
from base.models import Port
from base.models import DB_TABLE_PORT
from engine.datalastic import Datalastic
from geoalchemy2 import Geometry
from base.utils import update_geometry_from_wkb


def count():
    return session.query(Port).count()


def get_id(unlocode=None, marinetraffic_id=None):
    found = session.query(Port.id)

    if unlocode:
        found = found.filter(Port.unlocode==str(unlocode))

    elif marinetraffic_id:
        found = found.filter(Port.marinetraffic_id==marinetraffic_id)

    found = found.all()
    if len(found) == 0:
        logger.warning("Didn't find any port (unlocode: %s, marinetraffic: %s)" %(unlocode, marinetraffic_id))
        return None

    if len(found) > 1:
        logger.warning("Found more than one port (unlocode: %s, marinetraffic: %s)" %(unlocode, marinetraffic_id))
        return None

    return found[0][0]


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
    ports_gdf.loc[ports_gdf.lon.isnull(),"geometry"] = None
    ports_gdf = ports_gdf[["unlocode", "name", "iso2", "check_departure", "check_arrival", "geometry"]]
    ports_df = pd.DataFrame(ports_gdf)
    ports_df = update_geometry_from_wkb(ports_df, to="wkt")

    upsert(df=ports_df.loc[ports_df.check_departure],
           table=DB_TABLE_PORT,
           constraint_name="unique_port",
           dtype=({'geometry': Geometry('POINT', 4326)}))

    # (JUST FILLING GEOMETRY)
    from base.models import PortCall
    import sqlalchemy as sa

    missing_ports = session.query(Port) \
        .filter(Port.geometry == sa.null()) \
        .filter(Port.check_departure).all()

    for m in missing_ports:
        print(m)
        found_ports = Datalastic.get_port_infos(name=m.name, fuzzy=False)
        for found_port in found_ports:
            if found_port.unlocode == m.unlocode:
                m.geometry = found_port.geometry

    session.commit()
    return


def insert_new_port(iso2, unlocode, name=None, marinetraffic_id=None):
    if name is not None:
        new_ports = Datalastic.get_port_infos(name=name, marinetraffic_id=marinetraffic_id)
    else:
        new_ports = [Port(**{"unlocode": unlocode,
                           "iso2": iso2})]

    for new_port in new_ports:
        session.add(new_port)
    session.commit()