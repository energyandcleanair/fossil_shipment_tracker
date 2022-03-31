import pandas as pd
import geopandas as gpd

from base.db import session
from base.db_utils import upsert
from base.models import Port
from base.models import DB_TABLE_PORT
from engine.datalastic import Datalastic


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
    # upsert(df=ports_gdf.iloc[0:2], table=DB_TABLE_PORT, constraint_name="unique_port2")

    # (JUST FOR TRANSITIONING TO NEW PORT ID) Check ports in portcalls that aren't in port
    from base.models import PortCall
    import sqlalchemy

    missing_ports = session.query(PortCall.others) \
        .filter(PortCall.port_unlocode.notin_(session.query(Port.unlocode).filter(Port.unlocode != sqlalchemy.null()))).all()

    missing_ports = [{'marinetraffic_id': x[0]['marinetraffic']['PORT_ID'],
                    'name' : x[0]['marinetraffic']['PORT_NAME']} for x in missing_ports]
    missing_ports = pd.DataFrame(missing_ports).drop_duplicates().to_dict(orient="records")

    ports = []
    for m in missing_ports:
        print(m)
        port = Datalastic.get_port_infos(name=m["name"], marinetraffic_id=m["marinetraffic_id"])
        session.add(port)

    session.commit()
    return


def insert_new_port(iso2, unlocode, name=None, marinetraffic_id=None):
    if name is not None:
        new_port = Datalastic.get_port_infos(name=name, marinetraffic_id=marinetraffic_id)
    else:
        new_port = Port(**{"unlocode": unlocode,
                           "iso2": iso2})
    session.add(new_port)
    session.commit()