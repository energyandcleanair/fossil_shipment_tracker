import pandas as pd
import geopandas as gpd
import sqlalchemy as sa
from tqdm import tqdm
from sqlalchemy import func

from base.logger import logger
from base.db import session
from base.db_utils import upsert
from base.models import Port
from base.models import DB_TABLE_PORT
from engines.datalastic import Datalastic
from geoalchemy2 import Geometry
from base.utils import update_geometry_from_wkb


def count():
    return session.query(Port).count()


def get_id(unlocode=None, marinetraffic_id=None, name=None, add_if_needed=True):
    """
    Look for a port matching unlocode, marinetraffic and/or name
    Note that if unlocode is given, the other fields aren't used for querying
    the database, but only for asking Datalastic when missing
    :param unlocode:
    :param marinetraffic_id:
    :param name:
    :param add_if_needed:
    :return:
    """
    found = session.query(Port.id)

    if unlocode:
        found = found.filter(Port.unlocode == str(unlocode))
    elif marinetraffic_id:
        found = found.filter(Port.marinetraffic_id == marinetraffic_id)

    found = found.all()
    if len(found) == 0:
        if add_if_needed and name:
            ports = Datalastic.search_ports(
                name=name, marinetraffic_id=marinetraffic_id, fuzzy=False
            )
            if ports is not None and len(ports) == 1:
                port = ports[0]
                port.unlocode = unlocode
                port.marinetraffic_id = marinetraffic_id
                try:
                    session.add(port)
                    session.commit()
                    return port.id
                except sa.exc.IntegrityError as e:
                    if "psycopg2.errors.UniqueViolation" in str(e):
                        print("Failed to upload port: duplicated port")
                    session.rollback()
                    return None

            # TODO Add MT here

        logger.warning(
            "Didn't find any port (unlocode: %s, marinetraffic: %s)" % (unlocode, marinetraffic_id)
        )
        return None

    if len(found) > 1:
        logger.warning(
            "Found more than one port (unlocode: %s, marinetraffic: %s)"
            % (unlocode, marinetraffic_id)
        )
        return None

    return found[0][0]


def add_check_departure_to_anchorage():
    """Ports with check_departure set to True
    are the legitimate ports with UNLOCODE. We want to also check departure
    from related anchorages
    """
    ports_checked = Port.query.filter(Port.check_departure).all()
    for port in tqdm(ports_checked):
        regexps = [
            port.name + " ANCH",
            port.name.split("-")[0] + " ANCH",
            port.name.replace("'", "") + " ANCH",
            port.name.replace("`", "") + " ANCH",
        ]

        found = False
        for regexp in regexps:
            port_anchorages = Port.query.filter(
                sa.and_(Port.name.op("~*")(regexp), Port.iso2 == port.iso2)
            )
            if port_anchorages.count() > 0:
                found = True
                for port_anchorage in port_anchorages.all():
                    port_anchorage.check_departure = True
                    session.commit()

        if not found:
            logger.info(f"Didn't find anchorage for port {port.name}")

    insert_new_port(iso2="IN", marinetraffic_id=21982, name="SIKKA-ANCH")


def fill():
    """
    Fill port data from prepared file
    :return:
    """
    # Was originally created in another repo: https://github.com/energyandcleanair/shipment_tracking
    ports_df = pd.read_csv("assets/ports.csv")
    if not "check_arrival" in ports_df.columns:
        ports_df["check_arrival"] = False

    ports_gdf = gpd.GeoDataFrame(
        ports_df, geometry=gpd.points_from_xy(ports_df.lon, ports_df.lat), crs="EPSG:4326"
    )
    ports_gdf.loc[ports_gdf.lon.isnull(), "geometry"] = None
    ports_gdf = ports_gdf[
        ["unlocode", "name", "iso2", "check_departure", "check_arrival", "geometry"]
    ]
    ports_df = pd.DataFrame(ports_gdf)
    ports_df = update_geometry_from_wkb(ports_df, to="wkt")

    upsert(
        df=ports_df.loc[ports_df.check_departure],
        table=DB_TABLE_PORT,
        constraint_name="unique_port",
        dtype=({"geometry": Geometry("POINT", 4326)}),
    )

    # (JUST FILLING GEOMETRY)
    from base.models import PortCall
    import sqlalchemy as sa

    missing_ports = (
        session.query(Port).filter(Port.geometry == sa.null()).filter(Port.check_departure).all()
    )

    for m in missing_ports:
        print(m)
        found_ports = Datalastic.search_ports(name=m.name, fuzzy=False)
        for found_port in found_ports:
            if found_port.unlocode == m.unlocode:
                m.geometry = found_port.geometry

    session.commit()
    return


def insert_new_port(iso2, unlocode, name=None, marinetraffic_id=None):
    if name is not None:
        new_ports = Datalastic.search_ports(name=name, marinetraffic_id=marinetraffic_id)
    else:
        new_ports = [Port(**{"unlocode": unlocode, "iso2": iso2})]

    for new_port in new_ports:
        session.add(new_port)
    session.commit()


def update_area():
    session.query(Port).filter(Port.iso2 == "RU").update(
        {
            "area": sa.case(
                [
                    (func.ST_X(Port.geometry) > 100, "Pacific"),
                    (func.ST_Y(Port.geometry) > 62, "Arctic"),
                    (func.ST_Y(Port.geometry) > 50, "Baltic"),
                    (func.ST_X(Port.geometry) > 47, "Caspian Sea"),
                ],
                else_="Black sea",
            )
        },
        synchronize_session=False,
    )

    session.commit()
