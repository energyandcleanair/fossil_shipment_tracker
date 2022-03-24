import pandas as pd

from base.db_utils import upsert
from models import DB_TABLE_PORTCALL
from engine import ship
from engine import port


def fill():
    """
    Fill PortCall table with manually downloaded data (from MarimeTraffic interface)
    Original files are in assets/marinetraffic
    :return:
    """
    portcalls_df = pd.read_csv("assets/portcalls.csv")
    portcalls_df["move_type"] = portcalls_df.move_type.str.lower()
    portcalls_df = portcalls_df[["ship_mmsi", "ship_imo", "port_unlocode", "move_type",
                                 "date_utc", "terminal_id", "berth_id"]]
    portcalls_df = portcalls_df.drop_duplicates(subset=["ship_imo", "move_type", "date_utc"])
    portcall_imos = portcalls_df.ship_imo.unique()


    # First ensure ships are in our database
    ship.fill(imos=portcall_imos)

    # Ensure ports are loaded in database
    if port.count() == 0:
        port.fill()

    # Upsert portcalls
    upsert(df=portcalls_df, table=DB_TABLE_PORTCALL, constraint_name="unique_portcall")
    return


def update():
    """
    Fill port calls for ports of interest, for dates not yet queried in database
    :return:
    """
    #TODO
    # - query MarineTraffic for portcalls at ports of interest (assets/departure_port_of_interest.csv)
    # - upsert in db
    # Things to pay attention to:
    # - we want to minimize credits use from MarineTraffic. So need to only query from last date available.
    # - port call have foreignkeys towards Port and Ship tables. If Port or Ship are missing, we need to find
    # information and insert them in their respective tables (probably using Datalastic)

    return



def get(date_from=None):
    """

    :param date_from:
    :return: Pandas dataframe of portcalls
    """
    return