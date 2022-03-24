import pandas as pd

from base.db import engine, session
from models import DB_TABLE_PORTCALL
from engine import ship


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
    portcall_imos = portcalls_df.ship_imo.unique()

    # First ensure ships are in our database
    ship.fill(imos=portcall_imos)

    # Upload portcalls
    portcalls_df.to_sql(DB_TABLE_PORTCALL, con=engine, if_exists="append")

    #TODO Upsert portcalls instead
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