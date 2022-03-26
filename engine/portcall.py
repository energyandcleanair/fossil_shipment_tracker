import pandas as pd
import datetime as dt

from base.db import session
from base.db_utils import upsert
from models import DB_TABLE_PORTCALL
from engine import ship
from engine import port
from engine.marinetraffic import Marinetraffic

from models import PortCall


def fill(limit=None):
    """
    Fill PortCall table with manually downloaded data (from MarimeTraffic interface)
    Original files are in assets/marinetraffic
    :param limit: limit numbers of portcalls for debuging to be faster
    :return:
    """
    portcalls_df = pd.read_csv("assets/portcalls.csv")
    if limit:
        portcalls_df = portcalls_df.iloc[0:limit]

    portcalls_df["move_type"] = portcalls_df.move_type.str.lower()
    portcalls_df = portcalls_df[["ship_mmsi", "ship_imo", "port_unlocode", "move_type",
                                 "load_status", "port_operation", "date_utc", "terminal_id", "berth_id"]]
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


def get_first_arrival_portcall(imo, date_from, filter=None):

    # First look in DB
    cached_portcalls = PortCall.query.filter(PortCall.ship_imo==imo, PortCall.date_utc >= date_from).all()
    filtered_cached_portcalls = None

    if filter is not None:
        filtered_cached_portcalls = [x for x in cached_portcalls if filter(x)]
    else:
        filtered_cached_portcalls = cached_portcalls

    if filtered_cached_portcalls:
        # We found a matching portcall in db
        filtered_cached_portcalls.sort(key=lambda x: x.date_utc)
        return filtered_cached_portcalls[0]


    # If not, query MarineTraffic
    # But do so only after the last cached portcall to avoid additional costs
    if cached_portcalls:
        date_from = max([x.date_utc for x in cached_portcalls]) + dt.timedelta(minutes=1)

    filtered_portcall, portcalls = Marinetraffic.get_first_arrival_portcall(imo=imo,
                                                        date_from=date_from,
                                                        filter=filter)

    # Store them in db so that we won't query them
    for portcall in portcalls:
        session.add(portcall)
    session.commit()

    return filtered_portcall

def update_ports():
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



