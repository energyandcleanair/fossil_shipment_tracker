import pandas as pd
import datetime as dt
import sqlalchemy
from tqdm import tqdm

import base
from base.logger import logger
from base.db import session
from base.db_utils import upsert
from base.models import DB_TABLE_PORTCALL
from base.utils import to_datetime
from engine import ship
from engine import port
from engine.marinetraffic import Marinetraffic

from base.models import PortCall, Port, Ship


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


def get_first_arrival_portcall(imo,
                               date_from,
                               filter=lambda x: x.port_unlocode is not None and x.port_unlocode != "", # An arrival without unlocode is probably an anchoring
                               use_cache=True):

    # First look in DB
    if use_cache:
        cached_portcalls = PortCall.query.filter(PortCall.ship_imo == imo,
                                                 PortCall.date_utc >= date_from,
                                                 PortCall.move_type == "arrival").all()
    else:
        cached_portcalls = []

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

    # Store them in db so that we won't query them again
    for portcall in portcalls:
        try:
            session.add(portcall)
            session.commit()
        except sqlalchemy.exc.IntegrityError as e:
            session.rollback()
            if portcall.port_unlocode is None or portcall.port_unlocode=="":
                logger.debug("Port without unlocode. Probably an anchoring")
                continue
            # First try if this is a missing port
            if Port.query.filter(Port.unlocode==portcall.port_unlocode).count() == 0:
                port.insert_new_port(iso2=portcall.port_unlocode[0:2],
                                     unlocode=portcall.port_unlocode)

                # And try again
                session.add(portcall)
                session.commit()
            else:
                logger.warning("Failed to add portcall. Probably a duplicated portcall")
                continue

    return filtered_portcall


def update_departures(date_from=dt.date(2021, 11, 1),
                      date_to=dt.date.today() + dt.timedelta(days=1),
                      force_rebuild=False):
    """
    If force rebuild, we ignore cache port calls. Should only be used if we suspect
    we missed some port calls (e.g. in the initial fill using manually downloaded data)
    :param date_from:
    :param force_rebuild:
    :return:
    """
    print("=== Update departures (Portcall) ===")
    ports = Port.query.filter(Port.check_departure).all()

    for port in tqdm(ports):
        last_portcall = session.query(PortCall) \
            .filter(PortCall.port_unlocode==port.unlocode,
                    PortCall.move_type=="departure") \
            .order_by(PortCall.date_utc.desc()) \
            .first()

        if last_portcall is not None and not force_rebuild:
            date_from = last_portcall.date_utc + dt.timedelta(minutes=1)

        if not force_rebuild:
            filtered_portcall, portcalls = Marinetraffic.get_first_departure_portcall(unlocode=port.unlocode,
                                                                                      date_from=date_from,
                                                                                      date_to=date_to,
                                                                                      filter=None)
        else:
            portcalls = Marinetraffic.get_departure_portcalls_between_dates(unlocode=port.unlocode,
                                                                         date_from=date_from,
                                                                         date_to=date_to)

        # Store them in db so that we won't query them
        for portcall in portcalls:
            try:
                session.add(portcall)
                session.commit()
                if force_rebuild:
                    logger.info("Found a missing port call")
            except sqlalchemy.exc.IntegrityError as e:
                if "psycopg2.errors.UniqueViolation" in str(e):
                    logger.warning("Failed to upload portcall: duplicated port call")
                else:
                    logger.warning("Failed to upload portcall: %s" % (str(e),))
                session.rollback()
                continue

    return





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



def fill_arrival_gaps(imo = None, date_from=None, min_dwt=base.DWT_MIN):
    """
    We missed quite a lot of arrival data in the original filling. Since by default,
     the program is looking at arrivals after last departure, it will never look again
     at arrivals for previous departures.
     To solve this, we query again first arrival after problematic departures.
    :param imo:
    :param date_from:
    :param filter:
    :return:
    """

    query = PortCall.query
    if imo is not None:
        query = query.filter(PortCall.ship_imo == imo)

    if min_dwt is not None:
        query = query.join(Ship).filter(Ship.dwt >= min_dwt)

    if date_from is not None:
        query = query.filter(PortCall.date_utc >= to_datetime(date_from))

    portcall_df = pd.read_sql(query.statement, session.bind)

    # Isolate problematic ones: departure portcalls that are followed by another departure portcall
    portcall_df = portcall_df[~pd.isnull(portcall_df.port_unlocode)].copy()
    portcall_df['next_move_type'] = portcall_df.sort_values(['ship_imo', 'date_utc']) \
        .groupby("ship_imo")['move_type'].shift(-1)

    portcall_df = portcall_df.sort_values(['ship_imo', 'date_utc'])
    problematic_df = portcall_df[(portcall_df.move_type=="departure") \
                                 & (portcall_df.next_move_type=="departure") \
                                 & (portcall_df.load_status=='fully_laden') #TOOD this is just to start with most important ones
        ]

    for index, row in tqdm(problematic_df.iterrows(), total=problematic_df.shape[0]):
        new_portcall = get_first_arrival_portcall(imo=row.ship_imo, date_from=to_datetime(row.date_utc), use_cache=False)