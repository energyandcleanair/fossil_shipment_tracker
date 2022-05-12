import pandas as pd
import datetime as dt
import sqlalchemy
from tqdm import tqdm

import base
from base.logger import logger, logger_slack
from base.db import session
from base.db_utils import upsert
from base.models import DB_TABLE_PORTCALL
from base.utils import to_datetime, to_list
from engine import ship
from engine import port
from engine.marinetraffic import Marinetraffic

from base.models import PortCall, Port, Ship


def initial_fill(limit=None):
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
    portcalls_df["others"] = portcalls_df.apply(lambda row: {"marinetraffic":{"DRAUGHT": str(row.draught)}}, axis=1)


    portcalls_df = portcalls_df.drop_duplicates(subset=["ship_imo", "move_type", "date_utc"])
    portcall_imos = portcalls_df.ship_imo.unique()

    # First ensure ships are in our database
    ship.fill(imos=portcall_imos)

    # Ensure ports are loaded in database
    if port.count() == 0:
        port.fill()

    ports_df = pd.read_sql(session.query(Port.id, Port.unlocode).statement,
                           session.bind)

    portcalls_df = pd.merge(portcalls_df, ports_df.rename(columns={"id":"port_id", "unlocode":"port_unlocode"}))

    portcalls_df = portcalls_df[["ship_mmsi", "ship_imo", "port_id", "move_type",
                                 "load_status", "port_operation", "date_utc",
                                 "terminal_id", "berth_id", "others"]]

    from sqlalchemy.dialects.postgresql import JSONB

    portcalls_df['ship_mmsi'] = portcalls_df.ship_mmsi.apply(str)
    portcalls_df['ship_imo'] = portcalls_df.ship_imo.apply(str)
    portcalls_df['port_id'] = list([int(x) for x in portcalls_df['port_id'].values.astype(int)])
    import numpy as np
    portcalls_df.replace({np.nan: None}, inplace=True)

    # Upsert portcalls
    upsert(df=portcalls_df, table=DB_TABLE_PORTCALL, constraint_name="unique_portcall",
           dtype={'others': JSONB})
    session.commit()
    return


def fill_missing_port_operation():
    """
    We queried initially with MT v1, which misses PORT_OPERATION field.
    Here we take all already loaded arrival portcalls and query again MT
    :return:
    """
    portcalls_to_update = PortCall.query.filter(
        PortCall.move_type == "departure",
        PortCall.port_operation == sqlalchemy.null(),
        PortCall.port_id != sqlalchemy.null()).all()

    for pc in tqdm(portcalls_to_update):
        new_pc = Marinetraffic.get_departure_portcalls_between_dates(port_id=pc.port_id,
                                                                  date_from=pc.date_utc - dt.timedelta(minutes=1),
                                                                  date_to=pc.date_utc + dt.timedelta(minutes=1))
        if len(new_pc) == 1 \
                and pc.port_id == new_pc[0].port_id:
            pc.others = new_pc[0].others
            pc.load_status = new_pc[0].load_status
            pc.port_operation = new_pc[0].port_operation
            session.commit()

        else:
            logger.warning("Didn't find a single matching portcall")


def fill_missing_port_id():
    """
    For manually collected portcalls that no port_unlocode info,
    we need to query using api to at least have MT port_id and port_name
    :return:
    """
    portcalls_to_update = PortCall.query.filter(
        PortCall.others == sqlalchemy.null(),
        PortCall.port_id == sqlalchemy.null()).all()

    for pc in tqdm(portcalls_to_update):
        new_pc = Marinetraffic.get_portcalls_between_dates(imo=pc.ship_imo,
                                                           date_from=pc.date_utc - dt.timedelta(minutes=10),
                                                           date_to=pc.date_utc + dt.timedelta(minutes=10))
        if len(new_pc) == 1 \
                and pc.date_utc == to_datetime(new_pc[0].date_utc):
            pc.others = new_pc[0].others
            pc.load_status = new_pc[0].load_status
            pc.port_operation = new_pc[0].port_operation
            pc.port_id = new_pc[0].port_id
            session.commit()

        else:
            logger.warning("Didn't find a single matching portcall")


def upload_portcalls(portcalls):
    # Store them in db so that we won't query them again
    if portcalls:
        print("Uploading %d portcalls" % (len(portcalls),))
    for portcall in portcalls:
        try:
            session.add(portcall)
            session.commit()
        except sqlalchemy.exc.IntegrityError as e:
            session.rollback()

            # First try if this is a missing port
            if portcall.port_id is None and not "unique_portcall" in str(e):
                unlocode = portcall.others.get("marinetraffic", {}).get("UNLOCODE")
                name = portcall.others.get("marinetraffic", {}).get("PORT_NAME")
                marinetraffic_id = portcall.others.get("marinetraffic", {}).get("PORT_ID")

                if unlocode is None:
                    from engine.datalastic import Datalastic
                    from difflib import SequenceMatcher
                    import numpy as np
                    found = Datalastic.get_port_infos(name=name, marinetraffic_id=marinetraffic_id)
                    if found:
                        ratios = np.array([SequenceMatcher(None, x.name, name).ratio() for x in found])
                        if max(ratios) >= 0.9:
                            print("Best match: %s == %s (%f)" % (name, found[ratios.argmax()].name, ratios.max()))
                            found_and_filtered = found[ratios.argmax()]
                            if found_and_filtered:
                                session.add(found_and_filtered)
                                session.commit()
                                port_id = session.query(Port.id).filter(Port.iso2==found_and_filtered.iso2,
                                                              Port.name==found_and_filtered.name,
                                                              Port.unlocode==found_and_filtered.unlocode).first()[0]
                                portcall.port_id = port_id
                            else:
                                print("wasn't close enough")


                # And try again
                try:
                    session.add(portcall)
                    session.commit()
                except sqlalchemy.exc.IntegrityError as e:
                    session.rollback()
                    logger.warning("Failed to add portcall. Probably a problem with port: %s"%(str(e).split("\n")[0]))
                    continue
            else:
                if "unique_portcall" in str(e):
                    logger.warning("Failed to add portcall. Duplicated portcall: %s" % (str(e).split("\n")[0]))
                else:
                    logger.warning("Failed to add portcall: %s" % (str(e).split("\n")[0]))
                continue


def get_next_portcall(date_from,
                      arrival_or_departure,
                      date_to=None,
                      imo=None,
                      unlocode=None,
                      filter=None,
                      use_cache=True,
                      cache_only=False,
                      go_backward=False):

    # First look in DB
    if use_cache:
        cached_portcalls = PortCall.query.filter(PortCall.move_type == arrival_or_departure)
        if go_backward:
            direction = -1
            cached_portcalls = cached_portcalls.filter(PortCall.date_utc <= date_from)
            if date_to:
                cached_portcalls = cached_portcalls.filter(PortCall.date_utc >= date_to)
        else:
            direction = 1
            cached_portcalls = cached_portcalls.filter(PortCall.date_utc >= date_from)
            if date_to:
                cached_portcalls = cached_portcalls.filter(PortCall.date_utc <= date_to)
        if imo:
            cached_portcalls = cached_portcalls.filter(PortCall.ship_imo.in_(to_list(imo)))
        if unlocode:
            cached_portcalls = cached_portcalls.join(Port, Port.id == PortCall.port_id).filter(Port.unlocode.in_(to_list(unlocode)))

        cached_portcalls = cached_portcalls.all()
    else:
        cached_portcalls = []

    filtered_cached_portcalls = None

    if filter is not None:
        filtered_cached_portcalls = [x for x in cached_portcalls if filter(x)]
    else:
        filtered_cached_portcalls = cached_portcalls

    if filtered_cached_portcalls:
        # We found a matching portcall in db
        if go_backward:
            filtered_cached_portcalls.sort(key=lambda x: x.date_utc, reverse=True)
        else:
            filtered_cached_portcalls.sort(key=lambda x: x.date_utc)
        if cache_only:
            return filtered_cached_portcalls[0]

    if cache_only:
        return None

    # If not, query MarineTraffic
    flush = True
    # But do so only inbetween cached portcalls to avoid additional costs
    if cached_portcalls:
        portcalls = []
        direction = -1 if go_backward else 1
        cached_portcalls.sort(key=lambda x: x.date_utc, reverse=go_backward)
        #IMPORTANT marinetraffic uses UTC for filtering
        date_froms = [to_datetime(date_from)] + [x.date_utc for x in cached_portcalls]
        date_tos = [x.date_utc for x in cached_portcalls] + [to_datetime(date_to)]
        for dates in list(zip(date_froms, date_tos)):
            date_from = dates[0] + direction * dt.timedelta(minutes=1)
            date_to = dates[1] - direction * dt.timedelta(minutes=1) if dates[1] else dt.datetime.utcnow()

            filtered_portcall, portcalls_interval = Marinetraffic.get_next_portcall(imo=imo,
                                                   unlocode=unlocode,
                                                   date_from=date_from,
                                                   date_to=date_to,
                                                   filter=filter,
                                                   arrival_or_departure=arrival_or_departure,
                                                   go_backward=go_backward
                                                   )

            if flush:
                upload_portcalls(portcalls_interval)
            else:
                portcalls.extend(portcalls_interval)
            if filtered_portcall:
                break

    else:
        filtered_portcall, portcalls = Marinetraffic.get_next_portcall(imo=imo,
                                                                       unlocode=unlocode,
                                                                       date_from=date_from,
                                                                       date_to=date_to,
                                                                       arrival_or_departure=arrival_or_departure,
                                                                       go_backward=go_backward)

        upload_portcalls(portcalls)

    return filtered_portcall


def update_departures_from_russia(
        date_from="2022-01-01",
        date_to=dt.date.today() + dt.timedelta(days=1),
        unlocode=None,
        marinetraffic_port_id=None,
        force_rebuild=False):
    """
    If force rebuild, we ignore cache port calls. Should only be used if we suspect
    we missed some port calls (e.g. in the initial fill using manually downloaded data)
    :param date_from:
    :param force_rebuild:
    :return:
    """
    logger_slack.info("=== Update departures (Portcall) ===")
    ports = Port.query.filter(Port.check_departure)\

    if unlocode is not None:
        ports = ports.filter(Port.unlocode.in_(to_list(unlocode)))

    if marinetraffic_port_id is not None:
        ports = ports.filter(Port.marinetraffic_id.in_(to_list(marinetraffic_port_id)))

    for port in tqdm(ports.all()):
        last_portcall = session.query(PortCall) \
            .filter(PortCall.port_id==port.id,
                    PortCall.move_type=="departure",
                    PortCall.date_utc <= to_datetime(date_to)) \
            .order_by(PortCall.date_utc.desc()) \
            .first()

        if last_portcall is not None and not force_rebuild:
            date_from = last_portcall.date_utc + dt.timedelta(minutes=1)


        portcalls = Marinetraffic.get_portcalls_between_dates(arrival_or_departure="departure",
                                                              unlocode=port.unlocode,
                                                              marinetraffic_port_id=port.marinetraffic_id,
                                                              date_from=to_datetime(date_from),
                                                              date_to=to_datetime(date_to))

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



def find_arrival(departure_portcall,
                 date_to=dt.datetime.utcnow(),
                 cache_only=False):
    """
    Key function that will keep looking for subsequent portcalls
    to find where the boat stopped by looking at next departure with load_status in ballast
    :param departure_portcall:
    :return:
    """
    originally_checked_port_ids = [x for x, in session.query(Port.id).filter(Port.check_departure).all()]

    # filter_departure = lambda x: x.port_operation in ["discharge", "both"]
    filter_departure_russia = lambda x: x.port_id in originally_checked_port_ids and x.port_operation == 'load'
    filter_departure = lambda x: (departure_portcall.load_status == base.FULLY_LADEN and x.load_status == base.IN_BALLAST) \
                                 or x.port_operation in ["discharge", "both"]
    filter_arrival = lambda x: x.port_id is not None

    # We query new departures only if there is none between current portcall and next departure from russia
    next_departure = get_next_portcall(date_from=departure_portcall.date_utc + dt.timedelta(minutes=1),
                                       arrival_or_departure="departure",
                                       imo=departure_portcall.ship_imo,
                                       cache_only=True,
                                       filter=filter_departure)

    next_departure_russia = get_next_portcall(date_from=departure_portcall.date_utc + dt.timedelta(minutes=1),
                                              arrival_or_departure="departure",
                                              imo=departure_portcall.ship_imo,
                                              cache_only=True,
                                              filter=filter_departure_russia)

    if not next_departure \
            or not next_departure_russia \
            or next_departure.port_id in [originally_checked_port_ids] \
            or to_datetime(next_departure.date_utc) > to_datetime(
        next_departure_russia.date_utc):  # to_datetime(p.date_utc) + dt.timedelta(hours=24):

        next_departure_date_to = next_departure_russia.date_utc - dt.timedelta(
            minutes=1) if next_departure_russia else to_datetime(date_to)

        next_departure_date_from = departure_portcall.date_utc + dt.timedelta(minutes=1)
        next_departure = get_next_portcall(date_from=next_departure_date_from,
                                           date_to=next_departure_date_to,
                                           arrival_or_departure="departure",
                                           imo=departure_portcall.ship_imo,
                                           use_cache=True,
                                           filter=filter_departure)

        if next_departure is None and next_departure_russia is not None:
            next_departure = next_departure_russia

    if next_departure:
        # Then look backward for a relevant arrival
        # But only go until the next arrival (backward)
        cached_arrival = get_next_portcall(imo=next_departure.ship_imo,
                                    arrival_or_departure="arrival",
                                    date_from=next_departure.date_utc,
                                    date_to=departure_portcall.date_utc,
                                    filter=filter_arrival,
                                    go_backward=True,
                                    cache_only=True)

        # Return this one if we wanted to use cache only
        if cache_only:
            return cached_arrival

        if cached_arrival:
            date_to = cached_arrival.date_utc + dt.timedelta(minutes=1)
        else:
            date_to = departure_portcall.date_utc

        arrival = get_next_portcall(imo=next_departure.ship_imo,
                                    arrival_or_departure="arrival",
                                    date_from=next_departure.date_utc,
                                    date_to=date_to,
                                    filter=filter_arrival,
                                    go_backward=True,
                                    cache_only=False)
        return arrival
    else:
        return None


def fill_departure_gaps(imo=None,
                        commodities=[base.LNG,
                            base.CRUDE_OIL,
                            base.OIL_PRODUCTS,
                            base.OIL_OR_CHEMICAL,
                            base.BULK],
                        date_from=None,
                        date_to=None,
                        unlocode=None,
                        min_dwt=base.DWT_MIN):

    """
    Under the new strategy, we query departure portcall with discharge or both
    and then look backward to find closest arrival. We didn't fill departure portcalls beforehand.
    Doing it now, to prevent next departure to actually be the next one FROM Russia
    :param imo:
    :param date_from:
    :param filter:
    :return:
    """

    originally_checked_port_ids = [x for x, in session.query(Port.id).filter(Port.check_departure).all()]
    originally_checked_port_unlocodes = [x for x, in session.query(Port.unlocode).filter(Port.check_departure).all()]

    if unlocode is not None:
        originally_checked_port_unlocodes = [x for x in originally_checked_port_unlocodes if x in to_list(unlocode)]

    # 1/2: update port departures from Russia
    filter_impossible = lambda x: False # To force continuing
    for unlocode in tqdm(originally_checked_port_unlocodes):
        print(unlocode)
        next_departure = get_next_portcall(date_from=date_from,
                                           date_to=date_to,
                                           imo=imo,
                                           arrival_or_departure="departure",
                                           unlocode=unlocode,
                                           cache_only=False,
                                           filter=filter_impossible)


    # 2/2: update subsequent departure calls for ships leaving
    # query = PortCall.query.filter(
    #     PortCall.move_type == "departure",
    #     PortCall.load_status.in_([base.FULLY_LADEN]),
    #     PortCall.port_operation.in_(["load"])) \
    #     .join(Ship, Port).filter(Port.check_departure)
    #
    # if min_dwt is not None:
    #     query = query.filter(Ship.dwt >= min_dwt)
    #
    # if date_from is not None:
    #     query = query.filter(PortCall.date_utc >= to_datetime(date_from))
    #
    # if date_to is not None:
    #     query = query.filter(PortCall.date_utc <= to_datetime(date_to))
    #
    # if commodities:
    #     query = query.filter(Ship.commodity.in_(commodities))
    #
    # if imo:
    #     query = query.filter(Ship.imo.in_(to_list(imo)))
    #
    # portcall_russia = query.all()
    #
    # portcall_russia.sort(key=lambda x: x.date_utc)
    #
    # for p in tqdm(portcall_russia):
    #     find_arrival(departure_portcall=p, date_to=date_to)


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
                                 & (portcall_df.load_status == base.FULLY_LADEN) #TOOD this is just to start with most important ones
        ]

    for index, row in tqdm(problematic_df.iterrows(), total=problematic_df.shape[0]):
        new_portcall = get_next_portcall(arrival_or_departure="arrival", imo=row.ship_imo, date_from=to_datetime(row.date_utc), use_cache=False)



