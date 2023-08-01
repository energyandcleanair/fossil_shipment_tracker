import pandas as pd
import datetime as dt
import sqlalchemy as sa
from tqdm import tqdm
from sqlalchemy import func


import base
from base.logger import logger, logger_slack, slacker
from base.db import session
from base.db_utils import upsert
from base.utils import to_datetime, to_list, collapse_dates, remove_dates
from engine import ship
from engine import port
from engine.marinetraffic import Marinetraffic
from engine.datalastic import Datalastic

from base.models import PortCall, Port, Ship, Event, MarineTrafficCall, Shipment, Departure, Arrival
from base.models import DB_TABLE_PORTCALL

from engine.marinetraffic import MOVETYPE_DEPARTURE, MOVETYPE_ARRIVAL


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
    portcalls_df["others"] = portcalls_df.apply(
        lambda row: {"marinetraffic": {"DRAUGHT": str(row.draught)}}, axis=1
    )

    portcalls_df = portcalls_df.drop_duplicates(subset=["ship_imo", "move_type", "date_utc"])
    portcall_imos = portcalls_df.ship_imo.unique()

    # First ensure ships are in our database
    ship.fill(imos=portcall_imos)

    # Ensure ports are loaded in database
    if port.count() == 0:
        port.fill()

    ports_df = pd.read_sql(session.query(Port.id, Port.unlocode).statement, session.bind)

    portcalls_df = pd.merge(
        portcalls_df,
        ports_df.rename(columns={"id": "port_id", "unlocode": "port_unlocode"}),
    )

    portcalls_df = portcalls_df[
        [
            "ship_mmsi",
            "ship_imo",
            "port_id",
            "move_type",
            "load_status",
            "port_operation",
            "date_utc",
            "terminal_id",
            "berth_id",
            "others",
        ]
    ]

    from sqlalchemy.dialects.postgresql import JSONB

    portcalls_df["ship_mmsi"] = portcalls_df.ship_mmsi.apply(str)
    portcalls_df["ship_imo"] = portcalls_df.ship_imo.apply(str)
    portcalls_df["port_id"] = list([int(x) for x in portcalls_df["port_id"].values.astype(int)])
    import numpy as np

    portcalls_df.replace({np.nan: None}, inplace=True)

    # Upsert portcalls
    upsert(
        df=portcalls_df,
        table=DB_TABLE_PORTCALL,
        constraint_name="unique_portcall",
        dtype={"others": JSONB},
    )
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
        PortCall.port_operation == sa.null(),
        PortCall.port_id != sa.null(),
    ).all()

    for pc in tqdm(portcalls_to_update):
        new_pc = Marinetraffic.get_portcalls_between_dates(
            marinetraffic_port_id=pc.port_id,
            arrival_or_departure="departure",
            date_from=pc.date_utc - dt.timedelta(minutes=1),
            date_to=pc.date_utc + dt.timedelta(minutes=1),
        )
        if len(new_pc) == 1 and pc.port_id == new_pc[0].port_id:
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
        PortCall.others == sa.null(), PortCall.port_id == sa.null()
    ).all()

    for pc in tqdm(portcalls_to_update):
        new_pc = Marinetraffic.get_portcalls_between_dates(
            imo=pc.ship_imo,
            date_from=pc.date_utc - dt.timedelta(minutes=10),
            date_to=pc.date_utc + dt.timedelta(minutes=10),
        )
        if len(new_pc) == 1 and pc.date_utc == to_datetime(new_pc[0].date_utc):
            pc.others = new_pc[0].others
            pc.load_status = new_pc[0].load_status
            pc.port_operation = new_pc[0].port_operation
            pc.port_id = new_pc[0].port_id
            session.commit()

        else:
            logger.warning("Didn't find a single matching portcall")

    # Those that have MT info
    portcalls_to_update = PortCall.query.filter(
        PortCall.others != sa.null(), PortCall.port_id == sa.null()
    ).all()

    for pc in tqdm(portcalls_to_update):
        port_name = pc.others.get("marinetraffic", {}).get("PORT_NAME")
        mt_port_id = pc.others.get("marinetraffic", {}).get("PORT_ID")

        port_id = port.get_id(name=port_name, marinetraffic_id=mt_port_id, add_if_needed=True)
        if port_id:
            pc.port_id = port_id
            session.commit()


def upload_portcalls(portcalls):
    duplicated = 0

    # Store them in db so that we won't query them again
    if portcalls:
        print("Uploading %d portcalls" % (len(portcalls),))
    for portcall in portcalls:
        try:
            session.add(portcall)
            session.commit()
        except sa.exc.IntegrityError as e:
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

                    found = Datalastic.search_ports(name=name, marinetraffic_id=marinetraffic_id)
                    if found:
                        ratios = np.array(
                            [SequenceMatcher(None, x.name, name).ratio() for x in found]
                        )
                        if max(ratios) >= 0.9:
                            print(
                                "Best match: %s == %s (%f)"
                                % (name, found[ratios.argmax()].name, ratios.max())
                            )
                            found_and_filtered = found[ratios.argmax()]
                            if found_and_filtered:
                                session.add(found_and_filtered)
                                session.commit()
                                port_id = (
                                    session.query(Port.id)
                                    .filter(
                                        Port.iso2 == found_and_filtered.iso2,
                                        Port.name == found_and_filtered.name,
                                        Port.unlocode == found_and_filtered.unlocode,
                                    )
                                    .first()[0]
                                )
                                portcall.port_id = port_id
                            else:
                                print("wasn't close enough")

                # And try again
                try:
                    session.add(portcall)
                    session.commit()
                except sa.exc.IntegrityError as e:
                    session.rollback()
                    logger.warning(
                        "Failed to add portcall. Probably a problem with port: %s"
                        % (str(e).split("\n")[0])
                    )
                    continue
            else:
                if "unique_portcall" in str(e):
                    duplicated += 1
                else:
                    logger.warning("Failed to add portcall: %s" % (str(e).split("\n")[0]))
                continue

    if duplicated:
        logger.warning("Found %d duplicated portcalls." % (duplicated,))

    return


def get_next_portcall(
    date_from,
    arrival_or_departure=None,
    date_to=None,
    imo=None,
    unlocode=None,
    filter=None,
    use_cache=True,
    force_rebuild=True,
    cache_only=False,
    go_backward=False,
    use_call_based=False,
):
    date_from, date_to = to_datetime(date_from), to_datetime(date_to)

    # First look in DB
    if use_cache:
        cached_portcalls = PortCall.query
        if arrival_or_departure:
            cached_portcalls = cached_portcalls.filter(PortCall.move_type == arrival_or_departure)
        if go_backward:
            direction = -1
            cached_portcalls = cached_portcalls.filter(PortCall.date_utc < date_from)
            if date_to:
                cached_portcalls = cached_portcalls.filter(PortCall.date_utc >= date_to)
        else:
            direction = 1
            cached_portcalls = cached_portcalls.filter(PortCall.date_utc > date_from)
            if date_to:
                cached_portcalls = cached_portcalls.filter(PortCall.date_utc <= date_to)
        if imo:
            cached_portcalls = cached_portcalls.filter(PortCall.ship_imo.in_(to_list(imo)))
        if unlocode:
            cached_portcalls = cached_portcalls.join(Port, Port.id == PortCall.port_id).filter(
                Port.unlocode.in_(to_list(unlocode))
            )

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
        filtered_cached_portcalls.sort(key=lambda x: x.date_utc, reverse=go_backward)

        if cache_only:
            return filtered_cached_portcalls[0]

    if cache_only:
        return None

    if not force_rebuild:
        date_from_query, date_to_query = date_from, date_to
        if go_backward:
            date_from_query, date_to_query = date_to, date_from

        # check whether we called this ship imo in the MTCall table and get latest date
        portcall_queries = (
            session.query(
                MarineTrafficCall.params["fromdate"].astext.label("datefrom"),
                MarineTrafficCall.params["todate"].astext.label("dateto"),
            )
            .filter(
                MarineTrafficCall.method == base.VESSEL_PORTCALLS,
                MarineTrafficCall.status == base.HTTP_OK,
                sa.or_(
                    MarineTrafficCall.params.op("?")("imo"),
                    MarineTrafficCall.params.op("?")("unlocode"),
                ),
                sa.or_(
                    sa.and_(
                        (MarineTrafficCall.params["fromdate"].astext).cast(sa.TIMESTAMP)
                        >= date_from_query,
                        (MarineTrafficCall.params["fromdate"].astext).cast(sa.TIMESTAMP)
                        <= date_to_query,
                    ),
                    sa.and_(
                        (MarineTrafficCall.params["todate"].astext).cast(sa.TIMESTAMP)
                        >= date_from_query,
                        (MarineTrafficCall.params["todate"].astext).cast(sa.TIMESTAMP)
                        <= date_to_query,
                    ),
                ),
            )
            .order_by(MarineTrafficCall.params["todate"].astext.cast(sa.TIMESTAMP).asc())
        )

        if arrival_or_departure is None:
            portcall_queries = portcall_queries.filter(
                ~MarineTrafficCall.params.has_key("movetype")
            )
        else:
            portcall_queries = portcall_queries.filter(
                MarineTrafficCall.params["movetype"].astext
                == str(
                    {
                        "departure": MOVETYPE_DEPARTURE,
                        "arrival": MOVETYPE_ARRIVAL,
                    }[arrival_or_departure]
                )
            )

        if imo is not None:
            portcall_queries = portcall_queries.filter(
                MarineTrafficCall.params["imo"].astext == imo
            )

        if unlocode is not None:
            portcall_queries = portcall_queries.filter(
                MarineTrafficCall.params["unlocode"].astext == unlocode
            )

        portcall_query_dates = collapse_dates(
            [(to_datetime(p.datefrom), to_datetime(p.dateto)) for p in portcall_queries.all()]
        )

    # If not, query MarineTraffic
    flush = True
    # But do so only inbetween cached portcalls to avoid additional costs
    if cached_portcalls:
        portcalls = []
        direction = -1 if go_backward else 1
        cached_portcalls.sort(key=lambda x: x.date_utc, reverse=go_backward)

        # Only keep required intervals if we already have found matching ones in db
        if filtered_cached_portcalls:
            cached_portcalls = [
                x
                for x in cached_portcalls
                if (
                    (go_backward and x.date_utc > filtered_cached_portcalls[0].date_utc)
                    or (not go_backward and x.date_utc < filtered_cached_portcalls[0].date_utc)
                )
                and (x.date_utc != date_from)
            ]
            date_to = filtered_cached_portcalls[0].date_utc

        # IMPORTANT marinetraffic uses UTC for filtering
        date_froms = [to_datetime(date_from)] + [x.date_utc for x in cached_portcalls]
        date_tos = [x.date_utc for x in cached_portcalls] + [to_datetime(date_to)]

        for dates in list(zip(date_froms, date_tos)):
            date_from = dates[0] + direction * dt.timedelta(minutes=1)
            date_to = (
                dates[1] - direction * dt.timedelta(minutes=1) if dates[1] else dt.datetime.utcnow()
            )

            intervals = [(date_from, date_to)]
            if not force_rebuild:
                intervals = remove_dates(
                    (date_from, date_to), portcall_query_dates, go_backward=go_backward
                )

            for i in intervals:
                filtered_portcall, portcalls_interval = Marinetraffic.get_next_portcall(
                    imo=imo,
                    unlocode=unlocode,
                    date_from=i[0],
                    date_to=i[1],
                    filter=filter,
                    arrival_or_departure=arrival_or_departure,
                    go_backward=go_backward,
                    use_call_based=use_call_based,
                )

                if flush:
                    upload_portcalls(portcalls_interval)
                else:
                    portcalls.extend(portcalls_interval)
                if filtered_portcall:
                    return filtered_portcall

        # If we haven't found any new portcall,
        # we return the portcall that was already existing
        if filtered_cached_portcalls:
            return filtered_cached_portcalls[0]
        else:
            return None

    else:
        intervals = [(date_from, date_to)]
        if not force_rebuild:
            intervals = remove_dates(
                (date_from, date_to), portcall_query_dates, go_backward=go_backward
            )

        for i in intervals:
            filtered_portcall, portcalls = Marinetraffic.get_next_portcall(
                imo=imo,
                unlocode=unlocode,
                date_from=i[0],
                date_to=i[1],
                filter=filter,
                arrival_or_departure=arrival_or_departure,
                go_backward=go_backward,
                use_call_based=use_call_based,
            )

            upload_portcalls(portcalls)
            if filtered_portcall:
                return filtered_portcall

    return None


def update_departures(
    date_from="2022-01-01",
    date_to=dt.date.today() + dt.timedelta(days=1),
    unlocode=None,
    marinetraffic_port_id=None,
    port_id=None,
    departure_port_iso2=None,
    force_rebuild=False,
    between_existing_only=False,
    ignore_check_departure=False,
    use_call_based=False,
):
    """
    This function collects departure portcalls for ports which we have selected

    If force rebuild, we ignore cache port calls. Should only be used if we suspect
    we missed some port calls (e.g. in the initial fill using manually downloaded data)
    :param date_from:
    :param force_rebuild:
    :return:
    """
    try:
        logger_slack.info("=== Update departures (Portcall) ===")
        ports = session.query(Port)
        date_from = to_datetime(date_from)

        if use_call_based and between_existing_only:
            raise ValueError(
                """Most likely not what you want to use
                call_based and between_existing_only. Would burn lot of calls"""
            )

        if not ignore_check_departure:
            ports = ports.filter(Port.check_departure)

        if unlocode is not None:
            ports = ports.filter(Port.unlocode.in_(to_list(unlocode)))

        if marinetraffic_port_id is not None:
            ports = ports.filter(Port.marinetraffic_id.in_(to_list(marinetraffic_port_id)))

        if port_id is not None:
            ports = ports.filter(Port.id.in_(to_list(port_id)))

        if departure_port_iso2 is not None:
            ports = ports.filter(Port.iso2.in_(to_list(departure_port_iso2)))

        ports = ports.all()
        for port in tqdm(ports):
            # Three cases:
            # - only from last (force_rebuild=False)
            # - force rebuild between existing ones
            # - force rebuild all of them
            if not force_rebuild:
                last_portcall = (
                    session.query(PortCall)
                    .filter(
                        PortCall.port_id == port.id,
                        PortCall.move_type == "departure",
                        PortCall.date_utc <= to_datetime(date_to),
                    )
                    .order_by(PortCall.date_utc.desc())
                    .first()
                )

                if last_portcall is not None:
                    port_date_from = last_portcall.date_utc + dt.timedelta(minutes=1)
                else:
                    port_date_from = date_from

                date_bounds = [(port_date_from, to_datetime(date_to))]

            if force_rebuild and not between_existing_only:
                date_bounds = [(date_from, date_to)]

            if force_rebuild and between_existing_only:
                # Query existing portcalls, and use MT between existing portcalls only
                port_portcalls = (
                    session.query(PortCall)
                    .filter(
                        PortCall.port_id == port.id,
                        PortCall.move_type == "departure",
                        PortCall.date_utc >= to_datetime(date_from),
                        PortCall.date_utc <= to_datetime(date_to),
                    )
                    .order_by(PortCall.date_utc)
                    .all()
                )

                port_date_froms = [to_datetime(date_from)] + [
                    x.date_utc + dt.timedelta(minutes=1) for x in port_portcalls
                ]
                port_date_tos = [x.date_utc - dt.timedelta(minutes=1) for x in port_portcalls] + [
                    to_datetime(date_to)
                ]
                date_bounds = list(zip(port_date_froms, port_date_tos))

            for dates in date_bounds:
                query_date_from = dates[0]
                query_date_to = dates[1]

                portcalls = Marinetraffic.get_portcalls_between_dates(
                    arrival_or_departure="departure",
                    unlocode=port.unlocode,
                    marinetraffic_port_id=port.marinetraffic_id,
                    date_from=to_datetime(query_date_from),
                    date_to=to_datetime(query_date_to),
                    use_call_based=use_call_based,
                )

                # Store them in db so that we won't query them
                messages = {"missing": 0, "duplicated": 0, "uploaded": 0, "failed": 0}

                for portcall in portcalls:
                    try:
                        session.add(portcall)
                        session.commit()
                        if force_rebuild:
                            messages["missing"] += 1

                        messages["uploaded"] += 1
                    except sa.exc.IntegrityError as e:
                        if "psycopg2.errors.UniqueViolation" in str(e):
                            messages["duplicated"] += 1
                        else:
                            messages["failed"] += 1
                        session.rollback()
                        continue

                logger.info(
                    f"""{port.name} ({port.unlocode}) - {query_date_from} to {query_date_to}:
                    {messages['uploaded']} uploaded, {messages['missing']} missing, {messages['duplicated']} duplicated, {messages['failed']} failed"""
                )
    except Exception as e:
        logger_slack.error("Departure update failed")
        response = slacker.chat_postMessage(
            channel="#log-russia-counter", text="Please check error <@U012ZQ5NU4U>"
        )

    return


def find_arrival(departure, date_to=dt.datetime.utcnow(), cache_only=False):
    """
    Key function that will keep looking for subsequent portcalls
    to find where the boat stopped by looking at next departure with load_status in ballast
    :param departure_portcall:
    :return:
    """

    if departure.portcall_id is not None:
        departure_portcall = PortCall.query.filter(PortCall.id == departure.portcall_id).first()
        ship_imo, date_utc, load_status, is_sts = (
            departure_portcall.ship_imo,
            departure_portcall.date_utc,
            departure_portcall.load_status,
            False,
        )
    else:
        departure_event = Event.query.filter(Event.id == departure.event_id).first()
        ship_imo, date_utc, load_status, is_sts = (
            departure_event.interacting_ship_imo,
            departure_event.date_utc,
            base.FULLY_LADEN,
            True,
        )

    originally_checked_port_ids = [
        x for x, in session.query(Port.id).filter(Port.check_departure).all()
    ]

    # filter_departure = lambda x: x.port_operation in ["discharge", "both"]
    filter_departure_russia = (
        lambda x: x.port_id in originally_checked_port_ids and x.port_operation == "load"
    )
    filter_departure = lambda x: (
        load_status == base.FULLY_LADEN and x.load_status == base.IN_BALLAST and not is_sts
    ) or x.port_operation in ["discharge", "both"]
    filter_arrival = lambda x: x.port_id is not None

    # We query new departures if there is none between current portcall and next departure from Russia
    next_departure = get_next_portcall(
        date_from=date_utc + dt.timedelta(minutes=1),
        arrival_or_departure="departure",
        imo=ship_imo,
        cache_only=True,
        filter=filter_departure,
    )

    next_departure_russia = get_next_portcall(
        date_from=date_utc + dt.timedelta(minutes=1),
        arrival_or_departure="departure",
        imo=ship_imo,
        cache_only=True,
        filter=filter_departure_russia,
    )

    if (
        not next_departure
        or next_departure.port_id in [originally_checked_port_ids]
        or (
            next_departure_russia
            and to_datetime(next_departure.date_utc) > to_datetime(next_departure_russia.date_utc)
        )
    ):
        next_departure_date_to = (
            next_departure_russia.date_utc - dt.timedelta(minutes=1)
            if next_departure_russia
            else to_datetime(date_to)
        )

        next_departure_date_from = date_utc + dt.timedelta(minutes=1)
        next_departure = get_next_portcall(
            date_from=next_departure_date_from,
            date_to=next_departure_date_to,
            arrival_or_departure="departure",
            imo=ship_imo,
            use_cache=True,
            cache_only=cache_only,
            filter=filter_departure,
        )

        if next_departure is None and next_departure_russia is not None:
            next_departure = next_departure_russia

    if next_departure:
        # Then look backward for a relevant arrival
        # But only go until the next arrival (backward)
        cached_arrival = get_next_portcall(
            imo=next_departure.ship_imo,
            arrival_or_departure="arrival",
            date_from=next_departure.date_utc,
            date_to=date_utc,
            filter=filter_arrival,
            go_backward=True,
            cache_only=True,
        )

        # Return this one if we wanted to use cache only
        if cache_only:
            return cached_arrival

        if cached_arrival:
            date_to = cached_arrival.date_utc + dt.timedelta(minutes=1)
        else:
            date_to = date_utc

        arrival = get_next_portcall(
            imo=next_departure.ship_imo,
            arrival_or_departure="arrival",
            date_from=next_departure.date_utc,
            date_to=date_to,
            filter=filter_arrival,
            go_backward=True,
            cache_only=cache_only,
        )
        return arrival
    else:
        return None


def fill_departure_gaps(
    imo=None,
    commodities=[
        base.LNG,
        base.LPG,
        base.CRUDE_OIL,
        base.OIL_PRODUCTS,
        base.OIL_OR_CHEMICAL,
        base.BULK,
    ],
    date_from=None,
    date_to=None,
    unlocode=None,
    min_dwt=base.DWT_MIN,
):
    """
    Under the new strategy, we query departure portcall with discharge or both
    and then look backward to find closest arrival. We didn't fill departure portcalls beforehand.
    Doing it now, to prevent next departure to actually be the next one FROM Russia
    :param imo:
    :param date_from:
    :param filter:
    :return:
    """

    originally_checked_port_ids = [
        x for x, in session.query(Port.id).filter(Port.check_departure).all()
    ]
    originally_checked_port_unlocodes = [
        x for x, in session.query(Port.unlocode).filter(Port.check_departure).all()
    ]

    if unlocode is not None:
        originally_checked_port_unlocodes = [
            x for x in originally_checked_port_unlocodes if x in to_list(unlocode)
        ]

    # 1/2: update port departures from Russia
    filter_impossible = lambda x: False  # To force continuing
    for unlocode in tqdm(originally_checked_port_unlocodes):
        print(unlocode)
        next_departure = get_next_portcall(
            date_from=date_from,
            date_to=date_to,
            imo=imo,
            arrival_or_departure="departure",
            unlocode=unlocode,
            cache_only=False,
            filter=filter_impossible,
        )

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


def fill_gaps_within_shipments(
    ship_imo=None,
    date_from=None,
    min_dwt=base.DWT_MIN,
    max_time_delta=dt.timedelta(days=7),
):
    """
    For all completed shipments, look at departure portcalls.
    If gap between two departures is too wide, we query portcalls inbetween.
    """

    portcalls = (
        session.query(
            PortCall,
            func.lead(PortCall.date_utc)
            .over(partition_by=PortCall.ship_imo, order_by=PortCall.date_utc)
            .label("next_date_utc"),
        )
        .filter(PortCall.move_type == "departure")
        .subquery()
    )

    query = (
        session.query(
            Shipment.id,
            Departure.ship_imo,
            portcalls.c.date_utc,
            portcalls.c.next_date_utc,
        )
        .join(Departure, Departure.id == Shipment.departure_id)
        .join(Arrival, Arrival.id == Shipment.arrival_id)
        .join(
            portcalls,
            sa.and_(
                portcalls.c.ship_imo == Departure.ship_imo,
                portcalls.c.date_utc >= Departure.date_utc,
                portcalls.c.date_utc <= Arrival.date_utc,
            ),
        )
        .join(Ship, Departure.ship_imo == Ship.imo)
        .filter((portcalls.c.next_date_utc - portcalls.c.date_utc) >= max_time_delta)
    )

    if ship_imo is not None:
        query = query.filter(Departure.ship_imo.in_(to_list(ship_imo)))

    if min_dwt is not None:
        query = query.filter(Ship.dwt >= min_dwt)

    if date_from is not None:
        query = query.filter(portcalls.c.date_utc >= to_datetime(date_from))

    gaps = pd.read_sql(query.statement, session.bind)

    filter_departure = lambda x: x.port_operation in ["discharge", "both"]

    # For each shipment, we only fill gaps progressively,
    # until we find a matching departure portcall to save credits
    shipment_ids = gaps.id.unique()
    for shipment_id in tqdm(shipment_ids):
        shipment_gaps = gaps[gaps.id == shipment_id].sort_values(["date_utc"])
        next_portcall = None
        i = 0
        while not next_portcall and (i < len(shipment_gaps)):
            gap = shipment_gaps.iloc[
                i,
            ]
            new_portcall = get_next_portcall(
                arrival_or_departure="departure",
                imo=gap.ship_imo,
                date_from=to_datetime(gap.date_utc) + dt.timedelta(minutes=1),
                date_to=to_datetime(gap.next_date_utc) - dt.timedelta(minutes=1),
                use_cache=False,
                filter=filter_departure,
            )
            i = i + 1


def fill_gaps_within_shipments_using_mtcall(
    ship_imo=None,
    date_from=None,
    commodities=[base.CRUDE_OIL, base.OIL_PRODUCTS, base.LNG, base.LPG, base.OIL_OR_CHEMICAL],
    min_dwt=base.DWT_MIN,
    max_time_delta=dt.timedelta(days=7),
):
    """
    For all completed shipments, look at all calls made to MT
    and identify potential gaps
    """

    # sa.cast(MarineTrafficCall.params['imo'], sa.String)) \

    ship_imo = sa.func.regexp_replace(
        sa.func.regexp_replace(sa.cast(MarineTrafficCall.params["imo"], sa.String), '"', ""),
        '"',
        "",
    ).label("ship_imo")

    # Get hours for which
    calls = (
        session.query(
            sa.sql.expression.literal_column("0").label("dummy"),  # Necessary for the anti-join
            MarineTrafficCall.id,
            ship_imo,
            func.generate_series(
                func.date_trunc(
                    "hour",
                    sa.cast(
                        sa.cast(MarineTrafficCall.params["fromdate"], sa.String),
                        sa.DateTime,
                    ),
                ),
                func.date_trunc(
                    "hour",
                    sa.cast(
                        sa.cast(MarineTrafficCall.params["todate"], sa.String),
                        sa.DateTime,
                    ),
                ),
                "1 hour",
            ).label("date_utc"),
        )
        .join(Ship, Ship.imo == ship_imo)
        .filter(
            MarineTrafficCall.method == "portcalls/",
            sa.or_(
                MarineTrafficCall.params["movetype"] == sa.null(),
                sa.cast(MarineTrafficCall.params["movetype"], sa.String) == "departure",
            ),
            MarineTrafficCall.status == base.HTTP_OK,
        )
        .distinct()
    )

    if ship_imo is not None:
        calls = calls.filter(ship_imo.in_(to_list(ship_imo)))

    if date_from is not None:
        calls = calls.filter(
            sa.cast(sa.cast(MarineTrafficCall.params["todate"], sa.String), sa.DateTime)
            >= to_datetime(date_from)
        )

    if commodities is not None:
        calls = calls.filter(Ship.commodity.in_(to_list(commodities)))

    shipment_date_from = func.date_trunc("hour", Departure.date_utc).label("date_from")
    # The end of a shipment we consider is either its arrival (if completed),
    # its next departure (if undetected) or today (if ongoing)...
    shipment_date_to = func.date_trunc(
        "hour",
        func.coalesce(
            Arrival.date_utc,
            sa.func.lag(Departure.date_utc).over(
                Departure.ship_imo, order_by=Departure.date_utc.desc()
            ),
            dt.datetime.now(),
        ),
    ).label("date_to")

    shipments = (
        session.query(
            Departure.id.label("departure_id"),
            Departure.ship_imo,
            func.generate_series(shipment_date_from, shipment_date_to, "1 hour").label("date_utc"),
        )
        .outerjoin(Arrival, Arrival.departure_id == Departure.id)
        .join(Ship, Ship.imo == Departure.ship_imo)
        .distinct()
    )

    if ship_imo is not None:
        shipments = shipments.filter(Departure.ship_imo.in_(to_list(ship_imo)))

    if date_from is not None:
        shipments = shipments.filter(Departure.date_utc >= to_datetime(date_from))

    if commodities is not None:
        shipments = shipments.filter(Ship.commodity.in_(to_list(commodities)))

    calls = calls.subquery()
    shipments = shipments.subquery()

    missing_parts = (
        session.query(shipments)
        .outerjoin(
            calls,
            sa.and_(
                calls.c.ship_imo == shipments.c.ship_imo,
                calls.c.date_utc == shipments.c.date_utc,
            ),
        )
        .filter(calls.c.dummy == sa.null())
    )

    missing_parts_df = pd.read_sql(missing_parts.statement, session.bind)
    print(missing_parts_df)
