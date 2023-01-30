import pandas as pd
import sqlalchemy as sa
from tqdm import tqdm
import datetime as dt

from engine import portcall
from engine import departure
from engine.marinetraffic import Marinetraffic, MOVETYPE_DEPARTURE, MOVETYPE_ARRIVAL
import base
from base.logger import logger
from base.db import session
from base.utils import to_datetime, to_list
from base.models import (
    Departure,
    Shipment,
    Ship,
    MarineTrafficCall,
    Arrival,
    Port,
)

MAX_DAYS = 189  # Need to split above MAX_DAYS (MT limitation)
MIN_DAYS = 10  # Not worth using the call-based key uncer MIN_DAYS


tqdm.pandas()


def update(
    date_from,
    date_to,
    departure_port_iso2,
    commodities=[base.CRUDE_OIL, base.OIL_PRODUCTS, base.LNG],
):

    # This call is to update history using the CALL-BASED MARINE TRAFFIC KEY
    # meaning we'll try to maximize number of records captured per call
    # and minimize the number of calls made
    # i.e. the opposite of the RECORD-BASED MARINE TRAFFIC KEY
    #
    # We use it to fill historical data and laundromat countries

    update_departures(
        date_from=date_from, date_to=date_to, departure_port_iso2=departure_port_iso2
    )

    departure.update(date_from=date_from)

    update_arrivals(
        date_from=date_from,
        date_to=date_to,
        commodities=commodities,
        departure_port_iso2=departure_port_iso2,
    )


def get_queried_port_hours(port_id, date_from=None):
    """
    Return list of hours already succesfully queried, so that we don't query these dates again.
    :param port_id:
    :param date_from:
    :return:
    """
    # Get information on calls already made to MT
    queried = session.query(
        MarineTrafficCall.params["portid"].label("port_id"),
        MarineTrafficCall.params["fromdate"].label("date_from"),
        MarineTrafficCall.params["todate"].label("date_to"),
        MarineTrafficCall.date_utc,  # call date
        MarineTrafficCall.records,
    ).filter(
        MarineTrafficCall.method == "portcalls/",
        sa.or_(
            MarineTrafficCall.params["movetype"] == sa.null(),
            MarineTrafficCall.params["movetype"] == str(MOVETYPE_DEPARTURE),
        ),
        MarineTrafficCall.status == base.HTTP_OK,
        MarineTrafficCall.params["portid"] == '"%s"' % (port_id),
    )

    if date_from:
        queried = queried.filter(MarineTrafficCall.date_utc >= date_from)

    queried_df = pd.read_sql(queried.statement, session.bind)

    if len(queried_df):
        queried_df["date_from"] = pd.to_datetime(queried_df.date_from)
        queried_df["date_to"] = pd.to_datetime(queried_df.date_to)
        # Capping date_to to account for potential Marine Traffic latency
        queried_df["date_to_cap"] = pd.to_datetime(queried_df.date_utc) - dt.timedelta(
            hours=base.MARINETRAFFIC_LATENCY_HOURS
        )
        queried_df["date_to"] = queried_df[["date_to", "date_to_cap"]].min(axis=1)
        queried_df["dates"] = queried_df.apply(
            lambda row: pd.date_range(
                row.date_from.ceil("H"), row.date_to.floor("H"), freq="H"
            ),
            axis=1,
        )

        return (
            queried_df.explode("dates").drop_duplicates().dates.sort_values().tolist()
        )
    else:
        return []


def get_queried_ship_hours(ship_imo, date_from=None):
    """
    Return list of hours already succesfully queried, so that we don't query these dates again.
    :param port_id:
    :param date_from:
    :return:
    """
    # Get information on calls already made to MT
    queried = session.query(
        MarineTrafficCall.params["imo"].label("imo"),
        MarineTrafficCall.params["fromdate"].label("date_from"),
        MarineTrafficCall.params["todate"].label("date_to"),
        MarineTrafficCall.date_utc,  # call date
        MarineTrafficCall.records,
    ).filter(
        MarineTrafficCall.method == "portcalls/",
        MarineTrafficCall.params["movetype"] == sa.null(),
        MarineTrafficCall.status == base.HTTP_OK,
        MarineTrafficCall.params["imo"] == '"%s"' % (ship_imo),
    )

    if date_from:
        queried = queried.filter(MarineTrafficCall.date_utc >= date_from)

    queried_df = pd.read_sql(queried.statement, session.bind)

    if len(queried_df):
        queried_df["date_from"] = pd.to_datetime(queried_df.date_from)
        queried_df["date_to"] = pd.to_datetime(queried_df.date_to)
        # Capping date_to to account for potential Marine Traffic latency
        queried_df["date_to_cap"] = pd.to_datetime(queried_df.date_utc) - dt.timedelta(
            hours=base.MARINETRAFFIC_LATENCY_HOURS
        )
        queried_df["date_to"] = queried_df[["date_to", "date_to_cap"]].min(axis=1)
        queried_df["dates"] = queried_df.apply(
            lambda row: pd.date_range(
                row.date_from.floor("H"), row.date_to.floor("H"), freq="H"
            ),
            axis=1,
        )

        return (
            queried_df.explode("dates").drop_duplicates().dates.sort_values().tolist()
        )
    else:
        return []


def wanted_dates_to_hours(date_from, date_to):
    return pd.date_range(date_from, date_to, freq="H")


def wanted_intervals_to_hours(wanted_intervals):
    wanted_intervals["dates"] = wanted_intervals.apply(
        lambda row: pd.date_range(
            row.date_from.floor("H"), row.date_to.floor("H"), freq="H"
        ),
        axis=1,
    )

    wanted_hours = (
        wanted_intervals.explode("dates").dates.drop_duplicates().sort_values()
    )

    return wanted_hours


def get_intervals(
    wanted_hours=None,
    wanted_intervals=None,
    date_from=None,
    date_to=None,
    queried_hours=[],
    merge_under_max_days=True,
    extend=True,
):
    """
    Build intervals to auery, based on date_from, date_to, and the hours that have already been queried.
    It two intervals are relatively close, and don't span over MAX_DAYS, than we merge them, to reduce
    the number of queries
    :param date_from:
    :param date_to:
    :param queried_hours:
    :return:
    """

    if not wanted_hours:
        if wanted_intervals is not None:
            wanted_hours = wanted_intervals_to_hours(wanted_intervals=wanted_intervals)
        elif date_from and date_to:
            wanted_hours = wanted_dates_to_hours(date_from=date_from, date_to=date_to)
        else:
            raise ValueError(
                "Need to specify either wanted_hours, wanted_intervals or date_from/date_to"
            )

    wanted_hours = pd.Series(wanted_hours)
    all_hours = wanted_hours[~wanted_hours.isin(queried_hours)]

    # Create a DataFrame from all_dates
    df = pd.DataFrame({"datetime": all_hours})

    # Add a column with the difference between consecutive datetime objects
    df["diff"] = (df["datetime"] - df["datetime"].shift()).dt.total_seconds()

    # Add a column with a group number for consecutive datetime objects
    df["group"] = (df["diff"] != 3600).cumsum()

    # Group the DataFrame by the group number
    grouped = df.groupby("group")

    # Use the agg function to extract the first and last datetime object of each group
    intervals = grouped["datetime"].agg(["first", "last"])

    # Rename the columns to date_from and date_to
    intervals.columns = ["date_from", "date_to"]

    # Split those above MAX_DAYS
    split_rows = []
    for _, row in intervals.iterrows():
        date_diff = row["date_to"] - row["date_from"]
        if date_diff.days > MAX_DAYS:
            new_row1 = {
                "date_from": row["date_from"],
                "date_to": row["date_from"] + pd.Timedelta(MAX_DAYS, unit="d"),
            }
            new_row2 = {
                "date_from": row["date_from"] + pd.Timedelta(MAX_DAYS, unit="d"),
                "date_to": row["date_to"],
            }
            split_rows.append(new_row1)
            split_rows.append(new_row2)
        else:
            split_rows.append({
                "date_from": row["date_from"],
                "date_to": row["date_to"]
            })

    intervals = pd.DataFrame(split_rows, columns=["date_from", "date_to"])

    # Merge consecutive intervals if they are within MAX_DAYS
    # Reset the index of the intervals DataFrame
    intervals = intervals.reset_index(drop=True)
    intervals.sort_values("date_from", inplace=True)

    if merge_under_max_days:
        i = 0
        while i < len(intervals) - 1:
            if intervals.loc[i + 1, "date_to"] <= intervals.loc[
                i, "date_from"
            ] + pd.Timedelta(days=MAX_DAYS):
                intervals.loc[i, "date_to"] = max(
                    intervals.loc[i, "date_to"], intervals.loc[i + 1, "date_to"]
                )
                intervals = intervals.drop(i + 1)
                intervals = intervals.reset_index(drop=True)
            else:
                i += 1

    if extend and len(intervals) > 0:

        def extend_date_to(row):
            date_from = pd.to_datetime(row["date_from"])
            date_to = pd.to_datetime(row["date_to"])
            now = dt.datetime.now()
            diff = date_to - date_from
            if diff < dt.timedelta(days=MAX_DAYS) and diff > dt.timedelta(
                days=MIN_DAYS
            ):
                date_to = min(
                    date_from + dt.timedelta(days=MAX_DAYS), now - dt.timedelta(hours=1)
                )
                date_to = pd.to_datetime(date_to).floor("H")

            return date_to

        intervals["date_to"] = intervals.apply(extend_date_to, axis=1)

    return intervals.to_dict(orient="records")


def update_departures(date_from, date_to=None, departure_port_iso2=None):
    date_from = to_datetime(date_from)
    date_to = to_datetime(date_to) if date_to else dt.datetime.now()

    # Otherwise, we would think we queried portcalls that we actually didn't
    assert date_to < dt.datetime.now()

    ports = session.query(Port).filter(Port.check_departure)
    if departure_port_iso2:
        ports = ports.filter(Port.iso2.in_(to_list(departure_port_iso2)))

    ports = ports.all()

    for port in tqdm(ports):

        port_id = port.unlocode or port.marinetraffic_id
        queried_hours = get_queried_port_hours(port_id=port_id, date_from=date_from)
        intervals = get_intervals(
            date_from=date_from, date_to=date_to, queried_hours=queried_hours
        )
        for interval in intervals:

            if interval["date_to"] - interval["date_from"] > dt.timedelta(
                days=MIN_DAYS
            ):

                portcalls = Marinetraffic.get_portcalls_between_dates(
                    arrival_or_departure="departure",
                    unlocode=port.unlocode,
                    marinetraffic_port_id=port.marinetraffic_id,
                    date_from=interval["date_from"],
                    date_to=interval["date_to"],
                    use_call_based=True,
                )
                portcall.upload_portcalls(portcalls)

            else:
                logger.warning(
                    "Not worth using call-based key. Skipping for port %s, interval %s"
                    % (port_id, interval)
                )


def update_arrivals(
    commodities,  # Forcing a choice to avoid wasting credits
    date_from,
    date_to=dt.datetime.now(),
    ship_imo=None,
    departure_port_iso2=None,
    use_credit_key_if_short=False,
):
    """
    Update arrivals using callbased key for when we want to save credits and collect over long period

    Parameters
    ----------
    commodities : commodities to filter for
    date_from : date from
    date_to : date to
    ship_imo : ship imo
    departure_port_iso2 : port iso2 to filter for
    use_credit_key_if_short : whether to resort to credit based key if time interval is short

    Returns
    -------

    """
    date_to = to_datetime(date_to) if date_to else dt.datetime.now()
    # Otherwise, we would think we queried portcalls that we actually didn't
    assert date_to < dt.datetime.now()

    query_departure = (
        session.query(
            Departure.id,
            Departure.ship_imo.label("imo"),
            Shipment.status,
            Ship.commodity,
            Ship.dwt,
            Departure.date_utc.label("departure_date"),
            Arrival.date_utc.label("arrival_date"),
        )
        .join(Ship, Ship.imo == Departure.ship_imo)
        .join(Port, Port.id == Departure.port_id)
        .outerjoin(Arrival, Arrival.departure_id == Departure.id)
        .outerjoin(Shipment, Shipment.departure_id == Departure.id)
        .filter(Ship.commodity.in_(to_list(commodities)))
        .filter(
            Departure.date_utc >= to_datetime(date_from),
            Departure.date_utc <= to_datetime(date_to),
        )
    )

    if departure_port_iso2:
        query_departure = query_departure.filter(
            Port.iso2.in_(to_list(departure_port_iso2))
        )

    if ship_imo:
        query_departure = query_departure.filter(
            Ship.imo.in_(to_list(ship_imo))
        )

    # Get departures of interest
    departures = pd.read_sql(query_departure.statement, session.bind)
    departures["next_departure_date"] = (
        departures.sort_values(by=["departure_date"])
        .groupby("imo")["departure_date"]
        .shift(-1)
        .fillna(dt.datetime.utcnow())
    )

    departures["now"] = dt.datetime.now()
    departures["date_to"] = (
        departures[["arrival_date", "next_departure_date", "now"]]
        .bfill(axis=1)
        .iloc[:, 0]
    )

    departures = departures[~departures.imo.str.contains("NOTFOUND")]

    imos = departures.imo.unique()

    for imo in tqdm(imos):
        ship_departures = departures[departures.imo == imo]
        wanted_intervals = ship_departures[["departure_date", "date_to"]].rename(
            columns={"departure_date": "date_from"}
        )

        queried_hours = get_queried_ship_hours(
            ship_imo=imo, date_from=wanted_intervals.date_from.min()
        )
        intervals = get_intervals(
            wanted_intervals=wanted_intervals, queried_hours=queried_hours
        )

        # Only consider those invervals that start before date_to
        intervals = [x for x in intervals if x["date_from"] < date_to]

        for interval in intervals:
            if interval["date_to"] - interval["date_from"] > dt.timedelta(
                days=MIN_DAYS
            ):

                portcalls = portcall.get_next_portcall(
                    date_from=interval["date_from"],
                    date_to=interval["date_to"],
                    arrival_or_departure=None,
                    imo=imo,
                    use_call_based=True,
                    use_cache=False,  # IMPORTANT, so that it multiply queries
                    filter=lambda x: False,
                )
                # REMINDER: The function uploads portcalls
                # Hence no upload.portcalls here

            elif use_credit_key_if_short:
                logger.info(
                    "Not worth using call-based key. Using credit-based key for ship %s, interval %s"
                    % (imo, interval)
                )

                portcalls = portcall.get_next_portcall(
                    date_from=interval["date_from"],
                    date_to=interval["date_to"],
                    arrival_or_departure=None,
                    imo=imo,
                    use_call_based=False,
                    use_cache=True,
                    filter=lambda x: False,
                )
            else:
                logger.info(
                    "Not worth using call-based key. Skipping for ship %s, interval %s"
                    % (imo, interval)
                )
