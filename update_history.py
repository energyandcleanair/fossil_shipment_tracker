import pandas as pd

from engine import port
from engine import portcall
from engine import departure
from engine.marinetraffic import Marinetraffic
from engine import arrival
from engine import shipment
from engine import trajectory
from engine import position
from engine import destination
from engine import berth
from engine import ship
from engine import currency
from engine import rscript
from engine import counter
from engine import entsog
from engine import alert
from engine import company
from engine import mtevents
import integrity
import base
from base.db import session
from base.utils import to_datetime
import datetime as dt

import sqlalchemy as sa
from sqlalchemy import func
from tqdm import tqdm

from base.models import Departure, PortCall, Ship, MarineTrafficCall, Arrival, Port

tqdm.pandas()

def update_history():

    # This call is to update history using the CALL-BASED MARINE TRAFFIC KEY
    # meaning we'll try to maximize number of records captured per call
    # and minimize the number of calls made
    # i.e. the opposite of the RECORD-BASED MARINE TRAFFIC KEY
    #
    # We use it to fill past data

    # update_departures_portcalls(date_from='2020-07-01', date_to='2021-01-01')
    # departure.update(date_from='2020-07-01')
    update_arrival_portcalls(date_from='2020-07-01', date_to='2022-01-01')
    # update_sts_events()

    # Get gaps for each ship

def update_arrival_portcalls(date_from, date_to):
    query_departure = session.query(
                          Departure.id,
                          Departure.ship_imo.label('imo'),
                          Ship.commodity,
                          Ship.dwt,
                          Departure.date_utc.label('departure_date')) \
        .join(Ship, Ship.imo == Departure.ship_imo) \
        .outerjoin(Arrival, Arrival.departure_id == Departure.id) \
        .filter(Ship.commodity.in_([base.OIL_OR_CHEMICAL, base.COAL]))
        # .filter(Arrival.id == sa.null())

    queried = session.query(
        MarineTrafficCall.params['imo'].label('imo'),
        MarineTrafficCall.params['fromdate'].label('date_from'),
        MarineTrafficCall.params['todate'].label('date_to'),
        MarineTrafficCall.records
        ) \
    .filter(MarineTrafficCall.method == 'portcalls/',
            MarineTrafficCall.params['movetype'] == sa.null(),
            MarineTrafficCall.status == base.HTTP_OK
    )


    # Get departures of interest
    departure_df = pd.read_sql(query_departure.statement, session.bind)
    departure_df['next_departure_date'] = departure_df \
        .sort_values(by=['departure_date']) \
        .groupby('imo')['departure_date'] \
        .shift(-1) \
        .fillna(dt.datetime.utcnow())

    def departure_to_arrival(id, next_departure_date, **kwargs):
        arrival = portcall.find_arrival(Departure.query.filter(Departure.id == id).first(),
                                        date_to=next_departure_date,
                                        cache_only=True)
        return arrival.date_utc if arrival else None

    departure_df['next_arrival'] = departure_df.progress_apply(lambda x: departure_to_arrival(x.id, x.next_departure_date),
                                                      axis=1)
    departures = departure_df[pd.isna(departure_df.next_arrival)]
    departure_dates = departures.groupby('imo').agg(date_from=('departure_date', min),
                                               date_to=('departure_date', max))

    departure_dates = departure_dates.reset_index()
    departure_dates['dates'] = departure_dates.apply(
        lambda row: pd.date_range(row.date_from.floor('H'), row.date_to.floor('H'), freq='H'),
        axis=1)
    departure_dates = departure_dates[['imo','dates']].explode('dates').drop_duplicates().sort_values(['imo', 'dates'])

    # Get information on calls already made to MT
    queried_df = pd.read_sql(queried.statement, session.bind)
    queried_df = queried_df[queried_df.imo.isin(departure_dates.imo)]

    if len(queried_df):
        queried_df['date_from'] = pd.to_datetime(queried_df.date_from)
        queried_df['date_to'] = pd.to_datetime(queried_df.date_to)
        queried_df['dates'] = queried_df.progress_apply(
            lambda row: pd.date_range(row.date_from.floor('H'), row.date_to.floor('H'), freq='H'),
            axis=1)

        queried_dates = queried_df[['imo', 'dates']].explode('dates').drop_duplicates().sort_values(['imo', 'dates'])

        # See what dates are missing / need to be queried
        missing_dates = pd.concat([departure_dates, queried_dates, queried_dates]).drop_duplicates(keep=False)
    else:
        missing_dates = departure_dates

    missing_ship_dates = missing_dates[missing_dates.dates <= pd.to_datetime(date_to)] \
        .groupby('imo').agg(date_from=('dates', min),
                            date_to=('dates', max)) \
        .reset_index()

    missing_ship_dates['interval'] = missing_ship_dates.date_to - missing_ship_dates.date_from
    missing_ship_dates = missing_ship_dates.sort_values('interval', ascending=False)
    missing_ship_dates = missing_ship_dates[~missing_ship_dates.imo.str.contains('_')]
    missing_ship_dates = missing_ship_dates[missing_ship_dates.interval > dt.timedelta(hours=1)]

    for index, row in tqdm(missing_ship_dates.iterrows(), total=missing_ship_dates.shape[0]):

        imo = row.imo

        intervals = []
        delta_time = dt.timedelta(days=189) #MT maximum interval is 190 days
        start = row.date_from
        end = row.date_to
        while start < end:
            intervals.append([start, min(start + delta_time, end)])
            start += delta_time

        for interval in intervals:
            # VERY IMPORTANT TO USE THE RIGHT KEY!!
            use_call_based = (interval[1] - interval[0]) > dt.timedelta(days=20)
            if use_call_based:
                # Might as well query more, same cost
                interval[1] = max(interval[1], interval[0] + delta_time)

            portcalls = portcall.get_next_portcall(date_from=interval[0],
                                                   date_to=interval[1],
                                                   arrival_or_departure=None,
                                                   imo=imo,
                                                   use_call_based=use_call_based,
                                                   use_cache=False,
                                                   filter=lambda x: False
                                                   )
    return


def update_departures_portcalls(date_from, date_to):

    # Brute force: 3 calls per port
    date_from = to_datetime(date_from)
    date_to = to_datetime(date_to)
    ports = session.query(Port).filter(Port.check_departure).all()

    intervals = []
    delta_time = dt.timedelta(days=189)
    start = date_from
    end = date_to
    while start < end:
        intervals.append((start, min(start + delta_time, end)))
        start += delta_time

    for port in tqdm(ports):
        print("Port %s" % (port.marinetraffic_id,))
        for interval in intervals:

           # Check if this call has already been made
           found = MarineTrafficCall.query.filter(
               MarineTrafficCall.params['portid'].astext == (port.unlocode or port.marinetraffic_id),
               MarineTrafficCall.params['fromdate'].astext == interval[0].strftime('%Y-%m-%d %H:%M'),
               MarineTrafficCall.params['todate'].astext == interval[1].strftime('%Y-%m-%d %H:%M'),
               MarineTrafficCall.status == base.HTTP_OK
           ).count()
           if not found:
               portcalls = Marinetraffic.get_portcalls_between_dates(arrival_or_departure="departure",
                                                                  unlocode=port.unlocode,
                                                                  marinetraffic_port_id=port.marinetraffic_id,
                                                                  date_from=interval[0],
                                                                  date_to=interval[1],
                                                                  use_call_based=True)
               portcall.upload_portcalls(portcalls)


if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update_history()
