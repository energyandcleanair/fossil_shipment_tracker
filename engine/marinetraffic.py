import requests
import json
import datetime as dt

import tqdm

import base
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from base.db import session
from base.logger import logger
from base.env import get_env
from base.models import Ship, PortCall, MTVoyageInfo, MarineTrafficCall, Event, Position
from base.utils import to_datetime, latlon_to_point
from requests.adapters import HTTPAdapter, Retry
import urllib.parse

s = requests.Session()
retries = Retry(total=5,
                backoff_factor=0.1,
                status_forcelist=[500, 502, 503, 504])
s.mount('https://', HTTPAdapter(max_retries=retries))

from engine import ship, port

def load_cache(f):
    try:
        with open(f) as json_file:
            return json.load(json_file)
    except json.decoder.JSONDecodeError:
        return []


class Marinetraffic:

    api_base = 'https://services.marinetraffic.com/api/'
    cache_file_ship = 'cache/marinetraffic/ships.json'
    cache_file_events = 'cache/marinetraffic/events.json'
    cache_ship = load_cache(cache_file_ship)
    cache_events = load_cache(cache_file_events)

    @classmethod
    def call(cls, method, params, api_key, credits_per_record, save_empty_record=True):
        params_string = urllib.parse.urlencode(params)
        api_result = s.get(Marinetraffic.api_base + method + api_key + '?' + params_string)

        call_log = {
            'method': method,
            'date_utc': dt.datetime.utcnow(),
            'params': params,
            'key': api_key
        }

        if api_result.status_code != 200:
            call_log['records'] = 0
            call_log['credits'] = 0
            call_log['status'] = str(api_result.status_code) + " - " + str(api_result.content)
            result = (None, api_result)
        else:
            result = (api_result.json(), api_result)
            call_log['records'] = len(api_result.json())
            call_log['credits'] = len(api_result.json()) * credits_per_record
            call_log['status'] = str(api_result.status_code)

        if (call_log['records'] > 0) or save_empty_record:
            session.add(MarineTrafficCall(**call_log))
            session.commit()

        return result

    @classmethod
    def get_cached_object(cls, object_cache, filter):
        return [x for x in object_cache if filter(x)]

    @classmethod
    def do_cache_object(cls, response_data, object_cache, object_cache_file):
        """
        Add response data to cache
        :param response_data:
        :return:

        Parameters
        ----------
        object_cache :
        object_cache_file :
        """
        object_cache.append(response_data)
        with open(object_cache_file, 'w') as outfile:
            json.dump(object_cache, outfile)

    @classmethod
    def get_ship(cls, imo=None, mmsi=None, mt_id=None, use_cache=True):

        api_key = get_env("KEY_MARINETRAFFIC_VD02")

        # First look in cache to save query credits
        if use_cache:

            ship_filter = lambda x: (imo is not None and str(x["IMO"]) == str(imo)) \
                               or (mmsi is not None and str(x["MMSI"]) == str(mmsi)) \
                               or (mt_id is not None and str(x.get("SHIPID", "---")) == str(mt_id))

            response_data = cls.get_cached_object(cls.cache_ship, ship_filter)

            if len(response_data) == 1:
                response_data = response_data[0]
            elif len(response_data) > 1:
                logger.warning("Found more than 1 ship in cache with matching criteria...")

                return None

        else:
            response_data = None

        # Otherwise query datalastic (and cache it as well)
        if not response_data:
            params = {
                'v': 5,
                'protocol': 'jsono',
            }

            if imo is not None:
                params["imo"] = imo
            elif mmsi is not None:
                params["mmsi"] = mmsi
            elif mt_id is not None:
                params['shipid'] = mt_id

            (response_data, response) = cls.call(method='vesselmasterdata/',
                                                          api_key=api_key,
                                                          params=params,
                                                          credits_per_record=3)

            if response_data and 'DATA' in response_data:
                response_data = response_data.get('DATA')

            if not response_data:
                logger.warning("Marinetraffic: Failed to query vessel %s: %s" % (imo, response))
                return None

            if len(response_data) > 1:
                # We assume only ships higher than base.DWT_MIN have been queried
                try:
                    response_data = [x for x in response_data if float(x["SUMMER_DWT"]) > base.DWT_MIN]
                    if len(response_data) > 1:
                        logger.warning(
                            "Two ships available with this mmsi: %s" % (response_data,))
                        return None

                except Exception as e:
                    response_data = [{"MMSI": mmsi,
                                     "IMO": imo,
                                     "SHIPID": mt_id,
                                     "NAME": None
                                     }]

            response_data = response_data[0]
            if mt_id:
                response_data['SHIPID'] = mt_id

            if use_cache:
                cls.do_cache_object(response_data, cls.cache_ship, cls.cache_file_ship)

        data = {
            "mmsi": response_data["MMSI"],
            "imo": response_data["IMO"],
            "name": response_data.get("NAME"),
            "type": response_data.get("VESSEL_TYPE"),
            # "subtype": None,
            "dwt": response_data.get("SUMMER_DWT"),
            "country_iso2": response_data.get("FLAG"),
            # "country_name": None,
            # "home_port": None,
            "liquid_gas": response_data.get("LIQUID_GAS"),
            "liquid_oil": response_data.get("LIQUID_OIL"),
            # "owner": response_data.get("OWNER"),
            # "manager": response_data.get("MANAGER"),
            # "insurer": response_data.get("INSURER"),
            "others": {"marinetraffic": response_data}
        }

        return Ship(**data)


    @classmethod
    def get_portcalls_between_dates(cls, date_from, date_to,
                                    unlocode=None,
                                    imo=None,
                                    marinetraffic_port_id=None,
                                    arrival_or_departure=None,
                                    use_call_based=False):

        if imo is None and unlocode is None and marinetraffic_port_id is None:
            raise ValueError("Need to specify either imo, unlocode or marinetraffic_port_id")

        date_from = to_datetime(date_from)
        date_to = to_datetime(date_to)

        if use_call_based:
            api_key = get_env("KEY_MARINETRAFFIC_EV01_CALL_BASED")
        else:
            api_key = get_env("KEY_MARINETRAFFIC_EV01")

        params = {
            'v': 4,
            'protocol': 'jsono',
            'msgtype': 'extended',
            'fromdate': date_from.strftime("%Y-%m-%d %H:%M"),
            'todate': date_to.strftime("%Y-%m-%d %H:%M"),
            'exclude_intransit': 1,
            'dwt_min': base.DWT_MIN
        }

        if unlocode:
            params["portid"] = unlocode

        if imo:
            params["imo"] = imo

        if marinetraffic_port_id:
            params["portid"] = marinetraffic_port_id

        if arrival_or_departure:
            params["movetype"] = {"departure": 1, "arrival":0}[arrival_or_departure]

        (response_datas, response) = cls.call(method='portcalls/',
                                              api_key=api_key,
                                              params=params,
                                              credits_per_record=4,
                                              save_empty_record=use_call_based)

        if response_datas is None:
            if not imo or not '_v2' in imo:
                #TODO deal with these v2 and unknown
                logger.warning("Marinetraffic: Failed to query portcall %s: %s" % (unlocode, response))
            return []

        if not response_datas:
            return []

        portcalls = []

        # Filling missing ships
        if imo is None:
            mmsis = [x['MMSI'] for x in response_datas]
            found = ship.fill(mmsis=mmsis)

            mmsi_imo = session.query(Ship.mmsi, Ship.imo).filter(Ship.mmsi.in_(mmsis)).all()
            mmsi_imo_dict = dict(zip([x[0] for x in mmsi_imo],
                                    [x[1] for x in mmsi_imo]))



        for r in response_datas:
            # IMO's missing
            if imo is None:
                # Ship not found, let's add it
                #Very important: don't overwrite imo variable
                r_imo = mmsi_imo_dict.get(r["MMSI"])
                if r_imo is None:
                    r_imo = 'NOTFOUND_' + r['MMSI']
                    unknown_ship = Ship(imo=r_imo, mmsi=r['MMSI'], type=r["TYPE_NAME"],
                                    name=r['SHIPNAME'])
                    session.add(unknown_ship)
                    try:
                        session.commit()
                    except IntegrityError:
                        session.rollback()
                        is_same = bool(Ship.query.filter(Ship.imo==r_imo, Ship.name == r['SHIPNAME']).count())
                        if is_same:
                            continue
                        else:
                            n_imo_ships = Ship.query.filter(Ship.imo.op('~')(r_imo)).count()
                            if n_imo_ships > 0:
                                r_imo = "%s_v%d" % (r_imo, n_imo_ships + 1)
                                unknown_ship = Ship(imo=r_imo, mmsi=r['MMSI'], type=r["TYPE_NAME"],
                                                    name=r['SHIPNAME'])
                                session.add(unknown_ship)
                                session.commit()

                r["IMO"] = r_imo

            if imo is not None:
                r["IMO"] = imo

            portcalls.append(cls.parse_portcall(r))

        return portcalls

    @classmethod
    def parse_portcall(cls, response_data):
        data = {
            "ship_mmsi": response_data["MMSI"],
            "ship_imo": response_data["IMO"],
            "date_utc": response_data["TIMESTAMP_UTC"],
            "date_lt": response_data["TIMESTAMP_LT"],
            "port_id": port.get_id(name=response_data["PORT_NAME"], unlocode=response_data["UNLOCODE"], marinetraffic_id=response_data["PORT_ID"]),
            "load_status": response_data.get("LOAD_STATUS"),
            "move_type": response_data["MOVE_TYPE"],
            "port_operation": response_data.get("PORT_OPERATION"),
            "others": {"marinetraffic": response_data}
        }
        return PortCall(**data)


    @classmethod
    def get_next_portcall(cls, date_from, arrival_or_departure=None, date_to=None, imo=None, unlocode=None,
                          filter=None, go_backward=False, use_call_based=False):
        """
        The function returns collects arrival portcalls until it finds one matching
        filter (or until it finds one if filter is None).

        It returns the first matching one as well as all collected ones in the process,
        so that we can cache them in the db, and not query again and again useless records.
        :param imo:
        :param date_from:
        :param filter:
        :param use_call_based: weither to use the normal (credit-based) key or the call-based key
        :return: two things: (first_matching_portcall, list_of_portcalls_collected)
        """
        delta_time = dt.timedelta(hours=12) if not use_call_based else dt.timedelta(days=190)
        date_from = to_datetime(date_from)
        date_to = to_datetime(date_to)
        if date_to is None:
            date_to = to_datetime("2022-01-01") if go_backward else dt.datetime.utcnow()

        direction = -1 if go_backward else 1

        if imo is None and unlocode is None:
            raise ValueError("Need to specify either imo or unlocode")

        portcalls = []
        filtered_portcalls = []

        # Splitting in intervals
        intervals = []
        start = min(date_from, date_to)
        end = max(date_from, date_to)
        while start < end:
            intervals.append((start, min(start + delta_time, end)))
            start += delta_time

        if go_backward:
            intervals.reverse()

        for interval in intervals:
            date_from_call = interval[0]
            date_to_call = interval[1]

            period_portcalls = cls.get_portcalls_between_dates(imo=imo,
                                                               unlocode=unlocode,
                                                               arrival_or_departure=arrival_or_departure,
                                                               date_from=date_from_call,
                                                               date_to=date_to_call,
                                                               use_call_based=use_call_based)
            portcalls.extend(period_portcalls)
            if filter:
                filtered_portcalls.extend([x for x in period_portcalls if filter(x)])
            else:
                filtered_portcalls.extend(period_portcalls)

            if filtered_portcalls:
                break

        if not filtered_portcalls:
            # No arrival portcall arrived yet
            filtered_portcall = None
        else:
            # Sort by date in the unlikely case
            # there are several calls within this delta
            filtered_portcalls.sort(key=lambda x: x.date_utc, reverse=go_backward)
            filtered_portcall = filtered_portcalls[0]

        return filtered_portcall, portcalls

    @classmethod
    def get_voyage_info(cls, imo, date_from):

        # First look in cache to save query credits
        cached_info = MTVoyageInfo.query.filter(
            sa.and_(
                MTVoyageInfo.ship_imo == imo,
                MTVoyageInfo.queried_date_utc >= date_from)
            ).first()

        if cached_info:
            logger.info("Found a cached VoyageInfo: %s from %s: %s" % (imo, date_from, cached_info.destination_name))
            return cached_info

        # Otherwise query marinetraffic (and cache it as well)
        api_key = get_env("KEY_MARINETRAFFIC_VI01")

        params = {
            'protocol': 'jsono',
            'msgtype': 'simple',
            'imo': imo,
        }
        (response_datas, response) = cls.call(method='voyageforecast/',
                                              params=params,
                                              api_key=api_key,
                                              credits_per_record=4)

        if response_datas is None:
            logger.warning("Marinetraffic: Failed to query voyageforecast %s: %s" % (imo, response))
            return []

        if response_datas == []:
            logger.info("Didn't find any voyage infos for imo %s" % (imo,))
            return []

        voyageinfos = []
        for r in response_datas:
            r["IMO"] = imo
            voyageinfos.append(cls.parse_voyageinfo(r))

        # Cache them
        logger.info("Found %d voyage infos for imo %s (%d credits used)" % (len(voyageinfos), imo,
                                                                            len(voyageinfos) * 4))

        for v in voyageinfos:
            session.add(v)
        session.commit()

        return voyageinfos

    @classmethod
    def parse_voyageinfo(cls, response_data):
        data = {
            "ship_mmsi": response_data["MMSI"],
            "ship_imo": response_data["IMO"],
            "queried_date_utc": dt.datetime.utcnow(),
            "destination_name": response_data["DESTINATION"],
            "next_port_name": response_data["NEXT_PORT_NAME"],
            "next_port_unlocode": response_data["NEXT_PORT_UNLOCODE"],
            "others": {"marinetraffic": response_data}
        }
        return MTVoyageInfo(**data)

    @classmethod
    def get_positions(cls, imo, date_from, date_to, period=None):
        """
        This function returns positions for imo in a date range

        Parameters
        ----------
        period : hourly or daily - this allows MT to only return 1 position per hour/day - useful for limiting credits
        imo : ship imo
        date_from : date from
        date_to : date to

        Returns
        -------

        Returns all positions (Position objects) for ship imo which satisfy the conditions

        """

        api_key = get_env("KEY_MARINETRAFFIC_PS01")

        date_from = to_datetime(date_from)
        date_to = to_datetime(date_to)

        params = {
            'protocol': 'jsono',
            'imo': imo,
            'fromdate': date_from.strftime("%Y-%m-%d %H:%M"),
            'todate': date_to.strftime("%Y-%m-%d %H:%M")
        }

        if period:
            params['period'] = period


        (response_datas, response) = cls.call(method='exportvesseltrack/',
                                              api_key=api_key,
                                              params=params,
                                              credits_per_record=1,
                                              save_empty_record=True)

        if response_datas is None:
            logger.warning("Marinetraffic: Failed to query position for ship %s: %s" % (imo, response))
            return []

        if response_datas == []:
            logger.info("Didn't find any vessel positions for imo %s" % (imo,))
            return []

        positions = cls.parse_position_response_data(response_data=response_datas, imo=imo)

        for p in positions:
            try:
                session.add(p)
                session.commit()
            except IntegrityError:
                session.rollback()

        return positions

    @classmethod
    def parse_position_response_data(cls, response_data, imo):

        positions = [Position(**{
            "geometry": latlon_to_point(lat=float(x["LAT"]), lon=float(x["LON"])),
            "ship_imo": imo,
            "navigation_status": x["STATUS"],
            "speed": float(x["SPEED"]),
            "date_utc": dt.datetime.strptime(x["TIMESTAMP"], "%Y-%m-%dT%H:%M:%S"),
            "destination_name": None
        }) for x in response_data if abs(float(x["LAT"])) > 1e-4]

        return positions

    @classmethod
    def get_closest_position(cls, imo, date, window_hours = 24, interval_mins = 30):
        """
        Returns the closest position to a given date within the time window, using certain intervals to minimise number
        of queried/returned positions

        Parameters
        ----------
        imo : ship imo
        date : date to find the closest position to
        window_hours : time window around date to look for positions
        interval_mins : what size interval to look between

        Returns
        -------
        Returns Position object

        """
        date = to_datetime(date)

        intervals = [[date-dt.timedelta(minutes=d*interval_mins/2.),
                      date+dt.timedelta(minutes=d*interval_mins/2.)]
                        for d in range(1, int((window_hours*60.0)/interval_mins))]

        logger.info("Querying intervals for ship imo: {}.".format(imo))
        for interval in tqdm.tqdm(intervals):
            date_from = interval[0]
            date_to = interval[1]

            interval_positions = cls.get_positions(imo=imo, date_from=date_from, date_to=date_to)

            if interval_positions is not None and interval_positions:
                # Sort in case we have more than one
                interval_positions.sort(key=lambda x: x.date_utc)
                return interval_positions[0]

        # Otherwise return None
        return None
    @classmethod
    def get_ship_events_between_dates(cls, imo,
                                      date_from,
                                      date_to,
                                      use_cache=True,
                                      cache_objects=True,
                                      event_type='21'):
        """

        Parameters
        ----------
        date_from :
        date_to :
        imo :
        event_filter : Filter for specific type of events; by default '21,22' which is STS_START and STS_END
        please see here for documentation or look into mtevent_type table: https://help.marinetraffic.com/hc/en-us/articles/218604297-What-is-the-significance-of-the-MarineTraffic-Events-?flash_digest=0311450e3f0b388436ef20b3840a296cb29ffb10

        Returns
        -------

        """

        api_key = get_env("KEY_MARINETRAFFIC_EV02")

        date_from = to_datetime(date_from)
        date_to = to_datetime(date_to)

        if use_cache:
            event_filter = lambda x: (imo is not None and str(x["IMO"]) == str(imo)) \
                                    and (dt.datetime.strptime(str(x["TIMESTAMP"]), "%Y-%m-%dT%H:%M:%S") >= date_from) \
                                    and (dt.datetime.strptime(str(x["TIMESTAMP"]), "%Y-%m-%dT%H:%M:%S") <= date_to) \
                                    and x["EVENT_ID"] in event_type.split(",")

            response_datas = cls.get_cached_object(cls.cache_events, event_filter)

            print("Found {} cached events.".format(len(response_datas)))
        else:
            response_datas = None

        if not response_datas:

            params = {
                'protocol': 'jsono',
                'fromdate': date_from.strftime("%Y-%m-%d %H:%M"),
                'todate': date_to.strftime("%Y-%m-%d %H:%M"),
            }

            if imo is not None:
                params["imo"] = imo

            if event_type:
                params['event_type'] = event_type

            (response_datas, response) = cls.call(method='vesselevents/',
                                                  api_key=api_key,
                                                  params=params,
                                                  credits_per_record=2,
                                                  save_empty_record=True)

            if response_datas is None:
                logger.warning("Marinetraffic: Failed to query events %s: %s" % (imo, response))
                return []

            for r in response_datas:

                # if we are not using cache and queried MT, let's add ship imo to response and then cache object
                r["IMO"] = imo
                if cache_objects:
                    cls.do_cache_object(r, cls.cache_events, cls.cache_file_events)

        events = []
        for r in response_datas:

            events.append(cls.parse_event(r))

        return events

    @classmethod
    def parse_event(cls, response_data):
        data = {
            "ship_name": response_data["SHIPNAME"],
            "ship_imo": response_data["IMO"],
            "ship_closest_position": None,
            "interacting_ship_name": None,
            "interacting_ship_imo": None,
            "interacting_ship_closest_position": None,
            "interacting_ship_details": None,
            "date_utc": response_data["TIMESTAMP"],
            "type_id": response_data["EVENT_ID"],
            "content": response_data["EVENT_CONTENT"],
            "source": "marinetraffic"
        }
        return Event(**data)

    # @classmethod
    # #TODO
    # def get_port_anchorage(cls, unlocode=None, name=None):