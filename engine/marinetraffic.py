import requests
import json
import datetime as dt
import base

from base.db import session
from base.logger import logger
from base.env import get_env
from base.models import Ship, PortCall
from base.utils import to_datetime

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
    # cache_file_portcall = 'cache/marinetraffic/portcall.json'

    cache_ship = load_cache(cache_file_ship)
    # cache_portcall = load_cache(cache_file_portcall)


    @classmethod
    def get_ship_cached(cls, imo):
        try:
            return next(x for x in cls.cache_ship if str(x["IMO"]) == str(imo))
        except StopIteration:
            return None


    @classmethod
    def do_cache_ship(cls, response_data):
        """
        Add response data to cache
        :param response_data:
        :return:
        """
        cls.cache_ship.append(response_data)
        with open(cls.cache_file_ship, 'w') as outfile:
            json.dump(cls.cache_ship, outfile)


    # @classmethod
    # def get_portcall_cached(cls, imo, date_from):
    #     #TODO We are not sure the portcall are sorted with time,
    #     # this system won't work until we do
    #     try:
    #         return next(x for x in cls.cache_portcall \
    #                     if str(x["imo"]) == str(imo) \
    #                     and x["date_utc"] > date_from)
    #     except StopIteration:
    #         return None

    # @classmethod
    # def do_cache_portcall(cls, response_data):
    #     """
    #     Add response data to cache
    #     :param response_data:
    #     :return:
    #     """
    #     cls.cache_portcall.append(response_data)
    #     with open(cls.cache_file_portcall, 'w') as outfile:
    #         json.dump(cls.cache_portcall, outfile)


    @classmethod
    def get_ship(cls, imo=None, mmsi=None):

        api_key = get_env("KEY_MARINETRAFFIC_VD02")

        # First look in cache to save query credits
        response_data = cls.get_ship_cached(imo)

        # Otherwise query datalastic (and cache it as well)
        if not response_data:
            params = {
                'v': 1,
                'protocol': 'jsono',
            }

            if imo is not None:
                params["imo"] = imo
            elif mmsi is not None:
                params["mmsi"] = mmsi

            method = 'vesselmasterdata/'
            api_result = requests.get(Marinetraffic.api_base + method + api_key, params)
            if api_result.status_code != 200:
                logger.warning("Marinetraffic: Failed to query vessel %s: %s" % (imo, api_result))
                return None
            response_data = api_result.json()

            if not response_data:
                logger.warning("Marinetraffic: Failed to query vessel %s: %s" % (imo, "Response is empty"))
                response_data = [{"MMSI": mmsi,
                                 "IMO": imo,
                                 "NAME": None
                                 }]

            if len(response_data) > 1:
                # We assume only ships higher than base.DWT_MIN have been queried
                try:
                    response_data = [x for x in response_data if float(x["SUMMER_DWT"]) > base.DWT_MIN]
                    if len(response_data) > 1:
                        logger.warning(
                            "Two ships available with this mmsi: %s" % (response_data,))
                        response_data = [{"MMSI": mmsi,
                                          "IMO": imo,
                                          "NAME": None
                                          }]
                except Exception as e:
                    response_data = [{"MMSI": mmsi,
                                     "IMO": imo,
                                     "NAME": None
                                     }]

            response_data = response_data[0]
            cls.do_cache_ship(response_data)


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
            "others": {"marinetraffic": response_data}
        }

        return Ship(**data)

    @classmethod
    def get_portcalls_between_dates(cls, date_from, date_to,
                                    unlocode=None,
                                    imo=None,
                                    arrival_or_departure=None):

        if imo is None and unlocode is None:
            raise ValueError("Need to specify either imo or unlocode")

        date_from = to_datetime(date_from)
        date_to = to_datetime(date_to)

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

        if arrival_or_departure:
            params["movetype"] = {"departure":1, "arrival":0}[arrival_or_departure]

        method = 'portcalls/'
        api_result = requests.get(Marinetraffic.api_base + method + api_key, params)
        if api_result.status_code != 200:
            logger.warning("Marinetraffic: Failed to query portcall %s: %s %s" % (unlocode, api_result, api_result.content))
            return []
        response_datas = api_result.json()

        if not response_datas:
            return []

        portcalls = []
        for r in response_datas:
            # IMO's missing
            imo = session.query(Ship.imo).filter(Ship.mmsi==r["MMSI"]).first()
            if imo is None:
                # Ship not found, let's add it
                ship.fill(mmsis=[r["MMSI"]])
                imo = session.query(Ship.imo).filter(Ship.mmsi == r["MMSI"]).first()

            if imo is not None:
                r["IMO"] = imo[0]
                portcalls.append(cls.parse_portcall(r))

        return portcalls

    @classmethod
    def parse_portcall(cls, response_data):
        data = {
            "ship_mmsi": response_data["MMSI"],
            "ship_imo": response_data["IMO"],
            "date_utc": response_data["TIMESTAMP_UTC"],
            "date_lt": response_data["TIMESTAMP_LT"],
            "port_id": port.get_id(unlocode=response_data["UNLOCODE"], marinetraffic_id=response_data["PORT_ID"]),
            "load_status": response_data.get("LOAD_STATUS"),
            "move_type": response_data["MOVE_TYPE"],
            "port_operation": response_data.get("PORT_OPERATION"),
            "others": {"marinetraffic": response_data}
        }
        return PortCall(**data)


    @classmethod
    def get_next_portcall(cls, date_from, arrival_or_departure, date_to=None, imo=None, unlocode=None,  filter=None, go_backward=False):
        """
        The function returns collects arrival portcalls until it finds one matching
        filter (or until it finds one if filter is None).

        It returns the first matching one as well as all collected ones in the process,
        so that we can cache them in the db, and not query again and again useless records.
        :param imo:
        :param date_from:
        :param filter:
        :return: two things: (first_matching_portcall, list_of_portcalls_collected)
        """
        delta_time = dt.timedelta(hours=24)
        date_from = to_datetime(date_from)
        date_to = to_datetime(date_to)
        if date_to is None:
            date_to = to_datetime("2022-01-01") if go_backward else dt.datetime.utcnow()

        direction = -1 if go_backward else 1
        ncredits = 0
        credit_per_record = 4

        if imo is None and unlocode is None:
            raise ValueError("Need to specify either imo or unlocode")

        portcalls = []
        filtered_portcalls = []
        while not filtered_portcalls and \
            ((date_from < date_to and not go_backward) or \
             (date_from > date_to and go_backward)):
            date_from_call = min(date_from, date_from + direction * delta_time)
            if go_backward:
                date_to_call = max(date_to, max(date_from, date_from + direction * delta_time))
            else:
                date_to_call = min(date_to, max(date_from, date_from + direction * delta_time))

            period_portcalls = cls.get_portcalls_between_dates(imo=imo,
                                                               unlocode=unlocode,
                                                               arrival_or_departure=arrival_or_departure,
                                                               date_from=date_from_call,
                                                               date_to=date_to_call)
            ncredits += len(period_portcalls) * credit_per_record
            portcalls.extend(period_portcalls)
            if filter:
                filtered_portcalls.extend([x for x in period_portcalls if filter(x)])
            else:
                filtered_portcalls.extend(period_portcalls)

            date_from += (delta_time * direction)
        
        if ncredits > 0:
            print("%d credits used" % (ncredits,))
        if not filtered_portcalls:
            # No arrival portcall arrived yet
            filtered_portcall = None
        else:
            # Sort by date in the unlikely case
            # there are several calls within this delta
            filtered_portcalls.sort(key=lambda x: x.date_utc, reverse=go_backward)
            filtered_portcall = filtered_portcalls[0]


        return filtered_portcall, portcalls


