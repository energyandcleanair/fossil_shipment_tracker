import requests
import json
import datetime as dt

from base.logger import logger
from base.env import get_env
from models import Ship, PortCall


class Marinetraffic:

    api_base = 'https://services.marinetraffic.com/api/'
    api_key = None
    cache_file_ship = 'cache/marinetraffic/ships.json'
    cache_file_port = 'cache/marinetraffic/ships.json'

    try:
        with open(cache_file_ship) as json_file:
            cache_ships = json.load(json_file)
    except json.decoder.JSONDecodeError as e:
        cache_ships = []

    @classmethod
    def get_ship_cached(cls, imo):
        try:
            return next(x for x in cls.cache_ships if str(x["IMO"]) == str(imo))
        except StopIteration:
            return None


    @classmethod
    def cache_ship(cls, response_data):
        """
        Add response data to cache
        :param response_data:
        :return:
        """
        cls.cache_ships.append(response_data)
        with open(cls.cache_file_ship, 'w') as outfile:
            json.dump(cls.cache_ships, outfile)


    @classmethod
    def get_ship(cls, imo):

        api_key = get_env("KEY_MARINETRAFFIC_VD02")

        # First look in cache to save query credits
        response_data = cls.get_ship_cached(imo)

        # Otherwise query datalastic (and cache it as well)
        if not response_data:
            params = {
                # 'api-key': cls.api_key,
                'v': 1,
                'protocol': 'jsono',
                # 'mmsi': mmsi
                'imo': imo
            }
            method = 'vesselmasterdata/'
            api_result = requests.get(Marinetraffic.api_base + method + cls.api_key, params)
            if api_result.status_code != 200:
                logger.warning("Marinetraffic: Failed to query vessel %s: %s" % (imo, api_result))
                return None
            response_data = api_result.json()

            if not response_data:
                logger.warning("Marinetraffic: Failed to query vessel %s: %s" % (imo, "Response is empty"))
                return None

            if len(response_data) > 1:
                raise ValueError("This function only querys one ship at a time")

            response_data = response_data[0]
            cls.cache_ship(response_data)


        data = {
            "mmsi": response_data["MMSI"],
            "name": response_data["NAME"],
            "imo": response_data["IMO"],
            "type": response_data["VESSEL_TYPE"],
            # "subtype": None,
            "dwt": response_data["SUMMER_DWT"],
            "country_iso2": response_data["FLAG"],
            # "country_name": None,
            # "home_port": None,
            "liquid_gas": response_data.get("LIQUID_GAS"),
            "liquid_oil": response_data.get("LIQUID_OIL"),
            "others": {"marinetraffic": response_data}
        }

        return Ship(**data)

    @classmethod
    def get_arrival_portcalls_between_dates(cls, imo, date_from, date_to):
        api_key = get_env("KEY_MARINETRAFFIC_EV01")

        params = {
            'v': 1,
            'protocol': 'jsono',
            'imo': imo,
            'msgtype': 'extended',
            'movetype': 0, # Receive arrivals only
            'fromdate': date_from.strftime("%Y-%m-%d %H:%M"),
            'todate': date_to.strftime("%Y-%m-%d %H:%M"),
            'exclude_intransit': 1
        }

        method = 'portcalls/'
        api_result = requests.get(Marinetraffic.api_base + method + api_key, params)
        if api_result.status_code != 200:
            logger.warning("Marinetraffic: Failed to query vessel %s: %s" % (imo, api_result))
            return None
        response_data = api_result.json()

        if not response_data:
            return []

        portcalls = []
        for r in response_data:
            data = {
                "ship_mmsi": r["MMSI"],
                "ship_imo": imo,
                "date_utc": r["TIMESTAMP_UTC"],
                "port_unlocode": r["UNLOCODE"],
                "load_status": r.get("LOAD_STATUS"),
                "move_type": r["MOVE_TYPE"],
                "port_operation": r.get("PORT_OPERATION"),
                "others": {"marinetraffic": r}
            }
            portcalls.append(PortCall(**data))


        return portcalls




    @classmethod
    def get_first_arrival_portcall(cls, imo, date_from, filter=None):
        delta_time = dt.timedelta(hours=12)
        ncredits = 0
        credit_per_record = 4

        portcalls = []
        while not portcalls and date_from < dt.datetime.utcnow():
            portcalls = cls.get_arrival_portcalls_between_dates(imo=imo, date_from=date_from, date_to=date_from + delta_time)
            ncredits += len(portcalls * credit_per_record)
            if filter:
                portcalls = [x for x in portcalls if filter(x)]
            date_from += delta_time

        print("%d credits used" % (ncredits,))
        if not portcalls:
            # No arrival portcall arrived yet
            return None

        # Sort by date in the unlikely case
        # there are several calls within this delta
        portcalls.sort(key=lambda x: x.date_utc)
        portcall = portcalls[0]
        return portcall
