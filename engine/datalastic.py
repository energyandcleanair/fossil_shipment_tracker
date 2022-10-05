import requests
import json
import datetime as dt

import base
from base.utils import latlon_to_point
from base.utils import to_datetime
from base.env import get_env
from base.logger import logger
from base.models import Ship, Position, Port

from difflib import SequenceMatcher
import numpy as np

def load_cache(f):
    try:
        with open(f) as json_file:
            return json.load(json_file)
    except json.decoder.JSONDecodeError:
        return []


class Datalastic:

    api_base = 'https://api.datalastic.com/api/v0/'
    api_key = None
    cache_ships_file = 'cache/datalastic/ships.json'
    cache_ships = load_cache(cache_ships_file)


    @classmethod
    def get_ship_cached(cls, imo=None, mmsi=None):
        try:
            filter = lambda x : (imo is not None and str(x["imo"]) == str(imo)) or (mmsi is not None and str(x["mmsi"]) == str(mmsi))
            return next(x for x in cls.cache_ships if filter(x))
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
        with open(cls.cache_ships_file, 'w') as outfile:
            json.dump(cls.cache_ships, outfile)

    @classmethod
    def find_ship(cls, name, dwt_min = base.DWT_MIN, fuzzy=True, return_closest=1):
        """
        Find ship based on name from datalastic API

        TODO:
            - could add a function to query by name in cache using fuzzy lookup, but this gets tricky as we could find
            a vessel with the same name but could be wrong one - could check location too?
            - refactor fuzzy lookup to function by itself as it is used in multiple places now

        Parameters
        ----------
        name :
        fuzzy :
        return_closest :
        query_if_not_in_cache :
        use_cache :

        Returns
        -------
        Datalastic response and ship object

        """
        if not cls.api_key:
            cls.api_key = get_env("KEY_DATALASTIC")

        params = {
            'api-key': cls.api_key,
            'name': name,
        }

        # TODO:
        # datalastic seems to have a problem; if we add fuzzy parameter (whether 0/1) it always returns fuzzy so
        # solution for now is to only add when used
        if fuzzy:
            params['fuzzy'] = 1
        if dwt_min:
            params['deadweight_min'] = dwt_min

        method = 'vessel_find'
        api_result = requests.get(Datalastic.api_base + method, params, verify=False)
        if api_result.status_code != 200:
            logger.warning("Datalastic: Failed to query vessel %s: %s" % (name, api_result))
            return None

        response_data = api_result.json()["data"]

        if len(response_data) == 0:
            logger.debug("No vessel found matching name %s" % (name,))
            return None

        if len(response_data) == 1:
            logger.debug("Only 1 vessel found matching name %s (no need to compare strings)" % (name,))
            return [cls.parse_ship_data(response_data[0])]

        else:
            if not return_closest:
                # return first match
                return [cls.parse_ship_data(response_data[0])]

            ratios = np.array([SequenceMatcher(None, s["name"], name).ratio() for s in response_data])
            if max(ratios) > 0.90:
                print("Best match: %s == %s (%f)" % (name, response_data[ratios.argmax()]["name"], ratios.max()))

                sorted_response = [s for _, s in sorted(zip(ratios, response_data), key=lambda pair: pair[0], reverse=True)]

                if sorted_response:
                    return [cls.parse_ship_data(s) for s in sorted_response[0:return_closest]]
            else:
                print("No matches close enough")
                return None

    @classmethod
    def get_ship(cls, imo=None, mmsi=None, query_if_not_in_cache=True, use_cache=True):
        """

        Parameters
        ----------
        imo : ship imo
        mmsi : ship mmsi
        query_if_not_in_cache : use datalastic to query if ship not in cache
        use_cache : whether to check cache

        Returns
        -------
        Ship object

        """
        if not cls.api_key:
            cls.api_key = get_env("KEY_DATALASTIC")

        # First look in cache to save query credits
        if use_cache:
            response_data = cls.get_ship_cached(imo=imo, mmsi=mmsi)
        else:
            response_data = None

        # Otherwise query datalastic (and cache it as well)
        if not response_data:
            if not query_if_not_in_cache:
                return None

            params = {
                'api-key': cls.api_key
            }

            if imo is not None:
                params["imo"] = imo
            elif mmsi is not None:
                params["mmsi"] = mmsi

            method = 'vessel_info'
            api_result = requests.get(Datalastic.api_base + method, params)
            if api_result.status_code != 200:
                logger.warning("Datalastic: Failed to query vessel %s: %s"%(imo, api_result))
                return None
            response_data = api_result.json()["data"]

            if response_data["imo"] == '':
                return None

            if use_cache:
                cls.cache_ship(response_data)

        return cls.parse_ship_data(response_data)

    @classmethod
    def parse_ship_data(cls, response_data):
        data = {
            "mmsi": response_data["mmsi"],
            "name": response_data["name"],
            "imo": response_data["imo"],
            "type": response_data["type"],
            "subtype": response_data["type_specific"],
            "dwt": response_data["deadweight"],
            "country_iso2": response_data["country_iso"],
            "country_name": response_data["country_name"],
            "home_port": response_data["home_port"],
            "liquid_gas": response_data["liquid_gas"],
            "others": {"datalastic": response_data}
        }
        return Ship(**data)

    @classmethod
    def get_position(cls, imo, date, window=72):
        """
        Returns the position of the boat at the closest referenced time in datalastic

        Parameters
        ----------
        imo :
        date :
        window : this is the time window within which to look to find closest position in time

        Returns
        -------

        """
        date = to_datetime(date)

        date_from = (date - dt.timedelta(hours=window)).strftime("%Y-%m-%d")
        date_to = (date + dt.timedelta(hours=window)).strftime("%Y-%m-%d")

        print(date_from, date_to)
        positions = cls.get_positions(imo, date_from=date_from, date_to=date_to)

        if not positions:
            logger.warning("Datalastic: no positions found for ship (imo: {}) between dates: {}, {}.".format(imo, date_from, date_to))
            return None

        return min(positions, key=lambda p: abs(p.date_utc - date))

    @classmethod
    def get_positions(cls, imo, date_from, date_to):
        """
        Returns positions of the vessel by imo between the two dates

        Parameters
        ----------
        imo :
        date_from :
        date_to :

        Returns
        -------

        """

        if not cls.api_key:
            cls.api_key = get_env("KEY_DATALASTIC")

        date_from = to_datetime(date_from)
        date_to = to_datetime(date_to)

        params = {
            'api-key': cls.api_key,
            'imo': imo,
            'from': date_from.strftime("%Y-%m-%d")
        }
        if date_to is not None:
            params["to"] = date_to.strftime("%Y-%m-%d")

        # Datalastic doesn't accept more than one month
        if date_to - date_from >= dt.timedelta(days=31):
            positions = []
            while date_from < date_to:
                date_to_chunk = min(date_to, date_from + dt.timedelta(days=10))
                new_positions = cls.get_positions(imo=imo, date_from=date_from, date_to=date_to_chunk)
                if new_positions is not None:
                    positions.extend(new_positions)
                date_from = date_to_chunk + dt.timedelta(minutes=1)
            return positions

        method = 'vessel_history'
        api_result = requests.get(Datalastic.api_base + method, params)
        if api_result.status_code != 200:
            logger.warning("Datalastic: Failed to query vessel position %s: %s" % (imo, api_result))
            return None
        response_data = api_result.json()["data"]
        positions = Datalastic.parse_position_response_data(imo=imo, response_data=response_data)

        # Datalastic only takes day data as from,
        # we further filter to prevent duplicates in the same day
        positions = [p for p in positions if p.date_utc > date_from]
        if date_to is not None:
            positions = [p for p in positions if p.date_utc < date_to]

        return positions

    @classmethod
    def parse_position_response_data(cls, imo, response_data):

        positions = [Position(**{
            "geometry": latlon_to_point(lat=x["lat"], lon=x["lon"]),
            "ship_imo": imo,
            "navigation_status": x["navigation_status"],
            "speed": x["speed"],
            "date_utc": dt.datetime.strptime(x["last_position_UTC"], "%Y-%m-%dT%H:%M:%SZ"),
            "destination_name": x["destination"]
        }) for x in response_data["positions"] if abs(float(x["lat"])) > 1e-4]

        return positions

    @classmethod
    def search_ports(cls, name=None, marinetraffic_id=None, fuzzy=False):
        """
        Some ports aren't in the UNLOCODE base. MarineTraffic returns port_name however, so
        we can look them up by name here.
        :param name:
        :return:
        """
        if not cls.api_key:
            cls.api_key = get_env("KEY_DATALASTIC")

        params = {
            'api-key': cls.api_key,
            'name': name,
            'fuzzy': int(fuzzy)
        }

        method = 'port_find'
        api_result = requests.get(Datalastic.api_base + method, params)
        if api_result.status_code != 200:
            logger.warning("Datalastic: Failed to query port %s: %s" % (name, api_result))
            return None
        datas = api_result.json()["data"]

        # Some manual fixes for now
        #TODO Clean and put manual matchings in a separate files
        manual_matches = {
            "25565": {
                "port_name": name,
                "country_iso": "RU",
                "lat": 71.00034,
                "lon": 73.7961,
                "uuid": None,
                "unlocode": None
            },
            "25566": {
                "port_name": name,
                "country_iso": "RU",
                "lat": 69.08401,
                "lon": 33.20049,
                "uuid": None,
                "unlocode": None
            },
            "22097": {
                "port_name": name,
                "country_iso": "CN",
                "lat": 31.18777,
                "lon": 122.6466,
                "uuid": None,
                "unlocode": "CNCJK"
            },
            "25806": {
                "port_name": name,
                "country_iso": "GR",
                "lat": None,
                "lon": None,
                "uuid": None,
                "unlocode": None
            },
            "25570": {
                "port_name": name,
                "country_iso": "PH",
                "lat": 14.59285,
                "lon": 120.591,
                "uuid": None,
                "unlocode": None
            },
            "25584": {
                "port_name": name,
                "country_iso": "US",
                "lat": 40.99952,
                "lon": -72.64605,
                "uuid": None,
                "unlocode": None
            },
            "22921": {
                "port_name": name,
                "country_iso": "LB",
                "lat": 34.5125,
                "lon": 35.83174,
                "uuid": None,
                "unlocode": None
            },
            "258": {
                "port_name": name,
                "country_iso": "BR",
                "lat": -12.75194,
                "lon": -38.62173,
                "uuid": None,
                "unlocode": 'BRMDD'
            }
        }
        if marinetraffic_id in manual_matches:
            datas = [manual_matches[str(marinetraffic_id)]]

        if len(datas) == 0:
            logger.debug("No port found matching name %s" % (name,))
            return None

        if len(datas) > 1:
            # Manual fix: we use marinetraffic PORT_ID to disentangle potentially confusing ports
            fixes = {
                "20643": {"country_iso": "ES"},
                "22836": {'country_iso': 'BS'},
                '23300': {'country_iso': 'US'},
                "25566": {'country_iso': 'RU'},
                '156': {'country_iso': 'FR'}
            }
            if marinetraffic_id and str(marinetraffic_id) in fixes:
                datas = [x for x in datas if x[list(fixes[str(marinetraffic_id)].keys())[0]] == list(fixes[str(marinetraffic_id)].values())[0]]
            else:
                logger.debug("More than one port found matching name %s" % (name,))

        ports = []
        for data in datas:
            port = Port(**{
                "geometry": latlon_to_point(lat=data["lat"], lon=data["lon"]),
                "iso2": data["country_iso"],
                "unlocode": data["unlocode"] if data["unlocode"] != "" else None,
                "name": data["port_name"],
                "datalastic_id": data["uuid"]})

            if marinetraffic_id:
                port.marinetraffic_id = marinetraffic_id

            ports.append(port)

        # Datalastic fuzzy argument doesn't seem to work (always True)
        # We filter here
        if not fuzzy:
            ports = [x for x in ports if x.name == name]

        return ports