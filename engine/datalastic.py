import requests
import json
import datetime as dt

from base.utils import latlon_to_point
from base.utils import to_datetime
from base.env import get_env
from base.logger import logger
from base.models import Ship, Position, Port


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
    def get_ship(cls, imo=None, mmsi=None, query_if_not_in_cache=True, use_cache=True):

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
    def get_positions(cls, imo, date_from, date_to):

        if not cls.api_key:
            cls.api_key = get_env("KEY_DATALASTIC")

        params = {
            'api-key': cls.api_key,
            'imo': imo,
            'from': to_datetime(date_from).strftime("%Y-%m-%d")
        }
        if date_to is not None:
            params["to"] = to_datetime(date_to).strftime("%Y-%m-%d")

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
        api_result = requests.get(Datalastic.api_base + method, params, verify=False)
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
        }) for x in response_data["positions"] if abs(x["lat"] > 1e-4)]

        return positions

    @classmethod
    def get_port_infos(cls, name=None, marinetraffic_id=None, fuzzy=False):
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

        return ports