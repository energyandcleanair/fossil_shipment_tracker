import requests
import json

from base.env import get_env
from base.logger import logger
from models import Ship


def load_cache(f):
    try:
        with open(f) as json_file:
            return json.load(json_file)
    except json.decoder.JSONDecodeError:
        return []


class Datalastic:

    api_base = 'https://api.datalastic.com/api/v0/'
    api_key = None
    cache_file_ship = load_cache('cache/datalastic/ships.json')


    @classmethod
    def get_ship_cached(cls, imo):
        try:
            return next(x for x in cls.cache_ships if str(x["imo"]) == str(imo))
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
    def get_ship(cls, imo, query_if_not_in_cache=True):

        if not cls.api_key:
            cls.api_key = get_env("KEY_DATALASTIC")

        # First look in cache to save query credits
        response_data = cls.get_ship_cached(imo)

        # Otherwise query datalastic (and cache it as well)
        if not response_data:
            if not query_if_not_in_cache:
                return None

            params = {
                'api-key': cls.api_key,
                'imo': imo
            }
            method = 'vessel_info'
            api_result = requests.get(Datalastic.api_base + method, params)
            if api_result.status_code != 200:
                logger.warning("Datalastic: Failed to query vessel %s: %s"%(imo, api_result))
                return None
            response_data = api_result.json()["data"]
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
