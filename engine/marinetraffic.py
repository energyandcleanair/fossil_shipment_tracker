import requests
import json

from base.logger import logger
from base.env import get_env
from models import Ship


class Marinetraffic:

    api_base = 'https://services.marinetraffic.com/api/'
    api_key = None
    cache_file_ship = 'cache/marinetraffic/ships.json'

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

        if not cls.api_key:
            cls.api_key = get_env("KEY_MARINETRAFFIC")

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
