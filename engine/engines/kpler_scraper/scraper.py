import datetime as dt
import time
from typing import Optional
import pyotp
import requests
import urllib3
from requests.adapters import HTTPAdapter, Retry
import json
import os
import ast

import country_converter as coco

from base.env import get_env
from base import UNKNOWN_COUNTRY
from base.models import (
    DB_TABLE_KPLER_PRODUCT,
    KplerVessel,
    KplerProduct,
)
from base.db_utils import upsert
from base.db import session
from base.logger import logger
import pandas as pd
from unidecode import unidecode
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


from urllib.parse import parse_qs

from engines.kpler_scraper.token_manager import KplerCredentials, KplerTokenManager

KPLER_TOTAL = "Total"
CACHE_BASE_DIR = "cache/kpler/"


def split_into(s, n):
    size, remainder = divmod(len(s), n)
    start = 0
    for i in range(n):
        length = size + (i < remainder)
        yield s[start : start + length]
        start += length


class KplerClient:
    def __init__(
        self,
        *,
        credentials=KplerCredentials.from_env(),
        # Allows us to inject a different token manager for testing
        token_manager_provider=lambda credentials: KplerTokenManager(credentials=credentials),
        max_requests_per_second=3.0,
    ):
        self.credentials = credentials
        self.session = requests.Session()
        retries = Retry(
            total=10,
            connect=3,
            read=3,
            redirect=3,
            other=3,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            # We don't make any changes to the data so we can safely retry on posts.
            allowed_methods=["GET", "POST"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        self.token_manager: KplerTokenManager = token_manager_provider(credentials)

        self.last_request_time = None
        self.max_requests_per_second = max_requests_per_second

    def fetch(
        self,
        url,
        *,
        params=None,
        body=None,
        base_path="/api/",
        reauth=False,
    ):
        self._handle_rate_limiting()

        token = self.token_manager.get_token(reauth=reauth)
        headers = self._generate_headers(token)

        full_url = f"https://terminal.kpler.com{base_path}{url}"

        logger.info(f"Making Kpler request with url={full_url}, params={params}, body={body}")

        result = (
            self.session.post(full_url, params=params, headers=headers, json=body)
            if body
            else self.session.get(full_url, params=params, headers=headers)
        )

        if reauth and result.status_code == 401:
            raise RuntimeError(f"Request failed with 401 even after reauth. url={full_url}")

        if result.status_code == 401:
            return self.fetch(url, params=params, body=body, base_path=base_path, reauth=True)

        return result

    def _generate_headers(self, token):
        access_token = token.access_token

        headers = {
            "x-access-token": access_token,
            "use-access-token": "true",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        }

        return headers

    def _handle_rate_limiting(self):
        if self.last_request_time is not None:
            min_time_between_requests = 1 / self.max_requests_per_second
            time_since_last_request = time.time() - self.last_request_time
            if time_since_last_request < min_time_between_requests:
                time.sleep(min_time_between_requests - time_since_last_request)

        self.last_request_time = time.time()


_kpler_client = None


def get_singleton_kpler_client():
    global _kpler_client
    if _kpler_client is None:
        _kpler_client = KplerClient()
    return _kpler_client


### IMPORTANT
### Certain country names and to_zone_name are still empty after
### scraping, and have been updated manually in the database
### Before scraping again, add a constraint or a check that this is not the case
###
### Other required fixes:
### - Singapore has duplicates: zone_id 1109 and 833
### - Koweit has duplicates:zone_id 110755 and 505
### Ended up removing 833 and 110755 as having the lowest values
class KplerScraper:

    default_trade_flow_params = {
        "flowDirection": "export",
        "withBetaVessels": False,
        "onlyRealized": True,
        "withForecasted": True,
        "withFreightView": False,
        "withIncompleteTrades": True,
        "withIntraCountry": True,
        "withProductEstimation": True,
        "filters": {},
    }

    @staticmethod
    def default_params():
        return {**KplerScraper.default_trade_flow_params}

    def __init__(self, client=get_singleton_kpler_client()):
        self.cc = coco.CountryConverter()

        # To cache products
        self.products = None

        # Brute-force infos
        self.products_brute = None
        self.zones_brute = None
        self.installations_brute = None
        self.vessels_brute = None

        # Processed
        self.zones_countries = None

        self.client = client

    def get_installations(self, origin_iso2, split, product=None):
        # We collect flows split by installation
        # and get unique values
        flows = self.get_flows(
            origin_iso2=origin_iso2,
            split=split,
            product=product,
        )
        installations = list(flows.from_installation.unique())
        installations = [x for x in installations if x.lower() != "unknown"]
        installations = [x for x in installations if x.lower() != "total"]
        return installations

    def get_installations_brute(self):
        if self.installations_brute is not None:
            return self.installations_brute

        file = f"{CACHE_BASE_DIR}/installations.csv"

        if not os.path.exists(file):
            response = self.client.fetch("installations")
            data_from_kpler = pd.DataFrame(response.json())
            data_from_kpler.to_csv(file, index=False)

        data = pd.read_csv(file)

        self.installations_brute = data
        return data

    def get_zones_brute(self):
        if self.zones_brute is not None:
            return self.zones_brute

        file = f"{CACHE_BASE_DIR}/zones.csv"

        if not os.path.exists(file):
            if not os.path.exists(CACHE_BASE_DIR):
                os.makedirs(CACHE_BASE_DIR)
            response = self.client.fetch("zones")
            data_from_kpler = pd.DataFrame(response.json())
            data_from_kpler.to_csv(file, index=False)

        data = pd.read_csv(file)

        self.zones_brute = data
        return data

    def get_zones_countries(self):
        if self.zones_countries is not None:
            return self.zones_countries

        def parent_zones_to_zones_df(parent_zones):
            try:
                dicts = ast.literal_eval(parent_zones)
                df = pd.DataFrame([x for x in dicts if x.get("resourceType") == "zone"])
                # Add countries
                country = next(
                    (x.get("name") for x in dicts if x.get("type") == "country"),
                    next(
                        (x.get("name") for x in dicts if x.get("type") == "country_checkpoint"),
                        None,
                    ),
                )
                if country is None:
                    return pd.DataFrame()
                df["country"] = country
                df["iso2"] = self.cc.convert(country, to="ISO2")
                return df
            except ValueError:
                return pd.DataFrame()

        zones = self.get_zones_brute()
        zones_with_country = pd.concat([parent_zones_to_zones_df(x) for x in zones.parentZones])
        zones_with_country = zones_with_country[["id", "name", "country", "iso2"]].drop_duplicates()

        self.zones_countries = zones_with_country
        return zones_with_country

    def get_products_brute(self):
        if self.products_brute is not None:
            return self.products_brute

        file = f"{CACHE_BASE_DIR}/products.csv"

        if not os.path.exists(file):
            response = self.client.fetch("products")
            data_from_kpler = pd.DataFrame(response.json())
            data_from_kpler.to_csv(file, index=False)

        data = pd.read_csv(file)

        self.products_brute = data
        return data

    def get_commodities_brute(self):
        products = self.get_products_brute()
        products = products[~pd.isna(products.closestAncestorCommodity)]
        commodities = products.closestAncestorCommodity.apply(
            lambda x: pd.Series(json.loads(x.replace("'", '"')))
        )
        commodities = commodities.drop_duplicates()
        return commodities

    def get_zone_dict(self, iso2=None, id=None, name=None):
        if id is not None and int(id) == 0:
            return None

        if iso2 is not None and id is None and name is None:
            name = unidecode(self.cc.convert(iso2, to="name_short"))
            if iso2 == "RU":
                name = "Russian Federation"
            elif iso2 == "TR":
                name = "Turkey"
            elif iso2 == "SG":
                name = "Singapore Republic"
            elif iso2 == "CG":
                name = "Republic of the Congo"
            elif iso2 == "CD":
                name = "Democratic Republic of the Congo"
            elif iso2 == "LC":
                name = "Saint Lucia"
            elif iso2 == "CI":
                name = "Ivory Coast"
            elif iso2 == "FO":
                name = "Faroe Islands"
            elif iso2 == "BN":
                name = "Brunei"
            elif iso2 == "CV":
                name = "Cape Verde"

        found = False
        types = {
            "zone": self.get_zones_brute(),
            "installation": self.get_installations_brute(),
        }
        for type, zones in types.items():
            matching = zones
            if id is not None:
                matching = matching[matching["id"] == int(id)]
            if name is not None:
                matching = matching[matching["name"] == name]
            if len(matching) == 1:
                found = True
                break

        if not found:
            logger.warning(f"Zone not found: (country: {iso2}, id: {id}, name: {name})")
            return None

        return {"id": int(matching["id"].values[0]), "resourceType": type}

    def get_zone_iso2(self, id):
        zones_countries = self.get_zones_countries()
        found = zones_countries[(zones_countries["id"] == id)]["iso2"]
        if len(found) == 1:
            return found.values[0]
        else:
            # Manual values
            manual_iso2s = {
                299: "EG",
                943: "AE",
                561: "MT",
                261: "DJ",
                707: "PA",
                175: "CV",
                343: "GI",
            }
            manual_iso2 = manual_iso2s.get(id, None)
            return manual_iso2

    def get_zone_name(self, id, name=None):
        if name is not None:
            return name

        manual_names = {
            299: "Egypt",
            943: "United Arab Emirates",
            561: "Malta",
            261: "Djibouti",
            707: "Panama",
            175: "Cap Verde",
            343: "Gibraltar",
        }

        if manual_names.get(id, None) is not None:
            return manual_names[id]

        zones_countries = self.get_zones_countries()
        found = zones_countries[(zones_countries["id"] == id)]["name"]
        if len(found) == 1:
            return found.values[0]
        elif id == 0:
            return UNKNOWN_COUNTRY
        else:
            raise ValueError(f"Zone name not found: {id}")

    def get_vessel_raw_brute(self, kpler_vessel_id):
        """
        We use token from web interface to get more detailed ship info with kpler vessel id
        :param kpler_vessel_id: id of the vessel on kpler side
        :return:
        Returns KplerVessel object
        """

        try:
            r = self.client.fetch(f"vessels/{kpler_vessel_id}")
        except requests.exceptions.ChunkedEncodingError:
            logger.warning(f"Kpler request failed: {kpler_vessel_id}.")
            return None

        response_data = r.json()

        vessel_data = {
            "id": response_data["id"],
            "mmsi": [response_data["mmsi"]],
            "imo": response_data["imo"],
            "name": [response_data.get("name")],
            "type": response_data.get("statcode")["name"],
            "dwt": response_data.get("deadWeight"),
            "country_iso2": response_data.get("flagName"),
            "others": json.dumps({"kpler": response_data}),
        }

        return KplerVessel(**vessel_data)

    def get_vessels_brute(self):
        """
        We use token from web interface to get more detailed ship info with kpler vessel id
        :param kpler_vessel_id: id of the vessel on kpler side
        :return:
        Returns KplerShip object
        """
        if self.vessels_brute is not None:
            return self.vessels_brute

        file = f"{CACHE_BASE_DIR}/vessels.csv"

        if not os.path.exists(file):
            response = self.client.fetch("vessels")
            data_from_kpler = pd.DataFrame(response.json())
            data_from_kpler.to_csv(file, index=False)

        data = pd.read_csv(file)

        self.vessels_brute = data
        return data

    def get_products(self):
        return pd.read_sql(
            KplerProduct.query.statement,
            session.bind,
        )

    def get_product_id(self, name):
        manual_values = {"Crude/Co": 1370}
        if name in manual_values:
            return manual_values[name]

        products = self.get_products()
        product = products[products.name == name]
        return int(product.id.values[0]) if len(product) == 1 else None

    def fix_zone_id(self, id):
        """
        Certain zone ids are different based on whether we query with
        specified destination or specified origin. Meaning there can be double counting.
        We clean the zones in this function.
        MUST be run after the flows have been scraped!
        :return:
        """
        manual_fixes = {"1109": "833"}  # SINGAPORE vs SINGAPORE REPUBLIC
        return type(id)(manual_fixes.get(id, id))
