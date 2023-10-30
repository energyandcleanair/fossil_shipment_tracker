import datetime as dt
import time
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


KPLER_TOTAL = "Total"


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
    def __init__(self):
        self.platforms = ["liquids", "lng", "dry"]
        self.cc = coco.CountryConverter()

        # To cache products
        self.products = {}

        # Brute-force infos
        self.products_brute = {}
        self.zones_brute = {}
        self.installations_brute = {}
        self.vessels_brute = {}

        # Processed
        self.zones_countries = {}

        self.session = requests.Session()
        retries = Retry(total=10, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        self.login()

    def login(self):
        # r = self.session.post(
        #     "https://terminal.kpler.com/api/login",
        #     data={"email": get_env("KPLER_EMAIL"),
        #           "password": get_env("KPLER_PASSWORD")},
        #     headers={"Content-Type": "application/json",
        #              "Accept": "application/json",
        #              "origin": "https://terminal.kpler.com",
        #              "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        # )
        # if r.status_code != 200:
        #     raise Exception("Kpler login failed")
        # self.token = r.json()["token"]

        self.token = get_env("KPLER_TOKEN_BRUTE")

    def get_installations(self, origin_iso2, platform, split, product=None):
        # We collect flows split by installation
        # and get unique values
        flows = self.get_flows(
            origin_iso2=origin_iso2,
            platform=platform,
            split=split,
            product=product,
        )
        installations = list(flows.from_installation.unique())
        installations = [x for x in installations if x.lower() != "unknown"]
        installations = [x for x in installations if x.lower() != "total"]
        return installations

    def get_installations_brute(self, platform):
        if self.installations_brute.get(platform) is not None:
            return self.installations_brute[platform]

        file = f"assets/kpler/{platform}_installations.csv"
        if os.path.exists("engine"):
            file = f"engine/{file}"

        if not os.path.exists(file):
            token = self.token  # get_env("KPLER_TOKEN_BRUTE")
            url = {
                "dry": "https://dry.kpler.com/api/installations",
                "liquids": "https://terminal.kpler.com/api/installations",
                "lng": "https://lng.kpler.com/api/installations",
            }.get(platform)
            headers = {"Authorization": f"Bearer {token}"}
            r = self.session.get(url, headers=headers)
            data = pd.DataFrame(r.json())
            data.to_csv(file, index=False)
        else:
            data = pd.read_csv(file)

        self.installations_brute[platform] = data
        return data

    def get_zones_brute(self, platform):
        if self.zones_brute.get(platform) is not None:
            return self.zones_brute[platform]

        file = f"assets/kpler/{platform}_zones.csv"
        if os.path.exists("engine"):
            file = f"engine/{file}"

        if not os.path.exists(file):
            token = self.token  # get_env("KPLER_TOKEN_BRUTE")
            url = {
                "dry": "https://dry.kpler.com/api/zones",
                "liquids": "https://terminal.kpler.com/api/zones",
                "lng": "https://lng.kpler.com/api/zones",
            }.get(platform)
            headers = {"Authorization": f"Bearer {token}"}
            r = self.session.get(url, headers=headers)
            data = pd.DataFrame(r.json())
            data.to_csv(file, index=False)
        else:
            data = pd.read_csv(file)

        self.zones_brute[platform] = data
        return data

    def get_zones_countries(self, platform):
        if self.zones_countries.get(platform) is not None:
            return self.zones_countries[platform]

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

        zones = self.get_zones_brute(platform=platform)
        zones_with_country = pd.concat([parent_zones_to_zones_df(x) for x in zones.parentZones])
        zones_with_country = zones_with_country[["id", "name", "country", "iso2"]].drop_duplicates()

        self.zones_countries[platform] = zones_with_country
        return zones_with_country

    def get_products_brute(self, platform):
        if self.products_brute.get(platform) is not None:
            return self.products_brute[platform]

        file = f"assets/kpler/{platform}_products.csv"
        if os.path.exists("engine"):
            file = f"engine/{file}"

        if not os.path.exists(file):
            token = self.token  # get_env("KPLER_TOKEN_BRUTE")
            url = {
                "dry": "https://dry.kpler.com/api/products",
                "liquids": "https://terminal.kpler.com/api/products",
                "lng": "https://lng.kpler.com/api/products",
            }.get(platform)
            headers = {"Authorization": f"Bearer {token}"}
            r = self.session.get(url, headers=headers)
            data = pd.DataFrame(r.json())
            data.to_csv(file, index=False)
        else:
            data = pd.read_csv(file)

        self.products_brute[platform] = data
        return data

    def get_commodities_brute(self, platform):
        products = self.get_products_brute(platform)
        products = products[~pd.isna(products.closestAncestorCommodity)]
        commodities = products.closestAncestorCommodity.apply(
            lambda x: pd.Series(json.loads(x.replace("'", '"')))
        )
        commodities = commodities.drop_duplicates()
        return commodities

    def get_zone_dict(self, platform, iso2=None, id=None, name=None):
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

        found = False
        types = {
            "zone": self.get_zones_brute(platform=platform),
            "installation": self.get_installations_brute(platform=platform),
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
            logger.warning(f"Zone not found: {platform} {iso2} {id} {name}")
            return None

        return {"id": int(matching["id"].values[0]), "resourceType": type}

    def get_zone_iso2(self, platform, id):
        zones_countries = self.get_zones_countries(platform=platform)
        found = zones_countries[(zones_countries["id"] == id)]["iso2"]
        if len(found) == 1:
            return found.values[0]
        else:
            # Manual values
            manual_iso2s = {
                "liquids": {
                    299: "EG",
                    943: "AE",
                    561: "MT",
                    261: "DJ",
                    707: "PA",
                    175: "CV",
                    343: "GI",
                },
                "lng": {
                    299: "EG",
                    943: "AE",
                    561: "MT",
                    261: "DJ",
                    707: "PA",
                    175: "CV",
                    343: "GI",
                },
                "dry": {
                    299: "EG",
                    943: "AE",
                    561: "MT",
                    261: "DJ",
                    707: "PA",
                    175: "CV",
                    343: "GI",
                },
            }
            manual_iso2 = manual_iso2s.get(platform, {}).get(id, None)
            return manual_iso2

    def get_zone_name(self, platform, id, name=None):
        if name is not None:
            return name

        manual_names = {
            "liquids": {
                299: "Egypt",
                943: "United Arab Emirates",
                561: "Malta",
                261: "Djibouti",
                707: "Panama",
                175: "Cap Verde",
                343: "Gibraltar",
            },
            "lng": {
                299: "Egypt",
                943: "United Arab Emirates",
                561: "Malta",
                261: "Djibouti",
                707: "Panama",
                175: "Cap Verde",
                343: "Gibraltar",
            },
            "dry": {
                299: "Egypt",
                943: "United Arab Emirates",
                561: "Malta",
                261: "Djibouti",
                707: "Panama",
                175: "Cap Verde",
                343: "Gibraltar",
            },
        }

        if manual_names.get(platform, {}).get(id, None) is not None:
            return manual_names[platform][id]

        zones_countries = self.get_zones_countries(platform=platform)
        found = zones_countries[(zones_countries["id"] == id)]["name"]
        if len(found) == 1:
            return found.values[0]
        elif id == 0:
            return UNKNOWN_COUNTRY
        else:
            raise ValueError(f"Zone name not found: {platform} {id}")

    def get_vessel_raw_brute(self, kpler_vessel_id):
        """
        We use token from web interface to get more detailed ship info with kpler vessel id
        :param kpler_vessel_id: id of the vessel on kpler side
        :return:
        Returns KplerVessel object
        """

        token = self.token  # get_env("KPLER_TOKEN_BRUTE")
        url = "https://terminal.kpler.com/api/vessels/{}".format(kpler_vessel_id)
        headers = {"Authorization": f"Bearer {token}"}
        try:
            r = self.session.get(url, headers=headers)
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

    def get_vessels_brute(self, platform):
        """
        We use token from web interface to get more detailed ship info with kpler vessel id
        :param kpler_vessel_id: id of the vessel on kpler side
        :return:
        Returns KplerShip object
        """
        if self.vessels_brute.get(platform) is not None:
            return self.vessels_brute[platform]

        file = f"assets/kpler/{platform}_vessels.csv"
        if os.path.exists("engine"):
            file = f"engine/{file}"

        if not os.path.exists(file):
            token = self.token  # get_env("KPLER_TOKEN_BRUTE")
            url = {
                "dry": "https://dry.kpler.com/api/vessels",
                "liquids": "https://terminal.kpler.com/api/vessels",
                "lng": "https://lng.kpler.com/api/vessels",
            }.get(platform)
            headers = {"Authorization": f"Bearer {token}"}
            r = self.session.get(url, headers=headers)
            data = pd.DataFrame(r.json())
            data.to_csv(file, index=False)
        else:
            data = pd.read_csv(file)

        self.vessels_brute[platform] = data
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


class KplerProductInfo:
    cache = {}
    session = requests.Session()
    retries = Retry(total=10, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    token = get_env("KPLER_TOKEN_BRUTE")

    @classmethod
    def get_infos(cls, platform, id):
        if id in KplerProductInfo.cache:
            return KplerProductInfo.cache[id]
        else:
            infos = KplerProductInfo.collect_infos(platform=platform, id=id)
            KplerProductInfo.cache[id] = infos
            return infos

    @classmethod
    def get_grade_name(cls, platform, id):
        infos = cls.get_infos(platform=platform, id=id)
        return infos.get("closestAncestorGrade", {}).get("name")

    @classmethod
    def get_commodity_name(cls, platform, id):
        infos = cls.get_infos(platform=platform, id=id)
        return infos.get("closestAncestorCommodity", {}).get("name")

    @classmethod
    def get_group_name(cls, platform, id):
        infos = cls.get_infos(platform=platform, id=id)
        return infos.get("closestAncestorGroup", {}).get("name")

    @classmethod
    def get_family_name(cls, platform, id):
        infos = cls.get_infos(platform=platform, id=id)
        return infos.get("closestAncestorFamily", {}).get("name")

    @classmethod
    def collect_infos(cls, platform, id):
        token = KplerProductInfo.token  # get_env("KPLER_TOKEN_BRUTE")
        url = {
            "dry": "https://dry.kpler.com/api/products",
            "liquids": "https://terminal.kpler.com/api/products",
            "lng": "https://lng.kpler.com/api/products",
        }.get(platform)
        headers = {
            "Authorization": f"Bearer {token}",
            "x-web-application-version": "v21.316.0",
            "content-type": "application/json",
        }
        try:
            r = KplerProductInfo.session.get(f"{url}/{id}", headers=headers)
        except (requests.exceptions.ChunkedEncodingError, urllib3.exceptions.ReadTimeoutError):
            logger.warning(f"Kpler request failed")
            return None

        return r.json()


def fill_products():
    scraper = KplerScraper()
    products = scraper.get_products()
    upsert(products, DB_TABLE_KPLER_PRODUCT, "kpler_product_pkey")
    return
