import datetime as dt
import time
import requests
import urllib3
from requests.adapters import HTTPAdapter, Retry
import json
import os
import ast

import country_converter as coco

import base
from base.env import get_env
from base.utils import to_datetime, to_list
from base import UNKNOWN_COUNTRY
from base.models import (
    DB_TABLE_KPLER_PRODUCT,
    DB_TABLE_KPLER_FLOW,
    KplerVessel,
    KplerProduct,
    DB_TABLE_KPLER_TRADE,
)
from base.db_utils import upsert
from base.db import session, engine
from base.logger import logger
import pandas as pd
from tqdm import tqdm
import sqlalchemy as sa
from unidecode import unidecode

from kpler.sdk.configuration import Configuration
from kpler.sdk import Platform, exceptions
from kpler.sdk.resources.flows import Flows
from kpler.sdk.resources.products import Products
from kpler.sdk import FlowsDirection, FlowsSplit, FlowsPeriod, FlowsMeasurementUnit
from kpler.sdk.resources.installations import Installations
from .misc import get_split_name

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

        # self.configs = {
        #     "liquids": Configuration(
        #         Platform.Liquids, get_env("KPLER_EMAIL"), get_env("KPLER_PASSWORD")
        #     ),
        #     "lng": Configuration(Platform.LNG, get_env("KPLER_EMAIL"), get_env("KPLER_PASSWORD")),
        #     "dry": Configuration(Platform.Dry, get_env("KPLER_EMAIL"), get_env("KPLER_PASSWORD")),
        # }
        #
        # self.flows_clients = {
        #     "liquids": Flows(self.configs["liquids"]),
        #     "lng": Flows(self.configs["lng"]),
        #     "dry": Flows(self.configs["dry"]),
        # }
        #
        # self.products_clients = {
        #     "liquids": Products(self.configs["liquids"]),
        #     "lng": Products(self.configs["lng"]),
        #     "dry": Products(self.configs["dry"]),
        # }

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

    def get_flows_raw(
        self,
        platform,
        origin_iso2=None,
        destination_iso2=None,
        destination_country=None,
        from_installation=None,
        to_installation=None,
        product=None,
        date_from=None,
        date_to=None,
        split=None,
        granularity=FlowsPeriod.Daily,
        unit=FlowsMeasurementUnit.T,
    ):
        origin_country = (
            unidecode(self.cc.convert(origin_iso2, to="name_short")) if origin_iso2 else None
        )

        if destination_iso2 is not None and destination_country is None:
            destination_country = unidecode(self.cc.convert(destination_iso2, to="name_short"))

        params = {
            "from_zones": [origin_country] if not from_installation else None,
            "to_zones": [destination_country] if destination_country else None,
            "products": product,
            "from_installations": from_installation,
            "to_installations": to_installation,
            "flow_direction": [FlowsDirection.Export],
            "split": [split],
            "granularity": [granularity],
            "start_date": to_datetime(date_from),
            "end_date": to_datetime(date_to),
            "unit": [unit],
            "with_forecast": False,
            "with_intra_country": False,
        }

        try:
            try:
                df = self.flows_clients[platform].get(**params)
            except requests.exceptions.ChunkedEncodingError:
                time.sleep(3)
                df = self.flows_clients[platform].get(**params)
        except exceptions.HttpError as e:
            logger.warning(f"Kpler API error: {e}")
            return None

        if "Date" not in df.columns:
            logger.warning(f"No date in Kpler data: {params} {df}")
            return None

        if "Period End Date" in df.columns:
            df.drop("Period End Date", axis=1, inplace=True)

        df.rename(columns={"Date": "date"}, inplace=True)
        df = df.melt(id_vars=["date"], var_name="split")
        df["unit"] = unit.value
        return df

    def get_installations_brute(self, platform):
        if self.installations_brute.get(platform) is not None:
            return self.installations_brute[platform]

        file = f"assets/kpler/{platform}_installations.csv"
        if os.path.exists("engine"):
            file = f"engine/{file}"

        if not os.path.exists(file):
            token = get_env("KPLER_TOKEN_BRUTE")
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
            token = get_env("KPLER_TOKEN_BRUTE")
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
                country = next((x.get("name") for x in dicts if x.get("type") == "country"), None)
                df["country"] = country
                df["iso2"] = self.cc.convert(country, to="ISO2")
                return df
            except TypeError:
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
            token = get_env("KPLER_TOKEN_BRUTE")
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

    def get_zone_name(self, platform, id):
        installations = self.get_installations_brute(platform=platform)
        id = int(id)
        try:
            name = installations[(installations["id"] == id)]["name"].values[0]
        except IndexError:
            try:
                zones = self.get_zones_brute(platform=platform)
                name = zones[(zones["id"] == id)]["name"].values[0]
            except IndexError:
                try:
                    zones_countries = self.get_zones_countries(platform=platform)
                    name = zones_countries[(zones_countries["id"] == id)]["name"].values[0]
                except IndexError:
                    name = UNKNOWN_COUNTRY
        return name

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
                }
            }
            manual_iso2 = manual_iso2s.get(platform, {}).get(id, None)
            return manual_iso2

    def get_vessel_raw_brute(self, kpler_vessel_id):
        """
        We use token from web interface to get more detailed ship info with kpler vessel id
        :param kpler_vessel_id: id of the vessel on kpler side
        :return:
        Returns KplerVessel object
        """

        token = get_env("KPLER_TOKEN_BRUTE")
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
            token = get_env("KPLER_TOKEN_BRUTE")
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

    def get_trades_raw_brute(
        self,
        platform,
        installation=None,
        from_installation=None,
        origin_iso2=None,
        cursor_after=None,
        product=None,
    ):
        # products = self.get_products_brute(platform=platform)
        installations = self.get_installations_brute(platform=platform)
        zones = self.get_zones_brute(platform=platform)

        if installation and from_installation:
            logger.warning("Please choose either installation or from_installation, not both.")
            return None

        # Get zone dict

        params_raw = {
            "operationName": "voyages",
            "variables": {
                "after": None,
                "size": 1000,
                "where": {
                    "locations": [],
                    "fromLocations": [],
                    "toLocations": [],
                    "vesselIds": [],
                    "productIds": [],
                    "forecast": "EXCLUDE",
                    "freightView": False,
                },
                "sort": {"sortBy": "START"},
            },
            "query": """query voyages($size: Int!, $after: String, $where: VoyageFiltersInput!,
            $sort: VoyageSortsInput) {\n  voyages(size: $size, cursor: $after, where: $where, sort: $sort) {\n
            cursors {\n      after\n      __typename\n    }\n    hasMore\n    items {\n      ...voyage\n
            __typename\n    }\n    __typename\n  }\n}\n\nfragment voyage on Voyage {\n  charter {\n    charterer {\n
                id\n      name\n      __typename\n    }\n    id\n    spotCharterId\n    __typename\n  }\n  end\n
                id\n  portCalls {\n    analystDate\n    berthId\n    billOfLadingCheckedByAnalyst\n    confidence\n
                 constraints {\n      providerId\n      __typename\n    }\n    customsBillOfLadingDate\n
                 customsEntranceDate\n    customsClearanceDate\n    end\n    eta\n    estimatedBerthArrival\n
                 estimatedBerthDeparture\n    flowQuantities {\n      product {\n        ancestorIds\n        api\n
                      id\n        name\n        sulfur\n        __typename\n      }\n      flowQuantity: quantity {\n
                             energy\n        mass\n        volume\n        volume_gas: volumeGas\n
                             __typename\n      }\n      __typename\n    }\n    forecasted\n    forecastedTree {\n
                              confidence\n      installation {\n        id\n        name\n        __typename\n
                              }\n      zone {\n        id\n        name\n        __typename\n      }\n
                              __typename\n    }\n    id\n    installation {\n      id\n      name\n      __typename\n
                                 }\n    isGhost\n    operation\n    reexport\n    start\n    source\n    shipToShip\n
                                    shipToShipInfo {\n      id\n      vessel {\n        id\n        name\n
                                    __typename\n      }\n      __typename\n    }\n    zone {\n      id\n      name\n
                                        type\n      __typename\n    }\n    __typename\n  }\n  start\n  vessel {\n
                                        capacity {\n      energy\n      mass\n      volume\n      volume_gas:
                                        volumeGas\n      __typename\n    }\n    id\n    name\n    __typename\n  }\n
                                        __typename\n}\n""",
        }

        if cursor_after:
            params_raw["variables"]["after"] = cursor_after

        if product is not None:
            params_raw["variables"]["where"]["productIds"] = [
                self.get_product_id(platform=platform, name=product)
            ]

        if from_installation is not None or origin_iso2 is not None:
            params_raw["variables"]["where"]["fromLocations"] = [
                get_installation_dict(origin_iso2, from_installation)
            ]

        if installation:
            params_raw["variables"]["where"]["locations"] = [
                get_installation_dict(origin_iso2, installation)
            ]

        token = get_env("KPLER_TOKEN_BRUTE")
        url = "https://terminal.kpler.com/graphql/"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            r = requests.post(url, json=params_raw, headers=headers)
        except requests.exceptions.ChunkedEncodingError:
            logger.warning(f"Kpler request failed: {params_raw}. Probably empty")
            return None

        response_data = r.json()["data"]["voyages"]

        try:
            cursor, voyages_data = response_data["cursors"]["after"], response_data["items"]
        except KeyError:
            logger.warning("Missing data. Returning")
            return None

        voyages_infos = []
        for voyage in voyages_data:

            def parse_portcalls(portcalls):
                """
                Read the portcalls of a single voyage, and return key voyage information
                i.e. from_installation, to_installation, quantity, unit, product_id, status
                :param portcalls:
                :return:
                """
                if not portcalls:
                    raise ValueError("No portcalls found")

                load_portcalls = [x for x in portcalls if x["operation"] == "LOAD"]
                discharge_portcalls = [x for x in portcalls if x["operation"] == "DISCHARGE"]

                status = base.UNKNOWN
                try:
                    departure_zone_id = load_portcalls[0]["zone"]["id"]
                    departure_zone_name = load_portcalls[0]["zone"]["name"]
                except IndexError:
                    departure_zone_id = None
                    departure_zone_name = None

                if load_portcalls and load_portcalls[0].get("installation"):
                    departure_installation_id = load_portcalls[0]["installation"]["id"]
                    departure_installation_name = load_portcalls[0]["installation"]["name"]
                else:
                    departure_installation_id = None
                    departure_installation_name = None

                if not discharge_portcalls:
                    status = base.ONGOING
                    arrival_zone_id = None
                    arrival_zone_name = None
                    arrival_installation_id = None
                    arrival_installation_name = None
                elif discharge_portcalls:
                    status = base.COMPLETED

                    try:
                        arrival_zone_id = discharge_portcalls[-1]["zone"]["id"]
                        arrival_zone_name = discharge_portcalls[-1]["zone"]["name"]
                    except IndexError:
                        arrival_zone_id = None
                        arrival_zone_name = None

                    if discharge_portcalls[-1].get("installation"):
                        arrival_installation_id = discharge_portcalls[-1]["installation"]["id"]
                        arrival_installation_name = discharge_portcalls[-1]["installation"]["name"]
                    else:
                        arrival_installation_id = None
                        arrival_installation_name = None

                # Get quantities info
                last_portcall = portcalls[-1]
                flows = last_portcall["flowQuantities"]
                if not flows:
                    # Even if not flow, let's store the shipment
                    products = [{}]
                    quantities = [{}]
                else:
                    products = [x["product"] for x in flows]
                    quantities = [x["flowQuantity"] for x in flows]

                def abs_or_none(x):
                    if x is None:
                        return None
                    return abs(x)

                result = [
                    {
                        "departure_zone_id": departure_zone_id,
                        "departure_zone_name": departure_zone_name,
                        "departure_installation_id": departure_installation_id,
                        "departure_installation_name": departure_installation_name,
                        "arrival_zone_id": arrival_zone_id,
                        "arrival_zone_name": arrival_zone_name,
                        "arrival_installation_id": arrival_installation_id,
                        "arrival_installation_name": arrival_installation_name,
                        "status": status,
                        "product_id": products[i].get("id"),
                        "product_name": products[i].get("name"),
                        "value_tonne": abs_or_none(quantities[i].get("mass")),
                        "value_m3": abs_or_none(quantities[i].get("volume")),
                    }
                    for i in range(len(products))
                ]
                return result

            # forcing check of portcall data to make sure we dont mess up here
            vessels = self.get_vessels_brute(platform=platform)
            vessel_id = voyage.get("vessel")["id"]
            try:
                vessel_imo = vessels.imo[vessels.id.astype(int) == int(vessel_id)].values[0]
            except IndexError:
                vessel_imo = None

            voyage_infos = {
                "id": voyage.get("id"),
                "departure_date": dt.datetime.strptime(
                    voyage.get("start"), "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                "arrival_date": dt.datetime.strptime(voyage.get("end"), "%Y-%m-%dT%H:%M:%S.%fZ"),
                "vessel_id": vessel_id,
                "vessel_imo": str(vessel_imo),  # redundant, but just in case
                "others": {"kpler": voyage},
            }

            portcall_infos = parse_portcalls(voyage.get("portCalls"))

            # One voyage can have several flows
            def update_and_return(x, y):
                x_copy = x.copy()
                x_copy.update(y)
                return x_copy

            voyage_infos = [update_and_return(x, voyage_infos) for x in portcall_infos]
            voyages_infos.extend(voyage_infos)

        voyages_df = pd.DataFrame(voyages_infos)
        return cursor, voyages_df

    def get_flows_raw_brute(
        self,
        platform,
        date_from,
        date_to,
        split,
        from_zone=None,
        to_zone=None,
        product=None,
        unit=None,
        granularity=FlowsPeriod.Daily,
        include_total=True,
    ):
        """
        This one uses the token from the web interface,
        and another payload, that allows us to go back further than 1 year
        :param params:
        :param platform:
        :return:
        """
        if from_zone and from_zone.get("name") == "Unknown":
            return None

        from_locations = (
            [
                self.get_zone_dict(
                    id=from_zone.get("id"), name=from_zone.get("name"), platform=platform
                )
            ]
            if from_zone
            else []
        )

        to_locations = (
            [self.get_zone_dict(id=to_zone.get("id"), name=to_zone.get("name"), platform=platform)]
            if to_zone
            else []
        )

        params_raw = {
            "cumulative": False,
            # "filters": {"product": [1334]},
            "filters": {"product": []},
            "flowDirection": "export",
            # "fromLocations": [{"id": 451, "resourceType": "zone"}],
            "fromLocations": from_locations,
            "toLocations": to_locations,
            "granularity": granularity.value,
            "interIntra": "interintra",
            "onlyRealized": True,
            "view": "kpler",
            "withBetaVessels": False,
            "withForecasted": False,
            "withGrades": False,
            "withIncompleteTrades": True,
            "withIntraCountry": False,
            "vesselClassifications": [],
            "withFreightView": False,
            "withProductEstimation": False,
            "splitOn": split.value,
            "startDate": to_datetime(date_from).strftime("%Y-%m-%d"),
            "endDate": to_datetime(date_to).strftime("%Y-%m-%d"),
        }

        if to_zone is not None:
            params_raw["toLocations"] = [
                self.get_zone_dict(
                    id=to_zone.get("id"), name=to_zone.get("name"), platform=platform
                )
            ]

        if product is not None:
            if isinstance(product, dict):
                params_raw["filters"] = {"product": [int(product.get("id"))]}
            else:
                params_raw["filters"] = {
                    "product": [self.get_product_id(platform=platform, name=product)]
                }
        else:
            default_products = {"liquids": [1400, 1328, 1370]}
            params_raw["filters"] = {"product": default_products[platform]}

        token = get_env("KPLER_TOKEN_BRUTE")
        url = {
            "dry": "https://dry.kpler.com/api/flows",
            "liquids": "https://terminal.kpler.com/api/flows",
            "lng": "https://lng.kpler.com/api/flows",
        }.get(platform)
        headers = {
            "Authorization": f"Bearer {token}",
            "x-web-application-version": "v21.316.0",
            "content-type": "application/json",
        }
        try:
            r = self.session.post(url, json=params_raw, headers=headers)
        except (requests.exceptions.ChunkedEncodingError, urllib3.exceptions.ReadTimeoutError):
            logger.warning(f"Kpler request failed: {params_raw}. Probably empty")
            return None

        # read content to dataframe
        try:
            data = r.json()["series"]
        except requests.exceptions.JSONDecodeError:
            logger.warning(f"Kpler request failed: {params_raw}. Probably empty")
            return None

        dfs = []
        for x in data:
            df = pd.concat(
                [pd.DataFrame(y["splitValues"]) for y in x["datasets"]], ignore_index=True
            )
            if len(df) > 0:
                df = pd.concat([df.drop(["values"], axis=1), df["values"].apply(pd.Series)], axis=1)
                df["date"] = x["date"]
                dfs += [df]

            # Add total
            # if include_total:
            #     df_total = pd.DataFrame([y["values"] for y in x["datasets"]])
            #     df_total["date"] = x["date"]
            #     df_total["id"] = KPLER_TOTAL
            #     df_total["name"] = KPLER_TOTAL
            #     dfs += [df_total]

        if not dfs:
            return None

        df = pd.concat(dfs, ignore_index=True)
        df.rename(columns={"name": "split_name", "id": "split_id"}, inplace=True)
        df["split"] = df.apply(
            lambda row: {"id": row["split_id"], "name": row["split_name"]}, axis=1
        )
        df.drop(["split_id", "split_name"], axis=1, inplace=True)
        df = df.melt(id_vars=["date", "split"])
        df["date"] = pd.to_datetime(df["date"])

        units = {
            "mass": FlowsMeasurementUnit.T.value,
            "volume": "m3",
            "energy": "GJ?",
        }
        # Recode variable to unit using the dictionary
        df["unit"] = df.variable.map(units)
        df = df[~pd.isna(df.unit)]
        if unit:
            df = df[df.unit == unit.value]
        df.drop(["variable"], axis=1, inplace=True)
        return df

    def get_flows(
        self,
        platform,
        origin_iso2=None,
        destination_iso2=None,
        product=None,
        split=None,
        from_zone=None,
        from_split=None,
        to_zone=None,
        to_split=FlowsSplit.DestinationCountries,
        granularity=FlowsPeriod.Daily,
        unit=FlowsMeasurementUnit.T,
        date_from=dt.datetime.now() - dt.timedelta(days=365),
        date_to=dt.datetime.now(),
        use_brute_force=False,
    ):
        params = {
            "from_zone": from_zone,
            "to_zone": to_zone,
            "product": product,
            "split": split,
            "granularity": granularity,
            "unit": unit,
            "date_from": date_from,
            "date_to": date_to or dt.datetime.now(),
        }

        if use_brute_force:
            df = self.get_flows_raw_brute(platform=platform, **params, include_total=False)
        else:
            df = self.get_flows_raw(platform=platform, **params)
        if df is None:
            return None

        # if destination_iso2 is None and destination_country is not None:
        #     destination_iso2 = self.cc.convert(destination_country, to="ISO2")
        #
        # if destination_iso2 is not None and destination_country is None:
        #     destination_country = self.cc.convert(destination_iso2, to="name_short")

        # Ideally no NULL otherwise the unique constraints won't work
        # This should work from Postgres 15 onwards
        df["from_split"] = get_split_name(from_split)
        df["to_split"] = get_split_name(to_split)
        df["from_iso2"] = origin_iso2 if origin_iso2 else KPLER_TOTAL

        if product is None:
            product_name = KPLER_TOTAL
        elif isinstance(product, str):
            product_name = product
        elif isinstance(product, dict):
            product_name = product.get("name")
        else:
            raise ValueError(f"Unknown product type: {type(product)}")

        df["from_zone"] = df.apply(lambda x: from_zone or {"id": 0, "name": None}, axis=1)
        df["to_zone"] = df.apply(lambda x: to_zone or {"id": 0, "name": None}, axis=1)
        df["product"] = product_name
        df["unit"] = unit.value
        df["platform"] = platform
        df = df.rename(columns={"Date": "date"})

        # df = df.melt(
        #     id_vars=[
        #         x
        #         for x in [
        #             "date",
        #             "origin_iso2",
        #             "destination_iso2",
        #             "from_installation",
        #             "to_installation",
        #             "product",
        #             "unit",
        #             "platform",
        #         ]
        #         if x in df.columns
        #     ],
        #     var_name="split",
        #     value_name="value",
        # )

        def split_to_column(df, split):
            if split in [
                FlowsSplit.DestinationCountries,
                FlowsSplit.DestinationInstallations,
                FlowsSplit.DestinationPorts,
            ]:
                df["to_zone"] = df["split"]
            elif split in [
                FlowsSplit.OriginCountries,
                FlowsSplit.OriginInstallations,
                FlowsSplit.OriginPorts,
            ]:
                df["from_zone"] = df["split"]
            elif split == FlowsSplit.Products:
                df["product"] = df["split"].apply(lambda x: x.get("name"))

            return df

        df = split_to_column(df, split)
        df = df.drop(columns=["split"])

        df["from_zone_id"] = df.from_zone.apply(lambda x: int(x.get("id")))
        df["to_zone_id"] = df.to_zone.apply(lambda x: int(x.get("id")))

        df["from_zone_name"] = df.from_zone.apply(lambda x: x.get("name"))
        df["to_zone_name"] = df.to_zone.apply(lambda x: x.get("name"))
        #
        # df["from_zone_name"] = df.from_zone_id.apply(
        #     lambda x: self.get_zone_name(platform=platform, id=x)
        # )
        # df["to_zone_name"] = df.to_zone_id.apply(
        #     lambda x: self.get_zone_name(platform=platform, id=x)
        # )
        df["to_iso2"] = df.to_zone_id.apply(lambda x: self.get_zone_iso2(platform=platform, id=x))
        df["from_iso2"] = df.from_zone_id.apply(
            lambda x: self.get_zone_iso2(platform=platform, id=x)
        )

        df.drop(columns=["from_zone", "to_zone"], inplace=True)
        # # Sometimes we have duplicated values
        # df = (
        #     df.groupby(
        #         [
        #             "date",
        #             "origin_iso2",
        #             "destination_iso2",
        #             "destination_country",
        #             "from_zone",
        #             "to_zone",
        #             "from_split",
        #             "to_split"
        #             "product",
        #             "unit",
        #             "platform",
        #         ],
        #         dropna=False,
        #     )
        #     .sum()
        #     .reset_index()
        # )

        return df

    def get_products(self, platform=None):
        platforms = self.platforms if platform is None else [platform]

        def get_platform_products(platform):
            if self.products.get(platform) is None:
                # This yields 17 commodities while we had 20 when using the API
                # products = self.get_products_brute(platform=platform)
                # products = products[~pd.isna(products.closestAncestorCommodity)]
                # products = products[~pd.isna(products.closestAncestorGroup)]
                # commodities = products.closestAncestorCommodity.apply(lambda x: pd.Series(x))
                # commodities["group_name"] = products.closestAncestorGroup.apply(lambda x: x.get('name'))
                # commodities["family_name"] = products.closestAncestorFamily.apply(lambda x: x.get('name'))
                # commodities["belongs_to_platform"] = products.ancestors.apply(lambda x: any([y.get('name').lower() == platform and y.get('type') == 'family' for y in x]))
                # commodities = commodities[commodities.belongs_to_platform]
                # commodities = commodities.drop_duplicates()
                # commodities["platform"] = platform
                # commodities.rename(
                #     columns={
                #         "family_name": "family",
                #         "group_name": "group",
                #     },
                #     inplace=True,
                # )
                # columns = ["id", "name", "type", "family", "group"]
                # self.products[platform] = commodities[columns]
                products = pd.read_sql(
                    KplerProduct.query.filter(KplerProduct.platform == platform).statement,
                    session.bind,
                )
                self.products[platform] = products

            return self.products.get(platform)

        df = pd.concat([get_platform_products(platform) for platform in platforms])
        # df = df[["id", "name", "family", "type", "group", "platform"]].drop_duplicates()
        df = df[["name", "family", "group", "platform"]].drop_duplicates()
        df = df[~pd.isna(df.name)]
        return df

    def get_product_id(self, platform, name):
        products = self.get_products(platform=platform)
        product = products[products.name == name]
        return int(product.id.values[0]) if len(product) == 1 else None


def fill_products():
    scraper = KplerScraper()
    products = scraper.get_products()
    upsert(products, DB_TABLE_KPLER_PRODUCT, "kpler_product_pkey")
    return
