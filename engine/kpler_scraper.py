import datetime as dt
import time
import requests
import json
import os

import country_converter as coco
from base.env import get_env
from base.utils import to_datetime, to_list
from base import UNKNOWN_COUNTRY
from base.models import DB_TABLE_KPLER_PRODUCT, DB_TABLE_KPLER_FLOW, KplerShip
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

KPLER_TOTAL = "Total"


class KplerScraper:
    def __init__(self):
        self.platforms = ["liquids", "lng", "dry"]

        self.configs = {
            "liquids": Configuration(
                Platform.Liquids, get_env("KPLER_EMAIL"), get_env("KPLER_PASSWORD")
            ),
            "lng": Configuration(Platform.LNG, get_env("KPLER_EMAIL"), get_env("KPLER_PASSWORD")),
            "dry": Configuration(Platform.Dry, get_env("KPLER_EMAIL"), get_env("KPLER_PASSWORD")),
        }

        self.flows_clients = {
            "liquids": Flows(self.configs["liquids"]),
            "lng": Flows(self.configs["lng"]),
            "dry": Flows(self.configs["dry"]),
        }

        self.products_clients = {
            "liquids": Products(self.configs["liquids"]),
            "lng": Products(self.configs["lng"]),
            "dry": Products(self.configs["dry"]),
        }

        self.cc = coco.CountryConverter()

        # To cache products
        self.products = {}

        # Brute-force infos
        self.products_brute = {}
        self.zones_brute = {}
        self.installations_brute = {}

    def get_installations(self, origin_iso2, platform, product=None):
        # We collect flows split by installation
        # and get unique values
        flows = self.get_flows(
            origin_iso2=origin_iso2,
            platform=platform,
            split=FlowsSplit.OriginInstallations,
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
        destination_country = (
            unidecode(self.cc.convert(destination_iso2, to="name_short"))
            if destination_iso2
            else None
        )

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
            logger.error(f"Kpler API error: {e}")
            return None

        if "Date" not in df.columns:
            logger.error(f"No date in Kpler data: {params} {df}")
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
            r = requests.get(url, headers=headers)
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
            r = requests.get(url, headers=headers)
            data = pd.DataFrame(r.json())
            data.to_csv(file, index=False)
        else:
            data = pd.read_csv(file)

        self.zones_brute[platform] = data
        return data

    # def get_products_brute(self, platform):
    #     if self.products_brute.get(platform) is not None:
    #         return self.products_brute[platform]
    #
    #     token = get_env("KPLER_TOKEN_BRUTE")
    #     url = {
    #         "dry": "https://dry.kpler.com/api/products",
    #         "liquids": "https://terminal.kpler.com/api/products",
    #     }.get(platform)
    #     headers = {"Authorization": f"Basic {token}"}
    #     r = requests.get(url, params={"type": "commodity"}, headers=headers)
    #     data = pd.DataFrame(r.json())
    #
    #     data_ancestor = data.closestAncestorCommodity.apply(lambda x: pd.Series(x))
    #     # data.drop_duplicates(inplace=True)
    #     # data.to_csv(f"assets/kpler/{platform}_products.csv", index=False)
    #
    #     try:
    #         data = pd.read_csv(f"assets/kpler/{platform}_products.csv")
    #     except FileNotFoundError:
    #         data = pd.read_csv(f"engine/assets/kpler/{platform}_products.csv")
    #     self.products_brute[platform] = data
    #     return data

    def get_vessel_raw_brute(
            self,
            kpler_vessel_id
    ):
        """
        We use token from web interface to get more detailed ship info with kpler vessel id
        :param kpler_vessel_id: id of the vessel on kpler side
        :return:
        Returns KplerShip object
        """

        token = get_env("KPLER_TOKEN_BRUTE")
        url = "https://terminal.kpler.com/api/vessels/{}".format(kpler_vessel_id)
        headers = {"Authorization": f"Bearer {token}"}
        try:
            r = requests.get(url, headers=headers)
        except requests.exceptions.ChunkedEncodingError:
            logger.error(f"Kpler request failed: {kpler_vessel_id}.")
            return None

        response_data = r.json()

        ship_data = {
            "id": response_data["id"],
            "mmsi": [response_data["mmsi"]],
            "imo": response_data["imo"],
            "name": [response_data.get("name")],
            "type": response_data.get("statcode")["name"],
            "dwt": response_data.get("deadWeight"),
            "country_iso2": response_data.get("flagName"),
            "others": {"kpler": response_data},
        }

        return KplerShip(**ship_data)

    def get_flows_raw_brute(
            self,
            platform,
            origin_iso2,
            destination_iso2,
            date_from,
            date_to,
            split,
            from_installation=None,
            to_installation=None,
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
        # products = self.get_products_brute(platform=platform)
        installations = self.get_installations_brute(platform=platform)
        zones = self.get_zones_brute(platform=platform)

        # Get zone dict
        def get_installation_dict(iso2, installation):
            if installation is None:
                name = unidecode(self.cc.convert(iso2, to="name_short"))
                if iso2 == "RU":
                    name = "Russian Federation"
                elif iso2 == "TR":
                    name = "Turkey"
            else:
                name = from_installation

            try:
                id = installations[(installations["name"] == name)]["id"].values[0]
                type = "installation"
            except IndexError:
                id = zones[(zones["name"] == name)]["id"].values[0]
                type = "zone"

            return {"id": int(id), "resourceType": type}

        params_raw = {
            "cumulative": False,
            # "filters": {"product": [1334]},
            "filters": {"product": []},
            "flowDirection": "export",
            # "fromLocations": [{"id": 451, "resourceType": "zone"}],
            "fromLocations": [],
            "toLocations": [],
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
            "numberOfSplits": 1000,
        }

        if product is not None:
            params_raw["filters"] = {
                "product": [self.get_product_id(platform=platform, name=product)]
            }

        if from_installation is not None or origin_iso2 is not None:
            params_raw["fromLocations"] = [get_installation_dict(origin_iso2, from_installation)]

        if to_installation is not None or destination_iso2 is not None:
            params_raw["toLocations"] = [get_installation_dict(destination_iso2, to_installation)]

        token = get_env("KPLER_TOKEN_BRUTE")
        url = {
            "dry": "https://dry.kpler.com/api/flows",
            "liquids": "https://terminal.kpler.com/api/flows",
            "lng": "https://lng.kpler.com/api/flows",
        }.get(platform)
        headers = {"Authorization": f"Basic {token}"}
        try:
            r = requests.post(url, json=params_raw, headers=headers)
        except requests.exceptions.ChunkedEncodingError:
            logger.error(f"Kpler request failed: {params_raw}. Probably empty")
            return None

        # read content to dataframe
        data = r.json()["series"]
        dfs = []
        for x in data:
            df = pd.concat(
                [pd.DataFrame(y["splitValues"]) for y in x["datasets"]], ignore_index=True
            )
            if len(df) > 0:
                df = pd.concat([df.drop(["values"], axis=1), df["values"].apply(pd.Series)], axis=1)
                df["date"] = x["date"]
                df.drop(["id"], axis=1, inplace=True)
                dfs += [df]

            # Add total
            if include_total:
                df_total = pd.DataFrame([y["values"] for y in x["datasets"]])
                df_total["date"] = x["date"]
                df_total["name"] = KPLER_TOTAL
                dfs += [df_total]

        if not dfs:
            return None

        df = pd.concat(dfs, ignore_index=True)
        df.rename(columns={"name": "split"}, inplace=True)
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
            from_installation=None,
            to_installation=None,
            split=FlowsSplit.DestinationCountries,
            granularity=FlowsPeriod.Daily,
            unit=FlowsMeasurementUnit.T,
            date_from=dt.datetime.now() - dt.timedelta(days=365),
            date_to=dt.datetime.now(),
            use_brute_force=False,
    ):
        params = {
            "origin_iso2": origin_iso2,
            "destination_iso2": destination_iso2,
            "from_installation": from_installation,
            "to_installation": to_installation,
            "product": product,
            "split": split,
            "granularity": granularity,
            "unit": unit,
            "date_from": date_from,
            "date_to": date_to,
        }

        if use_brute_force:
            df = self.get_flows_raw_brute(platform=platform, **params, include_total=False)
        else:
            df = self.get_flows_raw(platform=platform, **params)
        if df is None:
            return None

        # Ideally no NULL otherwise the unique constraints won't work
        # This should work from Postgres 15 onwards
        df["origin_iso2"] = origin_iso2 if origin_iso2 else KPLER_TOTAL
        df["destination_iso2"] = destination_iso2 if destination_iso2 else KPLER_TOTAL
        df["from_installation"] = from_installation if from_installation else KPLER_TOTAL
        df["to_installation"] = to_installation if to_installation else KPLER_TOTAL
        df["product"] = product if product else KPLER_TOTAL
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
            if split == FlowsSplit.DestinationCountries:
                # if df.split only contains "Total"
                if set(df.split) == {"Total"}:
                    df["destination_iso2"] = UNKNOWN_COUNTRY
                else:
                    df["destination_iso2"] = self.cc.pandas_convert(
                        series=df.split, to="ISO2", not_found=UNKNOWN_COUNTRY
                    )
            elif split == FlowsSplit.Products:
                df["product"] = df["split"]
            elif split == FlowsSplit.OriginInstallations:
                df["from_installation"] = df["split"]
            return df

        df = split_to_column(df, split)
        df = df.drop(columns=["split"])

        # Sometimes we have duplicated values
        df = (
            df.groupby(
                [
                    "date",
                    "origin_iso2",
                    "destination_iso2",
                    "from_installation",
                    "to_installation",
                    "product",
                    "unit",
                    "platform",
                ]
            )
            .sum()
            .reset_index()
        )

        return df

    def get_products(self, platform=None):
        platforms = self.platforms if platform is None else [platform]

        def get_platform_products(platform):
            if self.products.get(platform) is None:
                columns = ["id", "product_type", "family_name", "group_name", "product_name"]
                products = self.products_clients[platform].get(columns=columns)
                products.columns = columns
                products = products[products.product_type == "commodity"]
                products["platform"] = platform
                products.rename(
                    columns={
                        "product_type": "type",
                        "family_name": "family",
                        "group_name": "group",
                        "product_name": "name",
                    },
                    inplace=True,
                )
                self.products[platform] = products

            return self.products.get(platform)

        df = pd.concat([get_platform_products(platform) for platform in platforms])
        df = df[["id", "name", "family", "type", "group", "platform"]].drop_duplicates()
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


def update(
        date_from=None,
        date_to=None,
        platforms=None,
        products=None,
        origin_iso2s=["RU"],
        split_from_installation=True,
        add_total_installation=True,
        ignore_if_copy_failed=False,
        use_brute_force=False,
):
    scraper = KplerScraper()

    _platforms = scraper.platforms if platforms is None else platforms
    for platform in _platforms:
        _products = scraper.get_products(platform=platform).name if products is None else products
        for origin_iso2 in tqdm(origin_iso2s):
            print(origin_iso2)
            for product in _products:
                print(product)

                if split_from_installation:
                    installations = scraper.get_installations(
                        platform=platform, origin_iso2=origin_iso2, product=product
                    )
                    if add_total_installation:
                        installations = installations + [None]
                else:
                    installations = [None]

                for installation in tqdm(installations):
                    df = scraper.get_flows(
                        platform=platform,
                        origin_iso2=origin_iso2,
                        date_from=date_from,
                        date_to=date_to,
                        product=product,
                        from_installation=installation,
                        split=FlowsSplit.DestinationCountries,
                        use_brute_force=use_brute_force,
                    )
                    if df is not None:
                        try:
                            df.to_sql(
                                DB_TABLE_KPLER_FLOW,
                                con=engine,
                                if_exists="append",
                                index=False,
                            )
                        except sa.exc.IntegrityError:
                            if ignore_if_copy_failed:
                                logger.info("Some rows already exist. Skipping")
                            else:
                                logger.info("Some rows already exist. Upserting instead")
                                upsert(df, DB_TABLE_KPLER_FLOW, "unique_kpler_flow")
