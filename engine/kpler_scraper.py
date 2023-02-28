import datetime as dt
import time
import requests
import json

import country_converter as coco
from base.env import get_env
from base.utils import to_datetime, to_list
from base import UNKNOWN_COUNTRY
from base.models import DB_TABLE_KPLER_PRODUCT, DB_TABLE_KPLER_FLOW
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

        # self.installations_clients = {
        #     # "liquids": Installations(self.configs["liquids"]),
        #     "lng": Installations(self.configs["lng"]),
        #     # "dry": Installations(self.configs["dry"]),
        # }

        self.cc = coco.CountryConverter()

        # Brute-force infos
        self.products_brute = {}
        self.zones_brute = {}

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

    def get_flows_raw(self, params, platform):
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

        return df

    def get_zones_brute(self, platform):
        if self.zones_brute.get(platform) is not None:
            return self.zones_brute[platform]

        # token = get_env("KPLER_TOKEN_BRUTE")
        # url = {
        #     "dry": "https://dry.kpler.com/api/zones",
        #     "liquids": "https://terminal.kpler.com/api/zones",
        # }.get(platform)
        # headers = {"Authorization": f"Bearer {token}"}
        # r = requests.get(url, headers=headers)
        # data = pd.DataFrame(r.json())
        # data.to_csv(f"assets/kpler/{platform}_zones.csv", index=False)
        data = pd.read_csv(f"assets/kpler/{platform}_zones.csv")
        self.zones_brute[platform] = data
        return data

    def get_products_brute(self, platform):
        if self.products_brute.get(platform) is not None:
            return self.products_brute[platform]

        # token = get_env("KPLER_TOKEN_BRUTE")
        # url = {
        #     "dry": "https://dry.kpler.com/api/products",
        #     "liquids": "https://terminal.kpler.com/api/products",
        # }.get(platform)
        # headers = {"Authorization": f"Basic {token}"}
        # r = requests.get(url, headers=headers)
        # data = pd.DataFrame(r.json())
        # data.to_csv(f"assets/kpler/{platform}_products.csv", index=False)
        data = pd.read_csv(f"assets/kpler/{platform}_products.csv")
        self.products_brute[platform] = data
        return data

    def get_flows_raw_brute(
        self,
        origin_iso2,
        destination_iso2,
        from_installation,
        to_installation,
        product,
        date_from,
        date_to,
        split,
        platform,
    ):
        """
        This one uses the token from the web interface,
        and another payload, that allows us to go back further than 1 year
        :param params:
        :param platform:
        :return:
        """
        products = self.get_products_brute(platform=platform)
        zones = self.get_zones_brute(platform=platform)

        # Get zone dict
        def get_zone_dict(iso2, installation):
            if installation is None:
                name = self.cc.convert(iso2, to="name_short")
                if iso2 == "RU":
                    name = "Russian Federation"
                type = "country"
            else:
                name = from_installation
                type = "port"

            id = zones[(zones["name"] == name) & (zones["type"] == type)]["id"].values[0]
            return {"id": int(id), "resourceType": "zone"}

        params_raw = {
            "cumulative": False,
            # "filters": {"product": [1334]},
            "flowDirection": "export",
            # "fromLocations": [{"id": 451, "resourceType": "zone"}],
            "granularity": "days",
            "interIntra": "interintra",
            "onlyRealized": True,
            "view": "kpler",
            "withBetaVessels": False,
            "withForecasted": True,
            "withGrades": False,
            "withIncompleteTrades": True,
            "withIntraCountry": False,
            "vesselClassifications": [],
            "withFreightView": False,
            "withProductEstimation": False,
            "splitOn": split.name,
            "startDate": to_datetime(date_from).strftime("%Y-%m-%d"),
            "endDate": to_datetime(date_to).strftime("%Y-%m-%d"),
            "numberOfSplits": 1000,
        }

        if product is not None:
            params_raw["filters"] = {
                "product": [products[products["Name"] == product]["id"].values[0]]
            }

        if from_installation is not None or origin_iso2 is not None:
            params_raw["fromLocations"] = [get_zone_dict(origin_iso2, from_installation)]

        if to_installation is not None or destination_iso2 is not None:
            params_raw["toLocations"] = [get_zone_dict(destination_iso2, to_installation)]

        unit = "t"
        token = get_env("KPLER_TOKEN_BRUTE")
        url = {
            "dry": "https://dry.kpler.com/api/flows",
            "liquids": "https://terminal.kpler.com/api/flows",
        }.get(platform)
        headers = {"Authorization": f"Basic {token}"}
        r = requests.post(url, json=params_raw, headers=headers)

        # read content to dataframe
        data = r.json()["series"]
        dfs = []
        for x in data:
            df = pd.concat(
                [pd.DataFrame(y["splitValues"]) for y in x["datasets"]], ignore_index=True
            )
            df = pd.concat([df.drop(["values"], axis=1), df["values"].apply(pd.Series)], axis=1)
            df["date"] = x["date"]
            df.drop(["id"], axis=1, inplace=True)
            dfs += [df]
            # Add total
            df_total = pd.DataFrame([y["values"] for y in x["datasets"]])
            df_total["date"] = x["date"]
            df_total["name"] = KPLER_TOTAL
            dfs += [df_total]

        df = pd.concat(dfs, ignore_index=True)
        df["unit"] = unit
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
        origin_country = unidecode(self.cc.convert(origin_iso2, to="name")) if origin_iso2 else None
        destination_country = (
            unidecode(self.cc.convert(destination_iso2, to="name")) if destination_iso2 else None
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

        # remove None values
        params = {k: v for k, v in params.items() if v is not None}
        if use_brute_force:
            df = self.get_flows_raw_brute(
                origin_iso2=origin_iso2,
                from_installation=from_installation,
                destination_iso2=destination_iso2,
                to_installation=to_installation,
                split=split,
                product=product,
                date_from=date_from,
                date_to=date_to,
                platform=platform,
            )
        else:
            df = self.get_flows_raw(params, platform)
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
        df = df.melt(
            id_vars=[
                "date",
                "origin_iso2",
                "destination_iso2",
                "from_installation",
                "to_installation",
                "product",
                "unit",
                "platform",
                "Period End Date",
            ],
            var_name="split",
            value_name="value",
        )

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
        df = df.drop(columns=["split", "Period End Date"])

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
            products = self.products_clients[platform].get(
                columns=["id", "family_name", "group_name", "product_name"]
            )
            products["platform"] = platform
            return products

        df = pd.concat([get_platform_products(platform) for platform in platforms])
        df = df.rename(
            columns={
                "Id (Product)": "id",
                "Family": "family",
                "Group": "group",
                "Product": "name",
            }
        )
        df = df[["name", "family", "group", "platform"]].drop_duplicates()
        df = df[~pd.isna(df.name)]
        return df


def fill_products():
    scraper = KplerScraper()
    products = scraper.get_products()
    upsert(products, DB_TABLE_KPLER_PRODUCT, "kpler_product_pkey")
    return


def update_flows(
    date_from=None,
    date_to=None,
    platforms=None,
    products=None,
    origin_iso2s=["RU"],
    split_from_installation=True,
    add_total_installation=True,
    ignore_if_copy_failed=False,
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

                for installation in installations:
                    df = scraper.get_flows(
                        platform=platform,
                        origin_iso2=origin_iso2,
                        date_from=date_from,
                        date_to=date_to,
                        product=product,
                        from_installation=installation,
                        split=FlowsSplit.DestinationCountries,
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
