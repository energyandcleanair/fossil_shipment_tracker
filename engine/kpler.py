import datetime as dt
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
from kpler.sdk import Platform
from kpler.sdk.resources.flows import Flows
from kpler.sdk.resources.products import Products
from kpler.sdk import FlowsDirection, FlowsSplit, FlowsPeriod, FlowsMeasurementUnit

KPLER_TOTAL = "Total"


class KplerScraper:
    def __init__(self):
        self.platforms = ["liquids", "lng", "dry"]

        self.configs = {
            "liquids": Configuration(
                Platform.Liquids, get_env("KPLER_EMAIL"), get_env("KPLER_PASSWORD")
            ),
            "lng": Configuration(
                Platform.LNG, get_env("KPLER_EMAIL"), get_env("KPLER_PASSWORD")
            ),
            "dry": Configuration(
                Platform.Dry, get_env("KPLER_EMAIL"), get_env("KPLER_PASSWORD")
            ),
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

    def get_installation(self, origin_iso2, product):
        return

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
    ):
        origin_country = (
            unidecode(self.cc.convert(origin_iso2, to="name")) if origin_iso2 else None
        )
        destination_country = (
            unidecode(self.cc.convert(destination_iso2, to="name"))
            if destination_iso2
            else None
        )

        params = {
            "from_zones": [origin_country],
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
        df = self.flows_clients[platform].get(**params)

        # Ideally no NULL otherwise the unique constraints won't work
        # This should work from Postgres 15 onwards
        df["origin_iso2"] = origin_iso2 if origin_iso2 else KPLER_TOTAL
        df["destination_iso2"] = destination_iso2 if destination_iso2 else KPLER_TOTAL
        df["from_installation"] = (
            from_installation if from_installation else KPLER_TOTAL
        )
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
            return df

        df = split_to_column(df, split)
        df = df.drop(columns=["split", "Period End Date"])
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


def update_flows(date_from=None, date_to=None, origin_iso2s=["RU"]):
    scraper = KplerScraper()

    for platform in scraper.platforms:
        products = scraper.get_products(platform=platform).name
        for origin_iso2 in tqdm(origin_iso2s):
            for product in products:
                print(product)
                df = scraper.get_flows(
                    platform=platform,
                    origin_iso2=origin_iso2,
                    date_from=date_from,
                    date_to=date_to,
                    product=product,
                    split=FlowsSplit.DestinationCountries,
                )
                try:
                    df.to_sql(
                        DB_TABLE_KPLER_FLOW, con=engine, if_exists="append", index=False
                    )
                except sa.exc.IntegrityError:
                    logger.info("Cannot copy. Upserting instead")
                    upsert(df, DB_TABLE_KPLER_FLOW, "unique_kpler_flow")
