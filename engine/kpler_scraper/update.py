import datetime as dt
import json
from base.utils import to_datetime, to_list
from base import UNKNOWN_COUNTRY
from base.models import (
    DB_TABLE_KPLER_PRODUCT,
    DB_TABLE_KPLER_FLOW,
    KplerVessel,
    DB_TABLE_KPLER_TRADE,
)
from base.db_utils import upsert
from base.db import session, engine
from base.logger import logger
import pandas as pd
from tqdm import tqdm
import sqlalchemy as sa
from kpler.sdk import FlowsDirection, FlowsSplit, FlowsPeriod, FlowsMeasurementUnit

from . import KplerScraper


def upload_flows(df, ignore_if_copy_failed=False):

    if df is None or len(df) == 0:
        return None

    if "destination_country" in df.columns:
        df.drop(columns=["destination_country"], inplace=True)

    if len(df) > 0:
        try:
            df.to_sql(
                DB_TABLE_KPLER_FLOW + "2",
                con=engine,
                if_exists="append",
                index=False,
            )
        except sa.exc.IntegrityError:
            if ignore_if_copy_failed:
                logger.info("Some rows already exist. Skipping")
            else:
                logger.info("Some rows already exist. Upserting instead")
                upsert(df, DB_TABLE_KPLER_FLOW + "2", "unique_kpler_flow2")


def get_from_zones(scraper, platform, product, origin_iso2, split):

    if split == FlowsSplit.OriginCountries:
        return [scraper.get_zone_dict(platform=platform, iso2=origin_iso2)]

    df = scraper.get_flows_raw_brute(
        platform=platform,
        product=product,
        date_from="2010-01-01",
        date_to=dt.date.today(),
        from_zone=scraper.get_zone_dict(platform=platform, iso2=origin_iso2),
        split=split,
        granularity=FlowsPeriod.Annually,
    )
    if df is None:
        return []
    else:
        # dict unashable. Use a trick to get unique values
        zones_unique = list({v["id"]: v for v in df.split}.values())
        return zones_unique


def get_to_zones(scraper, platform, product, origin_iso2, split):
    if split == FlowsSplit.DestinationCountries:
        return [None]

    df = scraper.get_flows_raw(
        platform=platform, product=product, origin_iso2=origin_iso2, split=split
    )
    zones = df.split.unique()
    return zones


def update_flows(
    date_from=None,
    date_to=None,
    platforms=None,
    products=None,
    origin_iso2s=["RU"],
    split_from_installation=[FlowsSplit.OriginCountries, FlowsSplit.OriginPorts],
    split_to_installation=[FlowsSplit.DestinationCountries, FlowsSplit.DestinationPorts],
    # add_total_installation=True,
    ignore_if_copy_failed=False,
    use_brute_force=False,
):
    scraper = KplerScraper()

    _platforms = scraper.platforms if platforms is None else platforms
    for platform in _platforms:
        _products = scraper.get_products(platform=platform).name if products is None else products

        for origin_iso2 in tqdm(origin_iso2s):

            for product in tqdm(_products):

                for from_split in split_from_installation:

                    from_zones = get_from_zones(
                        scraper=scraper,
                        platform=platform,
                        product=product,
                        origin_iso2=origin_iso2,
                        split=from_split,
                    )

                    for from_zone in from_zones:

                        for to_split in split_to_installation:

                            df = scraper.get_flows(
                                platform=platform,
                                origin_iso2=origin_iso2,
                                date_from=date_from,
                                date_to=date_to,
                                product=product,
                                from_zone=from_zone,
                                from_split=from_split,
                                to_split=to_split,
                                use_brute_force=use_brute_force,
                            )
                            upload_flows(df, ignore_if_copy_failed=ignore_if_copy_failed)


def upload_trades(trades, ignore_if_copy_failed=False):
    if trades is not None:
        try:
            trades["others"] = trades.others.apply(json.dumps)
            trades = trades[~pd.isnull(trades.product_id)]
            trades.to_sql(
                DB_TABLE_KPLER_TRADE,
                con=engine,
                if_exists="append",
                index=False,
            )
        except sa.exc.IntegrityError:
            if ignore_if_copy_failed:
                logger.info("Some rows already exist. Skipping")
            else:
                logger.info("Some rows already exist. Upserting instead")
                upsert(trades, DB_TABLE_KPLER_TRADE, "kpler_trade_pkey")


def update_trades(
    date_from=None,
    platforms=None,
    origin_iso2s=["RU"],
    ignore_if_copy_failed=False,
):
    scraper = KplerScraper()
    date_from = date_from or dt.date(2015, 1, 1)
    _platforms = scraper.platforms if platforms is None else platforms
    for platform in _platforms:
        print(platform)
        for origin_iso2 in tqdm(origin_iso2s):
            print(origin_iso2)
            cursor_after = None
            while True:
                cursor_after, trades = scraper.get_trades_raw_brute(
                    platform=platform, origin_iso2=origin_iso2, cursor_after=cursor_after
                )
                upload_trades(trades, ignore_if_copy_failed=ignore_if_copy_failed)
                print(trades.departure_date.min())
                if (
                    cursor_after is None
                    or len(trades) == 0
                    or trades.departure_date.min() < to_datetime(date_from)
                ):
                    break


def update_zones(platforms=None):
    scraper = KplerScraper()
    platforms = scraper.platforms if platforms is None else platforms
    for platform in platforms:
        zones = scraper.get_zones_brute(platform=platform)
        import ast

        import_installation = pd.concat(
            [pd.DataFrame(ast.literal_eval(x).get("installations")) for x in zones["import"]]
        )
        export_installation = pd.concat(
            [pd.DataFrame(ast.literal_eval(x).get("installations")) for x in zones["export"]]
        )

        def parent_zones_to_zones_df(parent_zones):
            """
            Cast a list of dicts to a dataframe, adding the country information
            that is found in one of them
            :param parent_zones:
            :return:
            """
            dicts = ast.literal_eval(parent_zones)
            df = pd.DataFrame([x for x in dicts if x.get("resourceType") == "zone"])
            country = next((x.get("name") for x in dicts if x.get("type") == "country"), None)
            df["country"] = country
            return df

        ports = pd.concat([parent_zones_to_zones_df(x) for x in zones.parentZones])
        ports = (
            ports[["id", "name", "country"]]
            .rename(columns={"id": "port_id", "name": "port_name", "country": "port_country"})
            .drop_duplicates()
        )

        import_installation["port_id"] = import_installation.port.apply(lambda x: x.get("id"))
        a = import_installation.merge(ports, on="port_id", how="left")
        assert len(import_installation) == len(a)
        assert pd.isna(a.port_country).sum() == 0
        b = a[(a.country != a.port_country) & ~pd.isna(a.country)]

        len(import_installation.id.unique())
        import_installation.columns
        export_installation.columns