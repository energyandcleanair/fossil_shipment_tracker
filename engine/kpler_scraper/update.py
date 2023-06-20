import datetime as dt
import json
import os
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


def update_is_valid():
    # Read sql from 'update_is_valid.sql'
    with open(os.path.join(os.path.dirname(__file__), "update_is_valid.sql")) as f:
        sql = f.read()
    session.execute(sql)
    session.commit()
    return


def upload_flows(df, ignore_if_copy_failed=False):

    if df is None or len(df) == 0:
        return None

    df["updated_on"] = dt.datetime.now()

    if "destination_country" in df.columns:
        df.drop(columns=["destination_country"], inplace=True)

    if len(df) > 0:
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


def get_products(scraper, platform, origin_iso2):
    df = scraper.get_flows_raw_brute(
        platform=platform,
        product=None,
        date_from="2010-01-01",
        date_to=dt.date.today(),
        from_zone=scraper.get_zone_dict(platform=platform, iso2=origin_iso2),
        split=FlowsSplit.Products,
        granularity=FlowsPeriod.Annually,
    )
    products_unique = list({v["id"]: v for v in df.split}.values())
    return products_unique


def get_from_zones(scraper, platform, product, origin_iso2, split, to_zone=None):

    if split == FlowsSplit.OriginCountries and origin_iso2 is not None:
        return [scraper.get_zone_dict(platform=platform, iso2=origin_iso2)]

    df = scraper.get_flows_raw_brute(
        platform=platform,
        product=product,
        date_from="2010-01-01",
        date_to=dt.date.today(),
        from_zone=scraper.get_zone_dict(platform=platform, iso2=origin_iso2)
        if origin_iso2
        else None,
        to_zone=to_zone,
        split=split,
        granularity=FlowsPeriod.Annually,
    )
    if df is None:
        return []
    else:
        # dict unashable. Use a trick to get unique values
        zones_unique = list({v["id"]: v for v in df.split}.values())
        return zones_unique


def get_to_zones(
    scraper, platform, product, split, from_zone=None, destination_iso2=None, include_unknown=True
):

    df = scraper.get_flows_raw_brute(
        platform=platform,
        product=product,
        date_from="2010-01-01",
        date_to=dt.date.today(),
        from_zone=from_zone,
        to_zone=scraper.get_zone_dict(platform=platform, iso2=destination_iso2),
        split=split,
        granularity=FlowsPeriod.Annually,
    )
    if df is None:
        return []
    else:
        # dict unashable. Use a trick to get unique values
        zones_unique = list({v["id"]: v for v in df.split}.values())
        if not include_unknown:
            zones_unique = [x for x in zones_unique if x["name"].lower() != UNKNOWN_COUNTRY.lower()]
        return zones_unique


def update_flows(
    date_from=None,
    date_to=None,
    platforms=None,
    products=None,
    origin_iso2s=["RU"],
    destination_iso2s=None,
    from_splits=[FlowsSplit.OriginCountries, FlowsSplit.OriginPorts],
    to_splits=[FlowsSplit.DestinationCountries, FlowsSplit.DestinationPorts],
    # add_total_installation=True,
    ignore_if_copy_failed=False,
    use_brute_force=True,
    add_unknown=True,
    add_unknown_only=False,
):
    scraper = KplerScraper()
    date_from = to_datetime(date_from) if date_from is not None else to_datetime("2013-01-01")
    date_to = to_datetime(date_to) if date_to is not None else dt.date.today()

    _platforms = scraper.platforms if platforms is None else platforms
    for platform in _platforms:
        # _products = scraper.get_products(platform=platform).name if products is None else products

        for origin_iso2 in tqdm(origin_iso2s):
            print(origin_iso2)
            for from_split in from_splits:

                from_zones = get_from_zones(
                    scraper=scraper,
                    platform=platform,
                    product=None,
                    origin_iso2=origin_iso2,
                    split=from_split,
                )

                for from_zone in from_zones:

                    for to_split in to_splits:

                        to_zones = get_to_zones(
                            scraper=scraper,
                            platform=platform,
                            from_zone=from_zone,
                            split=to_split,
                            product=None,
                        )

                        df_zones = []
                        for to_zone in to_zones:

                            df = scraper.get_flows(
                                platform=platform,
                                origin_iso2=origin_iso2,
                                date_from=date_from,
                                date_to=date_to,
                                from_zone=from_zone,
                                from_split=from_split,
                                to_zone=to_zone,
                                to_split=to_split,
                                split=FlowsSplit.Grades,
                                use_brute_force=use_brute_force,
                            )
                            if df is not None:
                                df_zones.append(df)
                            if not add_unknown_only:
                                upload_flows(df, ignore_if_copy_failed=ignore_if_copy_failed)

                        if add_unknown:
                            # Add an unknown one
                            total = scraper.get_flows(
                                platform=platform,
                                origin_iso2=origin_iso2,
                                date_from=date_from,
                                date_to=date_to,
                                from_zone=from_zone,
                                from_split=from_split,
                                to_zone=None,
                                to_split=to_split,
                                split=FlowsSplit.Products,
                                use_brute_force=use_brute_force,
                            )

                            if len(df_zones) == 0:
                                logger.warning("No flows found for %s", from_zone)
                            else:
                                known_zones = pd.concat(df_zones)
                                known_zones_total = (
                                    known_zones.groupby(["date", "product"])
                                    .value.sum()
                                    .reset_index()
                                )
                                if total is None:
                                    raise ValueError(
                                        "No total flows found for %s |%s | %s",
                                        platform,
                                        origin_iso2,
                                        from_zone,
                                    )

                                unknown = total.merge(
                                    known_zones_total,
                                    on=["product", "date"],
                                    how="left",
                                    suffixes=("", "_byzone"),
                                )
                                unknown["value_byzone"] = unknown["value_byzone"].fillna(0)
                                unknown["value_unknown"] = (
                                    unknown["value"] - unknown["value_byzone"]
                                )
                                unknown = unknown[unknown["value_unknown"] > 0]
                                unknown["to_zone_name"] = UNKNOWN_COUNTRY
                                unknown["value"] = unknown["value_unknown"]
                                unknown["updated_on"] = dt.datetime.now()
                                unknown = unknown[known_zones.columns]
                                upload_flows(unknown, ignore_if_copy_failed=ignore_if_copy_failed)


def update_flows_reverse(
    date_from=None,
    date_to=None,
    platforms=None,
    destination_iso2s=["IN"],
    from_splits=[FlowsSplit.OriginCountries, FlowsSplit.OriginPorts],
    to_splits=[FlowsSplit.DestinationCountries, FlowsSplit.DestinationPorts],
    # add_total_installation=True,
    ignore_if_copy_failed=False,
    use_brute_force=True,
    add_unknown=True,
    add_unknown_only=False,
):
    scraper = KplerScraper()
    date_from = to_datetime(date_from) if date_from is not None else to_datetime("2013-01-01")
    date_to = to_datetime(date_to) if date_to is not None else dt.date.today()

    _platforms = scraper.platforms if platforms is None else platforms
    for platform in _platforms:
        # _products = scraper.get_products(platform=platform).name if products is None else products

        for destination_iso2 in tqdm(destination_iso2s):

            for to_split in to_splits:

                to_zones = get_to_zones(
                    scraper=scraper,
                    platform=platform,
                    product=None,
                    destination_iso2=destination_iso2,
                    split=to_split,
                    include_unknown=False,
                )

                for to_zone in tqdm(to_zones):

                    for from_split in from_splits:

                        from_zones = get_from_zones(
                            scraper=scraper,
                            platform=platform,
                            to_zone=to_zone,
                            split=from_split,
                            product=None,
                            origin_iso2=None,
                        )

                        df_zones = []
                        for from_zone in tqdm(from_zones):

                            df = scraper.get_flows(
                                platform=platform,
                                destination_iso2=destination_iso2,
                                date_from=date_from,
                                date_to=date_to,
                                from_zone=from_zone,
                                from_split=from_split,
                                to_zone=to_zone,
                                to_split=to_split,
                                split=FlowsSplit.Products,
                                use_brute_force=use_brute_force,
                            )
                            if df is not None:
                                df_zones.append(df)

                            if not add_unknown_only:
                                upload_flows(df, ignore_if_copy_failed=ignore_if_copy_failed)

                        if add_unknown and len(df_zones) > 0:
                            # Add an unknown one
                            total = scraper.get_flows(
                                platform=platform,
                                destination_iso2=destination_iso2,
                                date_from=date_from,
                                date_to=date_to,
                                from_zone=None,
                                from_split=from_split,
                                to_zone=to_zone,
                                to_split=to_split,
                                split=FlowsSplit.Products,
                                use_brute_force=use_brute_force,
                            )

                            known_zones = pd.concat(df_zones)
                            known_zones_total = (
                                known_zones.groupby(["date", "product"]).value.sum().reset_index()
                            )
                            if total is not None:
                                unknown = total.merge(
                                    known_zones_total,
                                    on=["product", "date"],
                                    how="left",
                                    suffixes=("", "_byzone"),
                                )
                                unknown["value_byzone"] = unknown["value_byzone"].fillna(0)
                                unknown["value_unknown"] = (
                                    unknown["value"] - unknown["value_byzone"]
                                )
                                unknown = unknown[unknown["value_unknown"] > 0]
                                unknown["to_zone_name"] = UNKNOWN_COUNTRY
                                unknown["value"] = unknown["value_unknown"]
                                unknown["updated_on"] = dt.datetime.now()
                                unknown = unknown[known_zones.columns]
                                upload_flows(unknown, ignore_if_copy_failed=ignore_if_copy_failed)
                            else:
                                raise ValueError("Total should not be None if we have data by zone")


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
                upsert(trades, DB_TABLE_KPLER_TRADE, "unique_kpler_flow")


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
