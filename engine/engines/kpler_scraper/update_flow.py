import datetime as dt
import logging
import os
import warnings

from base.utils import to_datetime
from base import UNKNOWN_COUNTRY
from base.db import session
from base.logger import logger, logger_slack
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from kpler.sdk import FlowsDirection, FlowsSplit, FlowsPeriod, FlowsMeasurementUnit

from . import KplerScraper
from . import KplerFlowScraper
from .upload import upload_flows


def update_flow_is_valid():
    # Read sql from 'update_is_valid.sql'
    with open(os.path.join(os.path.dirname(__file__), "update_is_valid.sql")) as f:
        sql = f.read()
    session.execute(sql)
    session.commit()
    return


def get_products(scraper, origin_iso2):
    df = scraper.get_flows_raw_brute(
        product=None,
        date_from="2010-01-01",
        date_to=dt.date.today(),
        from_zone=scraper.get_zone_dict(iso2=origin_iso2),
        split=FlowsSplit.Products,
        granularity=FlowsPeriod.Annually,
    )
    products_unique = list({v["id"]: v for v in df.split}.values())
    return products_unique


def get_from_zones(scraper, product, origin_iso2, split, to_zone=None):
    if split == FlowsSplit.OriginCountries and origin_iso2 is not None:
        return [scraper.get_zone_dict(iso2=origin_iso2)]

    df = scraper.get_flows_raw_brute(
        product=product,
        date_from="2010-01-01",
        date_to=dt.date.today(),
        from_zone=(scraper.get_zone_dict(iso2=origin_iso2) if origin_iso2 else None),
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
    scraper, product, split, from_zone=None, destination_iso2=None, include_unknown=True
):
    df = scraper.get_flows_raw_brute(
        product=product,
        date_from="2010-01-01",
        date_to=dt.date.today(),
        from_zone=from_zone,
        to_zone=scraper.get_zone_dict(iso2=destination_iso2),
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
    products=None,
    origin_iso2s=["RU"],
    destination_iso2s=None,
    from_splits=[FlowsSplit.OriginCountries, FlowsSplit.OriginPorts],
    to_splits=[FlowsSplit.DestinationCountries, FlowsSplit.DestinationPorts],
    # add_total_installation=True,
    add_unknown=True,
    add_unknown_only=False,
):
    scraper = KplerFlowScraper()
    date_from = to_datetime(date_from) if date_from is not None else to_datetime("2013-01-01")
    date_to = to_datetime(date_to) if date_to is not None else dt.date.today()

    with logging_redirect_tqdm(loggers=[logging.root]), warnings.catch_warnings():
        warnings.simplefilter("ignore")

        for origin_iso2 in tqdm(origin_iso2s, unit="origin", leave=False):
            logger.info(f"Updating flows for  {origin_iso2}")
            for from_split in tqdm(from_splits, unit="from-splits", leave=False):
                from_zones = get_from_zones(
                    scraper=scraper,
                    product=None,
                    origin_iso2=origin_iso2,
                    split=from_split,
                )

                for from_zone in tqdm(from_zones, unit="from-zone", leave=False):
                    for to_split in tqdm(to_splits, unit="to-splits", leave=False):
                        to_zones = get_to_zones(
                            scraper=scraper,
                            from_zone=from_zone,
                            split=to_split,
                            product=None,
                        )

                        df_zones = []
                        for to_zone in tqdm(to_zones, unit="to-zone", leave=False):
                            df = scraper.get_flows(
                                origin_iso2=origin_iso2,
                                date_from=date_from,
                                date_to=date_to,
                                from_zone=from_zone,
                                from_split=from_split,
                                to_zone=to_zone,
                                to_split=to_split,
                                split=FlowsSplit.Grades,
                            )
                            if df is not None:
                                df_zones.append(df)
                            if not add_unknown_only:
                                upload_flows(df)

                        if add_unknown:
                            # Add an unknown one
                            total = scraper.get_flows(
                                origin_iso2=origin_iso2,
                                date_from=date_from,
                                date_to=date_to,
                                from_zone=from_zone,
                                from_split=from_split,
                                to_zone=None,
                                to_split=to_split,
                                split=FlowsSplit.Grades,
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
                                        "No total flows found for %s | %s",
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
                                upload_flows(unknown)


def update_flows_reverse(
    date_from=None,
    date_to=None,
    destination_iso2s=["IN"],
    from_splits=[FlowsSplit.OriginCountries, FlowsSplit.OriginPorts],
    to_splits=[FlowsSplit.DestinationCountries, FlowsSplit.DestinationPorts],
    # add_total_installation=True,
    add_unknown=True,
    add_unknown_only=False,
):
    scraper = KplerScraper()
    date_from = to_datetime(date_from) if date_from is not None else to_datetime("2013-01-01")
    date_to = to_datetime(date_to) if date_to is not None else dt.date.today()

    for destination_iso2 in tqdm(destination_iso2s, unit="destination", leave=False):
        for to_split in tqdm(to_splits, unit="to-splits", leave=False):
            to_zones = get_to_zones(
                scraper=scraper,
                product=None,
                destination_iso2=destination_iso2,
                split=to_split,
                include_unknown=False,
            )

            for to_zone in tqdm(to_zones, unit="to-zone", leave=False):
                for from_split in tqdm(from_splits, unit="from-splits", leave=False):
                    from_zones = get_from_zones(
                        scraper=scraper,
                        to_zone=to_zone,
                        split=from_split,
                        product=None,
                        origin_iso2=None,
                    )

                    df_zones = []
                    for from_zone in tqdm(from_zones, unit="from-zone", leave=False):
                        df = scraper.get_flows(
                            destination_iso2=destination_iso2,
                            date_from=date_from,
                            date_to=date_to,
                            from_zone=from_zone,
                            from_split=from_split,
                            to_zone=to_zone,
                            to_split=to_split,
                            split=FlowsSplit.Products,
                        )
                        if df is not None:
                            df_zones.append(df)

                        if not add_unknown_only:
                            upload_flows(df)

                    if add_unknown and len(df_zones) > 0:
                        # Add an unknown one
                        total = scraper.get_flows(
                            destination_iso2=destination_iso2,
                            date_from=date_from,
                            date_to=date_to,
                            from_zone=None,
                            from_split=from_split,
                            to_zone=to_zone,
                            to_split=to_split,
                            split=FlowsSplit.Products,
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
                            unknown["value_unknown"] = unknown["value"] - unknown["value_byzone"]
                            unknown = unknown[unknown["value_unknown"] > 0]
                            unknown["to_zone_name"] = UNKNOWN_COUNTRY
                            unknown["value"] = unknown["value_unknown"]
                            unknown["updated_on"] = dt.datetime.now()
                            unknown = unknown[known_zones.columns]
                            upload_flows(unknown)
                        else:
                            raise ValueError("Total should not be None if we have data by zone")
