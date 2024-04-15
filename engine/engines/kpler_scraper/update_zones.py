import pandas as pd
from engines.kpler_scraper.scraper import KplerScraper
from engines.kpler_scraper.upload import upload_zones

import ast


import country_converter as coco

cc = coco.CountryConverter()


def update_zones():
    scraper = KplerScraper()

    # A dataframe to store dfs of zones, each dataframe should contain:
    # - id
    # - name
    # - type
    # - port_id (if a port)
    # - port_name (if a port)
    # - country_id (optional)
    # - country_name (optional)
    # - country_iso2 (optional)
    collected_zones = []

    columns_we_care_about = [
        "id",
        "name",
        "type",
        "port_id",
        "port_name",
        "country_id",
        "country_name",
        "country_iso2",
    ]

    # This is a pandas dataframe which has the columns:
    # - name
    # - isPort
    # - isSupplyDemand
    # - geo
    # - continent
    # - export
    # - parentZones
    # - range
    # - subcontinent
    # - shape
    # - type
    # - import
    # - id
    # - isStorageSelected
    # - fullname
    all_zones = scraper.get_zones_brute()

    zones_we_care_about = [
        "anchorage",
        "bay",
        "canal",
        "checkpoint",
        "continent",
        "country",
        "country_checkpoint",
        "custom",
        "gulf",
        "gulf_checkpoint",
        "ocean",
        "port",
        "region",
        "sea",
        "storage",
        "strait",
        "subcontinent",
        "subregion",
    ]

    zones = all_zones[all_zones.type.isin(zones_we_care_about)].reset_index()

    zones["parentZones"] = zones.apply(parse_parent_zones, axis=1)
    zones = attach_country_info(zones)
    zones = attach_port_info(zones)

    collected_zones = collected_zones + [zones]

    zones_to_upload = pd.concat(collected_zones)[columns_we_care_about]

    zones_to_upload = zones_to_upload.drop_duplicates(subset=["id"])

    upload_zones(zones_to_upload)


def attach_country_info(zones):
    zones = zones.assign(
        country_id=zones.parentZones.apply(extract(["country", "country_checkpoint"], "id")),
        country_name=zones.parentZones.apply(extract(["country", "country_checkpoint"], "name")),
    )

    zones["country_id"] = zones.apply(
        lambda x: x["id"] if x["type"] in ["country", "country_checkpoint"] else x["country_id"],
        axis=1,
    )
    zones["country_name"] = zones.apply(
        lambda x: (
            x["name"] if x["type"] in ["country", "country_checkpoint"] else x["country_name"]
        ),
        axis=1,
    )

    zones["country_iso2"] = zones.apply(
        lambda x: cc.convert(x["country_name"], to="ISO2") if x["country_name"] else None, axis=1
    )
    return zones


def attach_port_info(zones):
    zones = zones.assign(
        port_id=zones.parentZones.apply(extract(["port"], "id")),
        port_name=zones.parentZones.apply(extract(["port"], "name")),
    )

    zones["port_id"] = zones.apply(
        lambda x: x["id"] if x["type"] == "port" else x["port_id"], axis=1
    )
    zones["port_name"] = zones.apply(
        lambda x: x["name"] if x["type"] == "port" else x["port_name"], axis=1
    )
    return zones


def parse_parent_zones(zone):
    """
    Extract the country information from the parentZones list
    :param parent_zones:
    :return:
    """

    dicts = ast.literal_eval(zone["parentZones"])
    return dicts


def extract(types, key):
    return lambda zones: next((x.get(key) for x in zones if x.get("type") in types), None)
