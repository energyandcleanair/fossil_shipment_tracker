import numpy as np
import pandas as pd
from requests import session
from engines.kpler_scraper.scraper import KplerScraper
from engines.kpler_scraper.upload import update_zone_areas, upload_zones

import ast


import country_converter as coco

from base.utils import latlon_to_point
from base.models.kpler import KplerZone

cc = coco.CountryConverter()


def update_zones():
    scraper = KplerScraper()

    columns_we_care_about = [
        "id",
        "name",
        "type",
        "port_id",
        "port_name",
        "country_id",
        "country_name",
        "country_iso2",
        "geometry",
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
    zones = attach_geo_info(zones)

    collected_zones = [zones]

    zones_to_upload = pd.concat(collected_zones)[columns_we_care_about]
    zones_to_upload = zones_to_upload.drop_duplicates(subset=["id"])

    upload_zones(zones_to_upload)
    update_zone_areas()


def attach_geo_info(zones):
    zones["geo"] = zones["geo"].replace({np.nan: None})
    zones["geo"] = zones.apply(
        lambda zone: ast.literal_eval(zone["geo"]) if zone["geo"] is not None else None, axis=1
    )
    zones["geo"] = zones["geo"].apply(
        lambda geom: latlon_to_point(lat=geom["lat"], lon=geom["lon"]) if geom is not None else None
    )
    zones["geometry"] = zones["geo"]
    return zones


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
