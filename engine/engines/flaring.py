# Author: Hubert Thieriot (hubert@energyandcleanair.org)
# Copyright: Centre for Research on Energy and Clean Air (@CREA)
#
# Purpose: This file estimates daily gas flaring around Russian gas & oil fields,
# using the method described in this publication:
# Zhizhin M, Matveev A, Ghosh T, Hsu F-C, Howells M, Elvidge C. Measuring Gas Flaring in Russia
# with Multispectral VIIRS Nightfire. Remote Sensing. 2021; 13(16):3078.
# https://doi.org/10.3390/rs13163078
#
# To do so, we leverage VIIRS Nightfire (VNF) data,
# which is available from https://eogdata.mines.edu/products/vnf/
#
# This file includes:
# - Scraping of facilities (oil/gas fields, pipelines, terminals, processing plants)
# - Collecting of VIIRS Night Fire data
# - Computations of flaring
# - Upload of flaring data to database
#
# Results can be browsed at https://crea.shinyapps.io/russia_flaring/
#
# License: MIT
#

import os
import numpy as np
import pandas as pd
import geopandas as gpd
import itertools
import shapely
import json
import requests
import gzip
import shutil

from tqdm import tqdm
from geoalchemy2 import Geometry
from sqlalchemy import func

from base.env import get_env
from base.db import session
from base.db_utils import upsert
from base.logger import logger, logger_slack
from base.models import DB_TABLE_FLARING_FACILITY, DB_TABLE_FLARING
from base.models import FlaringFacility, Flaring
from base.utils import to_datetime
from base.utils import update_geometry_from_wkb


def update(date_from="2015-01-01", date_to=-2, missing_dates_only=True, fill_facilities=False):
    """
    Overarching function
    Update flaring time series in database
    :param date_from: (if integer, time interval in days from today)
    :param date_to: (if integer, time interval in days from today)
    :param missing_dates_only: only collect flaring data since the last stored date.
    :param fill_facilities: whether to recollect and upload facilities
    :return:
    """

    logger_slack.info("=== Flaring update ===")

    # Get facilities and fill if need be
    facilities = FacilityScraper.download_facilities()
    if facilities.empty or fill_facilities:
        FacilityScraper.fill_facilities()
        facilities = FacilityScraper.download_facilities()

    if missing_dates_only:
        date_from = FlaringComputer.get_last_flaring_date() or date_from

    # Compute flaring
    flaring = FlaringComputer.get_flaring_ts(
        facilities=facilities,
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    # Upload flaring
    FlaringComputer.upload_flaring(flaring=flaring)
    return


class FlaringComputer:
    """
    Compute flaring amounts from vnf (VIIRS Nightfire) using this publication method:

    Zhizhin M, Matveev A, Ghosh T, Hsu F-C, Howells M, Elvidge C. Measuring Gas Flaring in Russia
    with Multispectral VIIRS Nightfire. Remote Sensing. 2021; 13(16):3078.
    https://doi.org/10.3390/rs13163078
    """

    # Constants from publication
    sigma = 5.67e-8
    b1 = 0.0294
    d = 0.7
    min_temp_k = 1200
    max_temp_k = 999999

    @classmethod
    def get_flaring_ts(
        cls,
        facilities,
        date_from="2018-01-01",
        date_to=-3,
        buffer_km_fields=10,
        buffer_km_infra=5,
    ):
        """
        Compute flaring time series for a given set of facilities
        :param facilities: dataframe of fields / pipelines etc. with a geometry column
        :param date_from: (if integer, time interval in days from today)
        :param date_to: (if integer, time interval in days from today)
        :param buffer_km_fields:
        :param buffer_km_infra:
        :return:
        """
        # Get geomtries, buffer, dissolve
        geometries = cls.get_geometries(
            facilities=facilities,
            buffer_km_fields=buffer_km_fields,
            buffer_km_infra=buffer_km_infra,
        )
        # Get flaring amount
        dates = pd.date_range(to_datetime(date_from), to_datetime(date_to))
        res = []
        pbar = tqdm(dates)

        for date in pbar:
            pbar.set_description("Processing %s" % date)
            res.append(cls.get_flaring_amount(date=date, geometries=geometries))

        res = [x for x in res if x is not None]
        if not res:
            return []

        res = pd.concat(res).groupby(["id", "date", "unit"])[["value", "count"]].sum().reset_index()

        # Adding buffer info
        res = res.merge(geometries[["id", "buffer_km"]])

        # Format for table
        res = res[["id", "date", "unit", "value", "buffer_km"]].rename(
            columns={"id": "facility_id"}
        )
        return res

    @classmethod
    def get_geometries(cls, facilities, buffer_km_fields, buffer_km_infra):
        buffered_fields = cls.buffer(
            df=facilities[facilities.type == "Field"],
            buffer_km=buffer_km_fields,
            add_field=True,
        )

        buffered_infra = cls.buffer(
            facilities[facilities.type != "Field"],
            buffer_km=buffer_km_infra,
            add_field=True,
        )

        geometries = pd.concat([buffered_fields, buffered_infra], ignore_index=True)
        return geometries

    @classmethod
    def get_flaring_amount(cls, date, geometries):
        """
        The computing function itself.
        :param date:
        :param geometries: buffered field geometries
        :return: DataFrame
        """

        fires = VnfScraper.get_vnf(date=date)
        if fires is None:
            return None

        fires = cls.keep_relevant(fires=fires)
        flares = cls.compute_flares(fires=fires)
        joined = cls.spatial_join(flares=flares, geometries=geometries)
        result = cls.aggregate(joined=joined)
        result = cls.complete(result=result, geometries=geometries, date=date)
        return result

    @classmethod
    def keep_relevant(cls, fires):
        """
        Only keep relevant detected fires
        :param fires:
        :return:
        """
        fires = fires[(fires.Temp_BB > cls.min_temp_k) & (fires.Temp_BB < cls.max_temp_k)]
        return fires

    @classmethod
    def compute_flares(cls, fires, units=["index", "mw"]):
        """
        The flaring estimation function itself.
        :param fires: dataframe
        :param units: what units to return
        :return:
        """

        flares = []

        if "index" in units:
            # Flares in index, proportional to BCM
            # TODO validate unit vs BCM
            # RH’=σT^4S^d
            flares_index = fires.copy()
            flares_index["rhp"] = (
                cls.sigma
                * np.power(flares_index.Temp_BB, 4)
                * np.power(flares_index.Area_BB, cls.d)
            )
            flares_index["value"] = cls.b1 * flares_index.rhp
            flares_index["unit"] = "index"
            flares.append(flares_index)

        if "mw" in units:
            # Flares in MW
            # Simply summing MW of detected fires
            flares_mw = fires.copy()
            flares_mw["value"] = flares_mw.RH
            flares_mw["unit"] = "mw"
            flares.append(flares_mw)

        flares = pd.concat(flares, ignore_index=True)

        # Format
        columns = {
            "Date_LTZ": "date",
            "Lon_GMTCO": "lon",
            "Lat_GMTCO": "lat",
            "value": "value",
            "unit": "unit",
        }

        flares = flares[columns.keys()].rename(columns=columns)
        return flares

    @classmethod
    def spatial_join(cls, flares, geometries):
        """
        Only keep flares overlapping with geometries
        :param flares:
        :param geometries:
        :return:
        """
        flares_gdf = gpd.GeoDataFrame(flares, geometry=gpd.points_from_xy(flares.lon, flares.lat))
        flares_gdf.crs = 4326
        flares_gdf["date"] = pd.to_datetime(flares_gdf.date).dt.floor("D")
        joined = gpd.GeoDataFrame(geometries[["id", "type", "geometry"]]).sjoin(
            flares_gdf, how="left"
        )
        return joined

    @classmethod
    def aggregate(cls, joined):
        """
        Aggregate (sum and count) flares by geometry
        :param joined:
        :return:
        """

        def count_non_na(x):
            return sum(~np.isnan(x))

        result = (
            joined.groupby(["id", "date", "unit"])
            .agg(value=("value", np.nansum), count=("value", count_non_na))
            .reset_index()
        )
        return result

    @classmethod
    def complete(cls, result, geometries, date):
        """
        Complete result with missing values
        """
        ids = geometries.id.unique()
        units = ["mw", "index"]
        combined = [ids, units]
        merger = pd.DataFrame(columns=["id", "unit"], data=list(itertools.product(*combined)))
        result = merger.merge(result, how="left")
        result["date"] = result.date.fillna(date)
        result["value"] = result.value.fillna(0)
        return result

    @staticmethod
    def buffer(df, buffer_km, add_field):
        """
        Buffer a geometry (point, line) by n km
        :param df: dataframe with a geometry column in EPSG:4326
        :param buffer_km:
        :return:
        """
        df = gpd.GeoDataFrame(df)
        df.crs = 4326
        df = df.to_crs(3857)
        df["geometry"] = df["geometry"].buffer(buffer_km * 1000)
        df = df.to_crs(4326)
        if add_field:
            df["buffer_km"] = buffer_km
        return df

    @classmethod
    def get_last_flaring_date(cls):
        """
        Get last flaring date in our database
        :return:
        """
        return session.query(func.max(Flaring.date)).first()[0]

    @classmethod
    def upload_flaring(cls, flaring):
        upsert(df=flaring, table=DB_TABLE_FLARING, constraint_name="unique_flaring")


class FacilityScraper:
    """
    Class to collect flaring facilities (fields, pipelines, terminals etc)
    from various sites
    """

    @classmethod
    def fill_facilities(cls):
        """
        Used to fill infrastructure the first time.
        Will overwrite existing facilities.
        :return:
        """
        facilities = cls.get_facilities()
        upsert(
            df=pd.DataFrame(facilities),
            table=DB_TABLE_FLARING_FACILITY,
            constraint_name="flaring_facility_pkey",
            dtype={"geometry": Geometry("GEOMETRY", 4326)},
        )

    @classmethod
    def download_facilities(cls):
        """
        Download facilities from our own database
        :return:
        """
        facilities = pd.read_sql(session.query(FlaringFacility).statement, session.bind)
        facilities = update_geometry_from_wkb(facilities, to="shape")
        return facilities

    @classmethod
    def get_facilities(cls):
        """
        Collect and combine oil/gas fields, pipelines, terminals
        :return:
        """
        fields = cls.get_fields(gas_only=True)
        infrastructure = cls.get_infrastructure()
        cols = ["name", "type", "geometry", "url"]
        facilities = gpd.GeoDataFrame(
            pd.concat(
                [
                    fields[cols],
                    infrastructure[cols],
                ]
            )
        )
        facilities = facilities.dissolve(by="name").reset_index()
        facilities["id"] = np.arange(len(facilities))

        # Add English name when avaialble
        english_lookup = pd.read_csv("assets/flaring/english_lookup.csv")
        facilities = facilities.merge(english_lookup[["name", "name_en"]], on="name")
        facilities["name_en"] = facilities.name_en.combine_first(facilities.name)

        # Convert to GeoDataFrame
        facilities = update_geometry_from_wkb(facilities, to="wkt")
        facilities = gpd.GeoDataFrame(facilities)
        return facilities

    @classmethod
    def get_fields(cls, gas_only=True):
        """
        Collect gas (and oil) fields from energybase.ru
        :param gas_only: whether to only keep gas or include oil as well
        :return:
        """

        # 403: need to download the file manually
        # url = "https://api.energybase.ru/v1/map-feature/field"
        file_path = "assets/flaring/russia_oil_gas_fields.json"
        fields = gpd.read_file(file_path)
        fields["geometry"] = fields.geometry.map(
            lambda pt: shapely.ops.transform(lambda x, y: (y, x), pt)
        )

        pattern = r"(https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}[-a-zA-Z0-9()@:%_+.~#?&/=]*)"
        fields["url"] = (
            fields["balloonContentHeader"].str.extract(pattern, expand=False).str.strip()
        )

        if gas_only:
            with open(file_path, "r") as file:
                data = json.load(file)

            is_gas = ["gas.svg" in x["options"]["iconImageHref"] for x in data["features"]]
            fields = fields.iloc[np.where(is_gas)]

        fields["type"] = "Field"
        fields["name"] = fields.clusterCaption
        return fields

    @classmethod
    def get_infrastructure(cls):
        url = "https://greeninfo-network.github.io/global-gas-infrastructure-tracker/data/data.csv?v=2.1"
        infra = pd.read_csv(url)
        infra = infra[infra.countries.str.contains("Russia")]

        lines = infra[(infra.geom == "line") & (infra.status == "operating")].copy()
        points = infra[
            (infra.geom == "point")
            & ((infra.status == "operating") | infra.project.str.contains("Portovaya"))
        ].copy()

        def route_to_linestring(route):
            # multiline = infra.route.str.contains(';')
            # if multiline:
            lines = route.split(";")
            segments = [x.split(":") for x in lines]
            coords = [[[float(z) for z in y.split(",")] for y in x] for x in segments]
            return shapely.geometry.MultiLineString(coords)

        lines["geometry"] = lines.route.apply(route_to_linestring)
        lines["geometry"] = lines.geometry.map(
            lambda pt: shapely.ops.transform(lambda x, y: (y, x), pt)
        )

        points["geometry"] = gpd.points_from_xy(points.lng, points.lat)
        points["type"] = "LNG Terminal"
        lines["type"] = "Pipeline"
        return pd.concat([points, lines]).rename(columns={"project": "name"})


class VnfScraper:
    """
    Class to collect VIIRS Night Fire data
    from the Earth Observation Group at Mines School.
    """

    @classmethod
    def download_vnf_date(cls, date, force=False):
        """
        Download VNF file from Mines Earth Observation Group
        :param date:
        :param force:
        :return: file path if successful, None otherwise
        """
        date = to_datetime(date)
        output_file = cls.date_to_localpath(date, ext="csv")
        output_file_gz = cls.date_to_localpath(date, ext="csv.gz")
        if os.path.exists(output_file) and not force:
            return output_file

        params = {
            "client_id": "eogdata_oidc",
            "client_secret": get_env("VNF_MINES_SECRET"),
            "username": get_env("VNF_MINES_EMAIL"),
            "password": get_env("VNF_MINES_PASSWORD"),
            "grant_type": "password",
        }
        token_url = "https://eogauth.mines.edu/auth/realms/master/protocol/openid-connect/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(token_url, data=params, headers=headers)
        if response.status_code != 200:
            logger.warning("VNF: Failed to query VNF for %s" % (date,))
            return None

        access_token_list = response.json()
        access_token = access_token_list["access_token"]

        # Submit request with token bearer and write to output file
        data_url = (
            "https://eogdata.mines.edu/wwwdata/viirs_products/vnf/v30//VNF_npp_d%s_noaa_v30-ez.csv.gz"
            % (date.strftime("%Y%m%d"),)
        )
        auth = "Bearer %s" % (access_token,)
        r = requests.get(data_url, allow_redirects=True, headers={"Authorization": auth})
        if r.status_code != 200:
            return None

        open(output_file_gz, "wb").write(r.content)

        with gzip.open(output_file_gz, "rb") as f_in:
            with open(output_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        if os.path.exists(output_file_gz):
            os.remove(output_file_gz)
        return output_file

    @classmethod
    def download_vnf(cls, date_from, date_to):
        dates = pd.date_range(to_datetime(date_from), to_datetime(date_to))
        for date in dates:
            cls.download_vnf_date(date)

    @classmethod
    def get_vnf(cls, date):
        vnf_file = cls.download_vnf_date(date)

        if not vnf_file:
            return None

        try:
            return pd.read_csv(vnf_file)
        except pd.errors.EmptyDataError:
            return None

    @staticmethod
    def date_to_localpath(date, ext="csv"):
        gis_dir = get_env("GIS_DIR")
        vnf_folder = os.path.join(gis_dir, "fire", "vnf")
        basename = "vnf_%s.%s" % (date.strftime("%Y%m%d"), ext)
        return os.path.join(vnf_folder, basename)
