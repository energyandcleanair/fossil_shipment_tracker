import geopandas as gpd
import shapely
import json
import numpy as np
import pandas as pd
import os
import requests
import gzip
import shutil
import datetime as dt
from tqdm import tqdm
from geoalchemy2 import Geometry
from sqlalchemy import func

from base.env import get_env
from base.logger import logger, logger_slack
from base.utils import to_datetime
from base.utils import update_geometry_from_wkb
from base.db_utils import upsert
from base.models import DB_TABLE_FLARING_FACILITY, DB_TABLE_FLARING
from base.models import FlaringFacility, Flaring
from base.db import session


def fill():
    """
    Used to fill infrastructure the first time.
    Will erase all existing facilities.
    :return:
    """
    facilities = get_flaring_facilities()
    facilities = update_geometry_from_wkb(facilities, to="wkt")
    facilities = gpd.GeoDataFrame(facilities)
    upsert(df=pd.DataFrame(facilities),
           table=DB_TABLE_FLARING_FACILITY,
           constraint_name="flaring_facility_pkey",
           dtype={'geometry': Geometry('GEOMETRY', 4326)})
    return


def update(date_from='2015-01-01',
           date_to=-2,
           force=False):

    logger_slack.info("=== Flaring update ===")
    facilities = pd.read_sql(session.query(FlaringFacility).statement, session.bind)
    facilities = update_geometry_from_wkb(facilities, to="shape")

    if not force:
        date_from = session.query(func.max(Flaring.date)).first()[0] or date_from

    flares = get_flaring_ts(facilities=facilities,
                            date_from=to_datetime(date_from),
                            date_to=to_datetime(date_to))

    upsert(df=flares,
           table=DB_TABLE_FLARING,
           constraint_name="unique_flaring")
    return


def get_flaring_facilities():
    """
    Combine oil/gas fields, pipelines, terminals
    :return:
    """
    fields = get_fields(gas_only=True)
    lines_points = get_infrastructure()
    facilities = gpd.GeoDataFrame(pd.concat([fields[['name', 'type', 'geometry', 'url']],
                                             lines_points[['name', 'type', 'geometry', 'url']]]))
    facilities = facilities.dissolve(by="name").reset_index()
    facilities['id'] = np.arange(len(facilities))

    # Add English name
    english_lookup = pd.read_csv('assets/flaring/english_lookup.csv')
    facilities = facilities.merge(english_lookup[['name', 'name_en']], on='name')
    facilities['name_en'] = facilities.name_en.combine_first(facilities.name)

    return facilities


def get_fields(gas_only=True):

    # 403: need to download the file manually
    # url = "https://api.energybase.ru/v1/map-feature/field"
    file_path = "assets/flaring/russia_oil_gas_fields.json"
    fields = gpd.read_file(file_path)
    fields['geometry'] = fields.geometry.map(lambda pt: shapely.ops.transform(lambda x, y: (y, x), pt))

    pattern = r'(https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}[-a-zA-Z0-9()@:%_+.~#?&/=]*)'
    fields['url'] = fields['balloonContentHeader'].str.extract(pattern, expand=False).str.strip()

    if gas_only:
        with open(file_path, 'r') as file:
            data = json.load(file)

        is_gas = ['gas.svg' in x['options']['iconImageHref'] for x in data['features']]
        fields = fields.iloc[np.where(is_gas)]

    fields['type'] = 'Field'
    fields['name'] = fields.clusterCaption
    return fields


def get_infrastructure():

    url = "https://greeninfo-network.github.io/global-gas-infrastructure-tracker/data/data.csv?v=2.1"
    infra = pd.read_csv(url)
    infra = infra[infra.countries.str.contains('Russia')]

    lines = infra[(infra.geom == 'line') & (infra.status == 'operating')].copy()
    points = infra[(infra.geom == 'point') & ((infra.status == 'operating') | infra.project.str.contains('Portovaya'))].copy()

    def route_to_linestring(route):
        # multiline = infra.route.str.contains(';')
        # if multiline:
        lines = route.split(';')
        segments = [x.split(':') for x in lines]
        coords = [[[float(z) for z in y.split(',')] for y in x ] for x in segments]
        return shapely.geometry.MultiLineString(coords)

    lines['geometry'] = lines.route.apply(route_to_linestring)
    lines['geometry'] = lines.geometry.map(lambda pt: shapely.ops.transform(lambda x, y: (y, x), pt))

    points['geometry'] = gpd.points_from_xy(points.lng, points.lat)
    points['type'] = 'LNG Terminal'
    lines['type'] = 'Pipeline'

    return pd.concat([points, lines]).rename(columns={'project': 'name'})


def date_to_localpath(date, ext='csv'):
    gis_dir = get_env('GIS_DIR')
    nvf_folder = os.path.join(gis_dir, 'fire', 'nvf')
    basename = 'nvf_%s.%s' % (date.strftime('%Y%m%d'),ext)
    return os.path.join(nvf_folder, basename)


def download_nvf_date(date, force=False):

    date = to_datetime(date)
    output_file = date_to_localpath(date, ext='csv')
    output_file_gz = date_to_localpath(date, ext='csv.gz')
    if os.path.exists(output_file) and not force:
        return True

    params = {
        'client_id': 'eogdata_oidc',
        'client_secret': get_env('NVF_MINES_SECRET'),
        'username': get_env('NVF_MINES_EMAIL'),
        'password': get_env('NVF_MINES_PASSWORD'),
        'grant_type': 'password'
    }
    token_url = 'https://eogauth.mines.edu/auth/realms/master/protocol/openid-connect/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    response = requests.post(token_url, data=params, headers=headers)
    if response.status_code != 200:
        logger.warning("NVF: Failed to query NVF for %s" % (date,))
        return False

    access_token_list = response.json()
    access_token = access_token_list['access_token']

    # Submit request with token bearer and write to output file
    data_url = 'https://eogdata.mines.edu/wwwdata/viirs_products/vnf/v30//VNF_npp_d%s_noaa_v30-ez.csv.gz' % (date.strftime('%Y%m%d'),)
    auth = 'Bearer %s' % (access_token,)

    # urllib.request.urlretrieve(data_url, data_url,  )
    r = requests.get(data_url, allow_redirects=True, headers={'Authorization': auth})
    if r.status_code != 200:
        return False

    open(output_file_gz, 'wb').write(r.content)

    with gzip.open(output_file_gz, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    if os.path.exists(output_file_gz):
        os.remove(output_file_gz)

    return os.path.exists(output_file)



def download_nvf(date_from, date_to):
    dates = pd.date_range(to_datetime(date_from),
                         to_datetime(date_to))
    for date in dates:
        download_nvf_date(date)


def get_nvf(date):
    nvf_file = date_to_localpath(date)
    if not download_nvf_date(date):
        return None

    try:
        return pd.read_csv(nvf_file)
    except pd.errors.EmptyDataError:
        return None


def get_flaring_amount(date, geometries):
    flares_raw = get_nvf(date=date)
    if flares_raw is None:
        return None

    # Only keep relevant
    # https://www.mdpi.com/2072-4292/13/16/3078/htm
    # RH’=σT^4S^d
    sigma = 5.67E-8
    b1 = 0.0294
    d = 0.7

    flares_raw = flares_raw[(flares_raw.Temp_BB > 1200) & (flares_raw.Temp_BB < 999999)]

    # Flares in 'BCM'
    flares_bcm = flares_raw.copy()
    flares_bcm['rhp'] = sigma * np.power(flares_bcm.Temp_BB, 4) * np.power(flares_bcm.Area_BB, d)
    flares_bcm['value'] = b1 * flares_bcm.rhp
    flares_bcm['unit'] = 'index' #Until we're confident this is bcm...

    # Flares in MW
    flares_mw = flares_raw.copy()
    flares_mw['value'] = flares_mw.RH
    flares_mw['unit'] = 'mw'

    flares = pd.concat([flares_bcm, flares_mw], ignore_index=True)

    flares = flares[['Date_LTZ', 'Lon_GMTCO', 'Lat_GMTCO', 'value', 'unit']] \
        .rename(columns={'Date_LTZ': 'date',
                         'Lon_GMTCO': 'lon',
                         'Lat_GMTCO': 'lat'})

    flares_gdf = gpd.GeoDataFrame(flares,
                                  geometry=gpd.points_from_xy(flares.lon, flares.lat))
    flares_gdf.crs = 4326
    flares_gdf['date'] = pd.to_datetime(flares_gdf.date).dt.floor('D')

    over = gpd.GeoDataFrame(geometries[['id', 'type', 'geometry']]) \
        .sjoin(flares_gdf, how="left")

    over['date'] = over.date.fillna(date)

    def count_non_na(x):
        return sum(~np.isnan(x))

    result = over.groupby(['id', 'date', 'unit']) \
        .agg(value=('value', np.nansum),
             count=('value', count_non_na)) \
        .reset_index()


    # Complete cases, filling with 0
    import itertools
    ids = geometries.id.unique()
    units = ['mw', 'index']
    combined = [ids, units]
    merger = pd.DataFrame(columns=['id', 'unit'], data=list(itertools.product(*combined)))
    result = merger.merge(result, how='left')
    result['date'] = result.date.fillna(date)
    result['value'] = result.value.fillna(0)

    return result


def buffer(df, buffer_km):
    df = gpd.GeoDataFrame(df)
    df.crs = 4326
    df = df.to_crs(3857)  # Pick another
    df['geometry'] = df['geometry'].buffer(buffer_km*1000)
    df = df.to_crs(4326)  # Back to 4326
    return df


def get_flaring_ts(facilities,
                   date_from="2018-01-01",
                   date_to=to_datetime(-3),
                   buffer_km_fields=10,
                   buffer_km_infra=5):


    # Get geomtries, buffer, dissolve
    buffered_fields = buffer(facilities[facilities.type == 'Field'], buffer_km_fields)
    buffered_fields['buffer_km'] = buffer_km_fields

    buffered_infra = buffer(facilities[facilities.type != 'Field'], buffer_km_infra)
    buffered_infra['buffer_km'] = buffer_km_infra

    geometries = pd.concat([
        buffered_fields,
        buffered_infra
    ], ignore_index=True)

    # Get flaring amount
    dates = pd.date_range(to_datetime(date_from), to_datetime(date_to))
    res = []
    pbar = tqdm(dates)

    for date in pbar:
        pbar.set_description("Processing %s" % date)
        res.append(get_flaring_amount(date=date,
                                      geometries=geometries))

    res = [x for x in res if x is not None]
    if not res:
        return []

    res = pd.concat(res) \
        .groupby(['id', 'date', 'unit'])[['value', 'count']] \
        .sum() \
        .reset_index()

    # Adding buffer info
    res = res.merge(geometries[['id', 'buffer_km']])

    # Format for table
    res = res[['id', 'date', 'unit', 'value', 'buffer_km']] \
        .rename(columns={'id': 'facility_id'})

    return res
