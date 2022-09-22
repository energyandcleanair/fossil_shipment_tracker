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
from base.logger import logger
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

    fields['type'] = 'field'
    fields['name'] = fields.clusterCaption
    return fields


def get_infrastructure():

    url = "https://greeninfo-network.github.io/global-gas-infrastructure-tracker/data/data.csv?v=2.1"
    infra = pd.read_csv(url)
    infra = infra[infra.countries.str.contains('Russia')]

    lines = infra[(infra.geom == 'line') & (infra.status == 'operating')]
    points = infra[(infra.geom == 'point') & ((infra.status == 'operating') | infra.project.str.contains('Portovaya'))]

    def route_to_linestring(route):
        # multiline = infra.route.str.contains(';')
        # if multiline:
        lines = route.split(';')
        segments = [x.split(':') for x in lines]
        coords = [[[float(z) for z in y.split(',')] for y in x ] for x in segments]
        return shapely.geometry.MultiLineString(coords)

    lines['geometry'] = lines.route.apply(route_to_linestring)
    lines['geometry'] = lines.geometry.map(lambda pt: shapely.ops.transform(lambda x, y: (y, x), pt))

    points['type'] = 'point'
    lines['type'] = 'pipeline'

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

    flares = flares_raw
    flares = flares[(flares.Temp_BB > 1200) &
                    (flares.Temp_BB < 999999)].copy()
    flares['rhp'] = sigma * np.power(flares.Temp_BB, 4) * np.power(flares.Area_BB, d)
    flares['bcm_est'] = b1 * flares.rhp

    flares = flares[['Date_LTZ', 'Lon_GMTCO', 'Lat_GMTCO', 'bcm_est']] \
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

    result = over.groupby(['id', 'type', 'date']) \
        .agg(bcm_est=('bcm_est', np.nansum),
             count=('bcm_est', count_non_na))

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
                   buffer_km_fields=20,
                   buffer_km_lines_points=10):
    
    
    # Get geomtries, buffer, dissolve
    geometries = pd.concat([
        buffer(facilities[facilities.type=='field'], buffer_km_fields),
        buffer(facilities[facilities.type != 'field'], buffer_km_lines_points),
    ], ignore_index=True)


    # Get flaring amount
    dates = pd.date_range(to_datetime(date_from), to_datetime(date_to))
    res = []
    for date in tqdm(dates):
        res.append(get_flaring_amount(date=date,
                                      geometries=geometries))

    res = [x for x in res if x is not None]
    if not res:
        return []

    res = pd.concat(res) \
        .groupby(['id', 'type', 'date'])[['bcm_est', 'count']] \
        .sum() \
        .reset_index()

    # Format for table
    res = res[['id', 'date', 'bcm_est']].rename(columns={'id': 'facility_id',
                                                'bcm_est': 'value'})

    return res



#
#
#   # Global tendencies
#   flare_amounts %>%
#     group_by(type, date) %>%
#     summarise_at(c('bcm_est'), sum, na.rm=T) %>%
#     rcrea::utils.running_average(14, vars_to_avg = c('bcm_est'), min_values = 10) %>%
#     mutate(year=lubridate::year(date),
#            date000=`year<-`(date, 2000)) %>%
#     ggplot() +
#       geom_line(aes(date000, bcm_est, col=factor(year))) +
#     scale_x_date(date_labels = '%b') +
#     scale_colour_brewer(palette='Reds', name=NULL) +
#     rcrea::theme_crea() +
#     facet_wrap(~type) +
#     labs(title='Fire radiative power around Russian gas infrastructure',
#          subtitle='14-day running average of radiative power within 20km of gas fields and 5km of gas infrastructure',
#          y='MW',
#          x=NULL,
#          caption='Source: CREA analysis based on VIIRS, EnergyBase.ru and Global Energy Monitor.')
#
#   ggsave('flaring.jpg', width=6, height=4, scale=1.5, dpi=150)
#
#
#   # Top fields
#   top_fields <- flare_amounts %>%
#     filter(type=='pipeline') %>%
#     group_by(id) %>%
#     summarise(bcm_est=sum(bcm_est, na.rm=T)) %>%
#     arrange(desc(bcm_est)) %>%
#     pull(id) %>%
#     head(20)
#
#
#   flare_amounts %>%
#     filter(id %in% top_fields) %>%
#     rcrea::utils.running_average(14, vars_to_avg = c('bcm_est', 'count'),
#                                  min_values=10) %>%
#     mutate(year=lubridate::year(date),
#            date000=`year<-`(date, 2000)) %>%
#     ggplot() +
#     geom_line(aes(date000, bcm_est, col=factor(year))) +
#     scale_x_date(date_labels = '%b') +
#     facet_wrap(~id, scales='free_y') +
#     scale_colour_brewer(palette='Reds', name=NULL) +
#     rcrea::theme_crea()
#
#
#   # Top fields
#   flare_amounts %>%
#     # filter(type=='pipeline') %>%
#     filter(id %in% 'Nord Stream Gas Pipeline') %>%
#     # rcrea::utils.running_average(14, vars_to_avg = c('bcm_est')) %>%
#     mutate(year=lubridate::year(date),
#            date000=`year<-`(date, 2000)) %>%
#     ggplot() +
#     geom_bar(aes(date, bcm_est), stat='identity') +
#     # scale_x_date(date_labels = '%b') +
#     facet_wrap(~id, scales='free_y')
#
#   dir.create('cache', F)
#   saveRDS(flare_amounts, 'cache/flaring.RDS')
#
#   return(flare_amounts)
# }
#
#
# flaring.detect_anomalies <- function(flare_amounts){
#
#   library(anomalize)
#
#   d <- flare_amounts %>%
#     filter(grepl('Northern Lights Gas Pipeline', id, ignore.case = T))
#
#
#  decomposed <- flare_amounts %>%
#     group_by(id) %>%
#    group_map(function(x, id){
#      print(head(id))
#      x %>%
#        time_decompose(bcm_est, method = "stl") %>%
#        anomalize(remainder, method = "iqr")
#    })
#
#
# }
#
