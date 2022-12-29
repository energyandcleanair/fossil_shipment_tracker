"""
Functions to scrape and aggregate physical flows from ENTSO-G tranpsarency platform

Author: Hubert Thieriot hubert@energyandcleanair.org

MIT License

Copyright (c) 2022 Centre for Research on Energy and Clean Air

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from time import sleep
import datetime as dt
import pandas as pd
import numpy as np
from collections import defaultdict
import sqlalchemy as sa
import requests
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm
tqdm.pandas()

import base
from base.db import engine
from base.db import session
from base.logger import logger, logger_slack
from base.utils import to_list, to_datetime
from base.db_utils import upsert
from base.models import DB_TABLE_ENTSOGFLOW, DB_TABLE_ENTSOGFLOW_RAW, EntsogFlow, EntsogFlowRaw
from base.db_utils import upsert


s = requests.Session()

retries = Retry(total=10,
                backoff_factor=2,
                status_forcelist=[500, 502, 503, 504])

s.mount('https://', HTTPAdapter(max_retries=retries))



class EntsogApi:

    def split(x, f):
        res = defaultdict(list)
        for v, k in zip(x, f):
            res[k].append(v)
        return res

    @staticmethod
    def api_req(url, params={}, limit=-1):
        params['limit'] = limit
        api_result = s.get(url, params=params, timeout=60)

        if api_result.status_code != 200:
            logger.warning("ENTSOG: Failed to query entsog %s %s" % (url, params))
            return None

        res = api_result.json()

        if res == {'message': 'No Data Available'}:
            return None

        try:
            if res['meta'] is not None and res["meta"]["total"] > res["meta"]["count"] * 1.2:
                # *1.X: for some reason, sometimes total is slightly superior to actual count
                # yet inferior to limit
                logger.warning("More data available (%d/%d). Increase limit or implement a loop here...",
                               res["meta"]["total"], res["meta"]["count"])
        except KeyError as e:
            logger.warning("May have failed for: %s %s" % (url, params))
            return None

        return res

    @staticmethod
    def get_balancing_zones():
        url = "https://transparency.entsog.eu/api/v1/balancingZones"
        d = EntsogApi.api_req(url).get("balancingZones")
        df = pd.DataFrame(d)
        return df

    @staticmethod
    def get_operators():
        url = "https://transparency.entsog.eu/api/v1/operators"
        d = EntsogApi.api_req(url).get("operators")
        df = pd.DataFrame(d)
        return df

    @staticmethod
    def get_interconnections(from_operator_key=None, to_operator_key=None):
        url = "https://transparency.entsog.eu/api/v1/interconnections"
        params = {}

        if from_operator_key:
            params['fromOperatorKey'] = ",".join(to_list(from_operator_key))

        if to_operator_key:
            params['to_operator_key'] = ",".join(to_list(to_operator_key))

        d = EntsogApi.api_req(url, params=params)
        d = d.get("interconnections")
        df = pd.DataFrame(d)
        return df

    @staticmethod
    def get_operator_point_directions():
        url = "https://transparency.entsog.eu/api/v1/operatorpointdirections"
        params = {}
        d = EntsogApi.api_req(url, params=params)
        d = d.get("operatorpointdirections")
        df = pd.DataFrame(d)
        return df

    @staticmethod
    def get_physical_flows(points,
                           date_from="2019-01-01",
                           date_to=dt.date.today()):

        # Exit points
        exit_points = points[points.directionKey == 'exit']
        exit_flows = EntsogApi._get_physical_flows(operator_key=exit_points.operatorKey.to_list(),
                                                    point_key=exit_points.pointKey.to_list(),
                                                    direction_key='exit',
                                                    date_from=date_from,
                                                    date_to=date_to
                                                    )
        # Entry points
        entry_points = points[points.directionKey == 'entry']
        entry_flows = EntsogApi._get_physical_flows(operator_key=entry_points.operatorKey.to_list(),
                                                    point_key=entry_points.pointKey.to_list(),
                                                    direction_key='entry',
                                                    date_from=date_from,
                                                    date_to=date_to
                                                    )

        return pd.concat([exit_flows, entry_flows], ignore_index=True)


    @staticmethod
    def _get_physical_flows(operator_key,
                           point_key,
                           direction_key,
                           date_from="2019-01-01",
                           date_to=dt.date.today(),
                           limit=-1):

        url = "https://transparency.entsog.eu/api/v1/operationalData"

        if point_key is not None and operator_key is None:
            raise ValueError("Needs to specify operator_key when point_key is given.")

        # Can only do one operator at a time
        if operator_key is not None and len(set(to_list(operator_key))) > 1:
            logger.info("Splitting by operator")
            splitted = EntsogApi.split(point_key, operator_key)
            result = []
            for operator_key, point_keys in tqdm(splitted.items()):
                r = EntsogApi._get_physical_flows(operator_key=operator_key,
                                       point_key=point_keys,
                                       direction_key=direction_key,
                                       date_from=date_from,
                                       date_to=date_to,
                                       limit=limit)
                if r is not None:
                    result.append(r)

            if result:
                return pd.concat(result, axis=0).drop_duplicates()
            else:
                return None

        # Can only do limited days per call. Doing a call per semester
        dates = pd.date_range(to_datetime(date_from),
                              to_datetime(date_to),
                              freq='d').to_list()
        dates_group = [str(x.year) + str((x.month - 1) // 6) for x in dates]

        if len(set(dates_group)) > 1:
            # logger.info("Splitting by dates")
            splitted = EntsogApi.split(dates, dates_group)
            result = []
            for dates in splitted.values():
                r = EntsogApi._get_physical_flows(operator_key=operator_key,
                                       point_key=point_key,
                                       direction_key=direction_key,
                                       date_from=min(dates),
                                       date_to=max(dates),
                                       limit=limit)
                if r is not None:
                    result.append(r)

            if result:
                return pd.concat(result, axis=0).drop_duplicates()
            else:
                return None

        params = {
            "indicator": "Physical Flow,GCV",
            "periodType": "day",
            "timezone": "CET"
        }

        if operator_key is not None:
            params['operatorKey'] = ','.join(list(set(to_list(operator_key))))

        if point_key is not None:
            params['pointKey'] = ','.join(list(set(to_list(point_key))))

        if date_from is not None:
            params['from'] = to_datetime(date_from).strftime("%Y-%m-%d")

        if date_to is not None:
            params['to'] = to_datetime(date_to).strftime("%Y-%m-%d")

        if direction_key is not None:
            params['directionKey'] = direction_key

        d = EntsogApi.api_req(url, params=params, limit=limit)

        if d is None or not d.get("operationalData"):
            return None

        df = pd.DataFrame(d.get("operationalData"))
        df["value"] = pd.to_numeric(df.value)
        df["isCmpRelevant"] = df["isCmpRelevant"].astype('bool')
        df["isCamRelevant"] = df["isCamRelevant"].astype('bool')
        df["periodFrom"] = pd.to_datetime(df.periodFrom, errors='coerce')
        df["periodTo"] = pd.to_datetime(df.periodTo)
        df["date"] = df.periodFrom.apply(lambda x: x.date())

        len_before = len(df)
        df = df.pivot_table(index=['pointKey', 'operatorKey', 'directionKey',
                                  'periodFrom', 'periodTo','flowStatus'],
                           columns=['indicator'],
                           values=['value'],
                           dropna=True).reset_index()
        len_after = len(df)
        assert len_after == len_before / 2

        # Remove 'value' in column names
        df.columns = [col[1] or col[0] for col in df.columns]

        # Fill GCV
        df['GCV'].replace({0: np.nanmedian(df.GCV),
                           np.nan: np.nanmedian(df.GCV)},
                           inplace=True)

        df.rename(columns={'Physical Flow': 'value_kwh',
                           'GCV': 'gcv_kwh_m3'},
                  inplace=True)

        # IMPORTANT
        # ENTSOG has duplicated records
        df = df.drop_duplicates()
        return df

    @staticmethod
    def get_aggregated_physical_flows(date_from="2021-01-01",
                                      date_to=dt.date.today(),
                                      limit=-1):
        url = "https://transparency.entsog.eu/api/v1/AggregatedData"

        params = {
            "indicator": "Physical Flow",
            "periodType": "day",
            "timezone": "CET"
        }

        if date_from:
            params['from'] = to_datetime(date_from).strftime("%Y-%m-%d")

        if date_to:
            params['to'] = to_datetime(date_to).strftime("%Y-%m-%d")

        d = EntsogApi.api_req(url, params=params, limit=limit)

        if d is None:
            return None

        df = pd.DataFrame(d.get("AggregatedData"))
        df["value"] = pd.to_numeric(df.value)
        df["periodFrom"] = pd.to_datetime(df.periodFrom)
        df["periodTo"] = pd.to_datetime(df.periodTo)
        df["date"] = df.periodFrom.dt.date
        df = df.drop_duplicates()
        return df


class EntsogDb:
    @staticmethod
    def get_physical_flows(points, date_from, date_to):

        # Download for all points (probably too much info)
        # and then inner join
        query = session.query(EntsogFlowRaw) \
            .filter(EntsogFlowRaw.pointKey.in_(points.pointKey.to_list()),
                    EntsogFlowRaw.date >= date_from,
                    EntsogFlowRaw.date <= date_to
                    )
        flows_all = pd.read_sql(query.statement, session.bind)
        flows_raw = points[['pointKey', 'pointLabel', 'operatorKey', 'operatorLabel', 'directionKey',
                        'country', 'partner', 'type']] \
            .merge(flows_all, how='inner').drop(['id', 'updated_on'], axis=1)
        return flows_raw

    @staticmethod
    def upload_flows_raw(flows):
        to_upload = flows[['id', 'date', 'periodFrom', 'periodTo',
                               'pointKey', 'operatorKey', 'directionKey', 'flowStatus', 'value_kwh', 'gcv_kwh_m3']]

        to_upload = to_upload[~pd.isna(to_upload.value_kwh)]
        try:
            to_upload.to_sql(DB_TABLE_ENTSOGFLOW_RAW, con=engine, if_exists="append", index=False)
        except sa.exc.IntegrityError:
            logger.info('Failed at inserting. Trying upserting instead (much slower).')
            upsert(df=to_upload, table=DB_TABLE_ENTSOGFLOW_RAW, constraint_name=DB_TABLE_ENTSOGFLOW_RAW + '_pkey')

    @staticmethod
    def upload_flows(flows, delete_before_upload=False):
        flows = flows[['commodity', 'departure_iso2', 'destination_iso2', 'date',
                       'value_tonne', 'value_mwh', 'value_m3', 'type']]

        # For flows update for debug or manual cleaning
        flows['updated_on'] = dt.datetime.now()

        if delete_before_upload:
            session.query(EntsogFlow) \
                .filter(EntsogFlow.date >= min(flows.date),
                        EntsogFlow.date <= max(flows.date),
                        ) \
                .delete()
            session.commit()

        try:
            flows.to_sql(DB_TABLE_ENTSOGFLOW, con=engine, if_exists="append", index=False)
        except sa.exc.IntegrityError:
            logger.info('Failed at inserting. Trying upserting instead (much slower).')
            upsert(df=flows, table=DB_TABLE_ENTSOGFLOW, constraint_name='unique_entsogflow')


def fix_opd_countries(opd):
    # Some adjacentCountries are wrong in opd data or simply missing
    # We trust interconnections better (e.g. it includes Algeria and Albania)
    # Also, we use tsoCountry by default but use IC to update it when available
    len_before = len(opd)

    # Fixing adjacent country for EU - Non EU using interconnections
    ic = EntsogApi.get_interconnections()
    added_adjacent_entries = ic[['toDirectionKey', 'fromCountryKey', 'fromOperatorKey', 'toPointKey', 'toOperatorKey', 'toCountryKey']] \
        .rename(columns={'toDirectionKey': 'directionKey',
                         'fromCountryKey': 'adjacentCountryFromIc',
                         'fromOperatorKey': 'adjacentOperatorKey',
                         'toPointKey': 'pointKey',
                         'toOperatorKey': 'operatorKey',
                         'toCountryKey': 'countryFromIc'}) \
        .drop_duplicates()

    opd = opd \
        .merge(added_adjacent_entries, on=['directionKey',
                                           'adjacentOperatorKey',
                                           'pointKey',
                                           'operatorKey'],
               how='left')

    # Use ic country when available
    opd['partner'] = opd[['adjacentCountryFromIc', 'adjacentCountry']].bfill(axis=1).iloc[:, 0]
    opd['country'] = opd[['countryFromIc', 'tSOCountry']].bfill(axis=1).iloc[:, 0]
    opd.drop(['adjacentCountryFromIc', 'countryFromIc'], axis=1, inplace=True)

    # Manual fixes
    # Greece to Albania is 77.5 TWh in 2021 according to IEA, but ENTSOG doesn't capture it
    # We for now bypass Albania, and assume the IT / TAP (which goes from Albania to IT)
    # is actually from Greece. In 2021 it was 75.9 TWh. Difference is probably Albania consumption
    # Improvement: account for that
    # Also, it was attributed to Switzerland because the TSO it Swiss
    # TODO check if others are wrong
    opd.loc[(opd.pointKey == 'ITP-00008') & (opd.operatorKey=='AL-TSO-0001'), 'country'] = 'GR'
    opd.loc[(opd.pointKey == 'ITP-00008') & (opd.operatorKey == 'IT-TSO-0001'), 'partner'] = 'GR'

    # LNG marked as LNG
    # Zeebrugge
    opd.loc[(opd.pointKey == 'LNG-00017') & (opd.directionKey == 'entry'), 'partner'] = 'lng'

    # Nea Mesimvria is from GR to GR, not from CH to GR
    opd.loc[opd.pointLabel == 'Nea Mesimvria', 'partner'] = 'GR'
    opd.loc[opd.pointLabel == 'Nea Mesimvria', 'country'] = 'GR'

    # EELV - Russia (Luhammar | Korneti)
    # TO Russia
    opd.loc[opd.id == '5LV-TSO-0001ITP-00493exit', 'partner'] = 'RU'
    # From Russia
    opd.loc[opd.id == '5LV-TSO-0001ITP-00493entry', 'partner'] = 'RU'

    len_after = len(opd)
    assert len_after == len_before

    # Remove crossborder within countries
    is_crossborder = opd.pointType.str.contains('Cross-Border')
    opd = opd[(opd.country != opd.partner) | ~is_crossborder]

    # Brandov
    # Remove transit DE-DE
    opd = opd[opd.id != '5DE-TSO-0016ITP-00452exitDE-TSO-0020']

    # Moffatt
    # Remove transit UK-UK (Northern Ireland)
    # Assuming everything goes to Ireland
    opd = opd[(opd.pointKey != 'ITP-00090') | (opd.country != opd.partner)]

    # Mark LNG partner as lng
    opd.loc[opd.pointType.str.contains('LNG Entry point'), 'partner'] = 'lng'
    opd.loc[opd.pointType.str.contains('LNG Exit point'), 'partner'] = 'lng'

    # Simplify cross-border storages -> single country
    # Consider Haidak and Haiming as a German storage
    is_crossborder_storage = opd.pointType.str.contains('Cross-Border Storage')
    german = opd.pointLabel.str.contains('Haiming|Haidach')
    opd.loc[german & is_crossborder_storage, 'country'] = 'DE'
    opd.loc[german & is_crossborder_storage, 'partner'] = 'DE'

    # Netherland storages
    netherlands = opd.pointLabel.str.contains('Etzel|Jemgum|Enschede|Nüttermoor|Vlieghuis')
    opd.loc[netherlands & is_crossborder_storage, 'country'] = 'NL'
    opd.loc[netherlands & is_crossborder_storage, 'partner'] = 'NL'

    # Slovakia storages
    slovakia = opd.pointLabel.str.contains('Bojanovice')
    opd.loc[slovakia & is_crossborder_storage, 'country'] = 'SK'
    opd.loc[slovakia & is_crossborder_storage, 'partner'] = 'SK'

    # Make storage single country (which makes it summable on user side)
    # import_storage = opd.pointType.str.contains('Cross-Border Storage') & (opd.directionKey=='entry')
    # opd.loc[import_storage, 'country'] = opd.loc[import_storage, 'partner']
    #
    # export_storage = opd.pointType.str.contains('Cross-Border Storage') & (opd.directionKey == 'exit')
    # opd.loc[export_storage, 'country'] = opd.loc[export_storage, 'partner']

    return opd


def get_points(country_iso2=None,
              remove_operators=[],
              remove_point_labels=[],
              remove_point_ids=[],
              remove_pipe_in_pipe=True,
              use_csv_selection=True,
              must_include_pointkeys=['UGS-00273']):

    opd = EntsogApi.get_operator_point_directions()

    # First filters
    opd = opd[opd.hasData]

    opd = fix_opd_countries(opd)
    opd = opd[['id', 'pointKey', 'pointLabel', 'operatorKey', 'operatorLabel', 'directionKey',
               'country', 'partner', 'pointType', 'crossBorderPointType', 'isPipeInPipe', 'isDoubleReporting']] \
        .drop_duplicates()

    if country_iso2:
        opd = opd.loc[opd.country.isin(to_list(country_iso2)) | opd.partner.isin(to_list(country_iso2))]

    if remove_operators:
        opd = opd.loc[~opd.operatorKey.isin(to_list(remove_operators))]

    if remove_point_labels:
        opd = opd.loc[~opd.pointLabel.isin(to_list(remove_point_labels))]

    if remove_point_ids:
        opd = opd.loc[~opd.id.isin(to_list(remove_point_ids))]

    if remove_pipe_in_pipe:
        # opd = opd.loc[opd.isPipeInPipe.isnull() |  ~opd.isPipeInPipe \
        #  | (opd.isPipeInPipe & opd.isDoubleReporting.isnull())]
        # For storage only as of now)
        opd = opd[~opd.pointType.str.contains('Storage') | ~opd.isPipeInPipe |
                  opd.isPipeInPipe.isnull() \
            | (opd.isPipeInPipe &  ~opd.isDoubleReporting.replace({np.nan: True})) \
            | opd.pointKey.isin(must_include_pointkeys)]

    is_crossborder = opd.pointType.str.contains('Cross-Border Transmission') \
                     | (opd.pointType.str.contains('Transmission') \
                        & opd.crossBorderPointType.str.contains('Cross'))
    is_transmission = opd.pointType.str.startswith('Transmission') & ~is_crossborder
    is_storage = opd.pointType.str.contains('Storage')
    is_lng = opd.pointType.str.contains('LNG')
    is_production = opd.pointType.str.contains('production')
    is_consumption = opd.pointType.str.contains('Consumers')
    is_distribution = opd.pointType.str.contains('Distribution')
    is_trading = opd.pointType.str.contains('Trading')

    # Check that category coverage is complete and not ambiguous
    union = (is_crossborder.astype(int)
            + is_transmission.astype(int)
            + is_storage.astype(int)
            + is_lng.astype(int)
            + is_production.astype(int)
            + is_consumption.astype(int)
            + is_distribution.astype(int)
            + is_trading.astype(int))

    assert all(union == 1)

    is_entry = opd.directionKey == 'entry'
    is_exit = opd.directionKey == 'exit'

    opd.loc[is_crossborder, 'type'] = base.ENTSOG_CROSSBORDER
    opd.loc[is_transmission, 'type'] = base.ENTSOG_TRANSMISSION
    opd.loc[is_storage, 'type'] = base.ENTSOG_STORAGE
    opd.loc[is_lng, 'type'] = base.ENTSOG_LNG
    opd.loc[is_production, 'type'] = base.ENTSOG_PRODUCTION
    opd.loc[is_consumption, 'type'] = base.ENTSOG_CONSUMPTION
    opd.loc[is_distribution, 'type'] = base.ENTSOG_DISTRIBUTION
    opd.loc[is_trading, 'type'] = base.ENTSOG_TRADING

    if use_csv_selection:
        to_remove = pd.read_csv('assets/entsog/opd_to_remove.csv').drop_duplicates()
        # Do an antijoin
        outer_join = opd.merge(to_remove, how='outer', indicator=True)
        opd = outer_join[(outer_join._merge == 'left_only')].drop('_merge', axis=1)

    def keep_unique(x):
        return x.drop_duplicates(subset=['pointKey', 'operatorKey', 'directionKey'])

    # We ignore TRADING points
    opd = opd[opd.type != base.ENTSOG_TRADING]
    opd = keep_unique(opd)
    return opd


def get_flows_raw(date_from='2022-01-01',
                  date_to=dt.date.today(),
                  country_iso2=None,
                  remove_operators=[],
                  remove_point_labels=[],
                  remove_point_ids=[],
                  remove_pipe_in_pipe=False,
                  use_csv_selection=True,
                  use_db=False):

    points = get_points(country_iso2=country_iso2,
                        remove_operators=remove_operators,
                        remove_point_labels=remove_point_labels,
                        remove_point_ids=remove_point_ids,
                        remove_pipe_in_pipe=remove_pipe_in_pipe,
                        use_csv_selection=use_csv_selection)
    
    if use_db:
        flows_raw = EntsogDb.get_physical_flows(points=points,
                                                date_from=date_from,
                                                date_to=date_to)

    else:
        flows_raw = EntsogApi.get_physical_flows(points=points,
                                                 date_from=date_from,
                                                 date_to=date_to)
    def add_countries(x):
        if x is None:
            return None
        return x.merge(
            points[['pointKey', 'operatorKey', 'directionKey', 'country', 'partner']] \
                .drop_duplicates())

    flows_raw = add_countries(flows_raw)
    return flows_raw


def process_flows_raw(flows_raw,
                              save_intermediary_to_file=False,
                              intermediary_filename=None,
                              save_to_file=False,
                              filename=None):

    # Reconcile import and exports
    flows_import = flows_raw[flows_raw.directionKey == 'entry']
    flows_export = flows_raw[flows_raw.directionKey == 'exit']
    flows_merged = flows_import.merge(flows_export,
                               left_on=['pointKey', 'date', 'country', 'partner', 'type'],
                               right_on=['pointKey', 'date', 'partner', 'country', 'type'],
                               how='outer',
                               suffixes=['_import', '_export']
                               )

    def keep_max_duration(df):
        # Take those with max duration only
        df['duration_import'] = df['periodTo_import'] - df['periodFrom_import']
        df['duration_export'] = df['periodTo_export'] - df['periodFrom_export']

        df = df.sort_values(by=['duration_import', 'duration_export'], ascending=False) \
            .groupby(['pointKey', 'date', 'type',
                         'operatorKey_import', 'operatorKey_export',
                         'operatorLabel_import', 'operatorLabel_export',
                         'pointLabel_import', 'pointLabel_export',
                         'flowStatus_import', 'flowStatus_export',
                         'country_import', 'country_export',
                         'partner_import', 'partner_export'
                         ],
                        dropna=False) \
            .head(1) \
            .reset_index(drop=True)
        return df

    def keep_confirmed_over_provisional(df):
        # Confirmed < Provisional
        df = df.sort_values(by=['flowStatus_import', 'flowStatus_export'],  ascending=True) \
            .groupby(['pointKey', 'date', 'type',
                         'operatorKey_import', 'operatorKey_export',
                         'operatorLabel_import', 'operatorLabel_export',
                         'pointLabel_import', 'pointLabel_export',
                         'country_import', 'country_export',
                         'partner_import', 'partner_export'
                         ],
                        dropna=False) \
            .head(1) \
            .reset_index(drop=True)
        return df

    def average_both_sides(df):
        def nanmean(x):
            # If all is nan return nan without warning
            if np.all(x != x):
                return np.NaN
            if np.all(x == 0):
                return 0
            if (np.std(x) / np.nanmean(x)) > 0.1:
                logger.warning("Flows are dissimilar before averaging")
            return np.nanmean(x)

        # Average on both sides: export and import
        df = df.groupby(['pointKey', 'date', 'type',
                         'operatorKey_import', 'operatorKey_export',
                         'operatorLabel_import', 'operatorLabel_export',
                         'pointLabel_import', 'pointLabel_export',
                    'flowStatus_import', 'flowStatus_export',
                    'country_import', 'country_export',
                    'partner_import', 'partner_export'],
                        dropna=False) \
                [['value_kwh_import', 'value_kwh_export',
                  'gcv_kwh_m3_import', 'gcv_kwh_m3_export']] \
            .agg(nanmean) \
            .reset_index()
        return df

    def coalesce_and_aggregate(df):
        df['partner'] = \
            np.where(df['country_export'].isnull(), df['partner_import'], df['country_export'])

        df['country'] = \
            np.where(df['country_import'].isnull(), df['partner_export'], df['country_import'])

        df['value_kwh'] = \
            np.where(df.type == base.ENTSOG_CROSSBORDER,
                np.where(df['value_kwh_import'].isnull(), df['value_kwh_export'], df['value_kwh_import']),
                df.value_kwh_import.fillna(0) - df.value_kwh_export.fillna(0),
                )

        df['gcv_kwh_m3'] = np.where(df['gcv_kwh_m3_import'].isnull(), df['gcv_kwh_m3_export'], df['gcv_kwh_m3_import'])
        df['value_m3'] = df.value_kwh / df.gcv_kwh_m3

        df = df.groupby(['pointKey', 'date', 'type',
                          'operatorKey_import', 'operatorKey_export',
                          'operatorLabel_import', 'operatorLabel_export',
                         'pointLabel_import', 'pointLabel_export',
                          'country', 'partner'],
                        dropna=False) \
            [['value_kwh', 'value_m3']].agg(np.nansum) \
            .reset_index()
        return df

    def remove_outliers(df):
        # IE UK have a couple outliers (>20bcm in one day...)
        max_bcm = 20
        max_kwh = max_bcm * 1e9 * base.GCV_KWH_PER_M3
        df = df[pd.isna(df.value_kwh_import) | (df.value_kwh_import < max_kwh)]
        df = df[pd.isna(df.value_kwh_export) | (df.value_kwh_export < max_kwh)]
        return df

    def process(df):
        df = remove_outliers(df)
        df = keep_max_duration(df)
        df = keep_confirmed_over_provisional(df)
        df = average_both_sides(df)
        df = coalesce_and_aggregate(df)
        return df

    flows_intermediary = process(flows_merged)

    if save_intermediary_to_file:
        intermediary_filename = intermediary_filename or "entsog_flows_intermediary.csv"
        flows_intermediary.to_csv(intermediary_filename, index=False)

    flows = flows_intermediary \
        .groupby(['country', 'partner', 'date', 'type'], dropna=False) \
        [['value_kwh', 'value_m3']].agg(np.nansum) \
        .reset_index() \
        .rename(columns={'country': 'destination_iso2',
                         'partner': 'departure_iso2'
                         }) \
        .reset_index()

    def fix_kipi_flows(flows):
        # Bruegel: Finally, on Turkey, our assumption was to attribute:
        # • All of Kipi to Azerbaijan,
        # • All of Strandzha to Russia.
        # -> we remove TR -> GR
        idx = (flows.departure_iso2 == 'TR') & (flows.destination_iso2 == 'GR')
        flows.loc[idx, 'departure_iso2'] = 'AZ'
        return flows

    flows = fix_kipi_flows(flows)
    flows['value_tonne'] = flows.value_m3 * base.KG_PER_M3 / 1000
    flows['value_mwh'] = flows.value_kwh / 1000
    flows['commodity'] = 'natural_gas'
    flows.drop(['value_kwh'], axis=1, inplace=True)
    flows.drop(['index'], axis=1, inplace=True)
    flows.replace({'departure_iso2': {'UK': 'GB'},
                   'destination_iso2': {'UK': 'GB'}},
                  inplace=True)
    flows.replace({'type': {base.ENTSOG_LNG: base.ENTSOG_CROSSBORDER}},
                  inplace=True)
    if save_to_file:
        filename = filename or "entsog_flows.csv"
        flows.to_csv(filename, index=False)

    return flows


def update_db(date_from='2022-01-01',
              date_to=dt.date.today(),
              force=False):

    # Last date
    if not force:
        date_from = session.query(sa.func.max(EntsogFlowRaw.updated_on)).first()[0] or date_from

    # ENTSOG doesn't have future data...
    date_to = min(to_datetime(date_to), to_datetime(dt.date.today()))

    if to_datetime(date_to) > to_datetime(date_from):
        # DB should contain all points, in case opd selection changes. We'll filter later
        points = get_points(use_csv_selection=False,
                            remove_pipe_in_pipe=False)
        buffer = dt.timedelta(days=7) #ENTSOG data might not be updated simultaneously for all OPDs
        date_from = to_datetime(date_from) - buffer
        flows_raw = EntsogApi.get_physical_flows(points=points,
                                                 date_from=date_from,
                                                 date_to=date_to)
        # Save to DB
        EntsogDb.upload_flows_raw(flows_raw)


def get_flows(date_from='2022-01-01',
              date_to=dt.date.today(),
              country_iso2=None,
              use_csv_selection=True,
              remove_pipe_in_pipe=False,
              force=False,
              save_intermediary_to_file=False,
              intermediary_filename=None,
              save_to_file=False,
              filename=None):

    # ENTSOG API -> ENTSOG DB
    update_db(date_from=date_from,
              date_to=date_to,
              force=force)

    # Get raw information from db
    flows_raw = get_flows_raw(date_from=date_from,
                              date_to=date_to,
                              country_iso2=country_iso2,
                              use_csv_selection=use_csv_selection,
                              remove_pipe_in_pipe=remove_pipe_in_pipe,
                              use_db=True)

    # Process cross border & production
    flows = process_flows_raw(flows_raw=flows_raw,
                              save_intermediary_to_file=save_intermediary_to_file,
                              intermediary_filename=intermediary_filename,
                              save_to_file=save_to_file,
                              filename=filename)

    return flows



def update(date_from=-7, date_to=dt.date.today(), country_iso2=None,
           filename=None,
           save_to_file=True,
           save_intermediary_to_file=False,
           intermediary_filename=None,
           nodata_error_date_from=None,
           force=False,
           delete_before_upload=False,
           remove_pipe_in_pipe=True):
    """

    :param date_from:
    :param date_to:
    :param filename:
    :param save_to_file:
    :param nodata_error_date_from: if no data after this date, raise an error. Can be an integer
    :return:
    """

    if isinstance(date_from, int):
        last_date = session.query(sa.func.max(EntsogFlow.date)).filter(EntsogFlow.value_m3 > 0).first()[0]
        date_from = to_datetime(last_date) + dt.timedelta(days=date_from)

    flows = None
    itry = 0
    ntries = 3

    while flows is None and itry <= ntries:
        itry += 1
        try:
            flows = get_flows(date_from=date_from,
                              date_to=date_to,
                              country_iso2=country_iso2,
                              remove_pipe_in_pipe=remove_pipe_in_pipe,
                              save_intermediary_to_file=save_intermediary_to_file,
                              intermediary_filename=intermediary_filename,
                              force=force,
                              save_to_file=save_to_file,
                              filename=filename)
        except TypeError:
            logger.warning("ENTSOG failed. Trying again")
            continue

    if flows is None:
        logger_slack.error("Failed to get ENTSOG data")
        raise ValueError("Failed to get ENTSOG data.")

    EntsogDb.upload_flows(flows, delete_before_upload=delete_before_upload)


    # Raise alert if no recent data was found
    if nodata_error_date_from is not None and flows.date.max() < to_datetime(nodata_error_date_from).date():
        logger_slack.error("No ENTSOG flow found after %s (most recent is %s)" % (to_datetime(nodata_error_date_from).date(), flows.date.max()))

    return flows