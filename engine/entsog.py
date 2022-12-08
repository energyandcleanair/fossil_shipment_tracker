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
from base.db import session
from base.logger import logger, logger_slack
from base.utils import to_list, to_datetime
from base.db_utils import upsert
from base.models import DB_TABLE_ENTSOGFLOW, EntsogFlow

s = requests.Session()

retries = Retry(total=10,
                backoff_factor=2,
                status_forcelist=[500, 502, 503, 504])

s.mount('https://', HTTPAdapter(max_retries=retries))


def split(x, f):
    res = defaultdict(list)
    for v, k in zip(x, f):
        res[k].append(v)
    return res


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


def get_balancing_zones():
    url = "https://transparency.entsog.eu/api/v1/balancingZones"
    d = api_req(url).get("balancingZones")
    df = pd.DataFrame(d)
    return df


def get_operators():
    url = "https://transparency.entsog.eu/api/v1/operators"
    d = api_req(url).get("operators")
    df = pd.DataFrame(d)
    return df


def get_interconnections(from_operator_key=None, to_operator_key=None):
    url = "https://transparency.entsog.eu/api/v1/interconnections"
    params = {}

    if from_operator_key:
        params['fromOperatorKey'] = ",".join(to_list(from_operator_key))

    if to_operator_key:
        params['to_operator_key'] = ",".join(to_list(to_operator_key))

    d = api_req(url, params=params)
    d = d.get("interconnections")
    df = pd.DataFrame(d)
    return df


def get_operator_point_directions():
    url = "https://transparency.entsog.eu/api/v1/operatorpointdirections"
    params = {}
    d = api_req(url, params=params)
    d = d.get("operatorpointdirections")
    df = pd.DataFrame(d)
    return df


def get_physical_flows(operator_key,
                       point_key,
                       direction,
                       date_from="2019-01-01",
                       date_to=dt.date.today(),
                       limit=-1):
    url = "https://transparency.entsog.eu/api/v1/operationalData"

    if point_key is not None and operator_key is None:
        raise ValueError("Needs to specify operator_key when point_key is given.")

    # Can only do one operator at a time
    if operator_key is not None and len(set(to_list(operator_key))) > 1:
        logger.info("Splitting by operator")
        splitted = split(point_key, operator_key)
        result = []
        for operator_key, point_keys in tqdm(splitted.items()):
            r = get_physical_flows(operator_key=operator_key,
                                   point_key=point_keys,
                                   direction=direction,
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
        splitted = split(dates, dates_group)
        result = []
        for dates in splitted.values():
            r = get_physical_flows(operator_key=operator_key,
                                   point_key=point_key,
                                   direction=direction,
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
        "indicator": "Physical Flow",
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

    if direction is not None:
        params['directionKey'] = direction

    d = api_req(url, params=params, limit=limit)

    if d is None or not d.get("operationalData"):
        return None

    df = pd.DataFrame(d.get("operationalData"))
    df["value"] = pd.to_numeric(df.value)
    df["isCmpRelevant"] = df["isCmpRelevant"].astype('bool')
    df["isCamRelevant"] = df["isCamRelevant"].astype('bool')
    df["periodFrom"] = pd.to_datetime(df.periodFrom, errors='coerce')
    df["periodTo"] = pd.to_datetime(df.periodTo)
    df["date"] = df.periodFrom.apply(lambda x: x.date())

    # IMPORTANT
    # ENTSOG has duplicated records
    df = df.drop_duplicates()
    return df


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

    d = api_req(url, params=params, limit=limit)

    if d is None:
        return None

    df = pd.DataFrame(d.get("AggregatedData"))
    df["value"] = pd.to_numeric(df.value)
    df["periodFrom"] = pd.to_datetime(df.periodFrom)
    df["periodTo"] = pd.to_datetime(df.periodTo)
    df["date"] = df.periodFrom.dt.date
    df = df.drop_duplicates()
    return df


def fix_opd_countries(opd):
    # Some adjacentCountries are wrong in opd data or simply missing
    # We trust interconnections better (e.g. it includes Algeria and Albania)
    # Also, we use tsoCountry by default but use IC to update it when available
    len_before = len(opd)

    # Fixing adjacent country for EU - Non EU using interconnections
    ic = get_interconnections()
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
    opd.loc[(opd.pointKey=='ITP-00008') & (opd.operatorKey=='AL-TSO-0001'), 'country'] = 'GR'
    opd.loc[(opd.pointKey == 'ITP-00008') & (opd.operatorKey == 'IT-TSO-0001'), 'partner'] = 'GR'


    # LNG marked as LNG
    # Zeebrugge
    opd.loc[(opd.pointKey == 'LNG-00017') & (opd.directionKey == 'entry'), 'partner'] = 'lng'

    # BE - LU
    opd.loc[opd.pointKey == 'ITP-00113']


    len_after = len(opd)
    assert len_after == len_before

    return opd


# def get_flows_by_pointtype(date_from='2022-01-01',
#                               date_to=dt.date.today(),
#                               country_iso2=None,
#                               remove_pipe_in_pipe=True,
#                               remove_operators=[],
#                               remove_point_labels=[]):
#     """
#     Mainly to debug/understand why germany so low on consumption + distribution
#     :param date_from:
#     :param date_to:
#     :param country_iso2:
#     :param remove_pipe_in_pipe:
#     :param remove_operators:
#     :param remove_point_labels:
#     :return:
#     """
#     opd = get_operator_point_directions()
#     opd = fix_opd_countries(opd)
#
#     if country_iso2:
#         opd = opd.loc[opd.country.isin(to_list(country_iso2))]
#
#     if remove_pipe_in_pipe:
#         opd = opd.loc[opd.isPipeInPipe.isnull() | ~opd.isPipeInPipe \
#                       | (opd.isPipeInPipe & opd.isDoubleReporting.isnull())]
#
#     if remove_operators:
#         opd = opd.loc[~opd.operatorKey.isin(to_list(remove_operators))]
#
#     if remove_point_labels:
#         opd = opd.loc[~opd.pointLabel.isin(to_list(remove_point_labels))]
#
#     def keep_unique(x):
#         return x[['pointKey', 'operatorKey']].drop_duplicates()
#
#     def add_countries(x):
#         if x is None:
#             return None
#
#         return x.merge(
#             opd[['pointKey', 'operatorKey', 'directionKey', 'country', 'partner']] \
#                 .drop_duplicates())
#
#     flows = get_physical_flows(
#         operator_key=opd.operatorKey.to_list(),
#         point_key=opd.pointKey.to_list(),
#         direction=None,
#         date_from=to_datetime(date_from),
#         date_to=to_datetime(date_to),
#     )
#
#     flows = add_countries(flows)
#     flows['value_m3'] = flows.value / base.GCV_KWH_PER_M3
#
#     flows_sum = flows.groupby(['directionKey', 'pointType', 'country', 'partner']) \
#         .aggregate({'value_m3': 'sum'}) \
#         .reset_index()
#
#     # flows_within_country = flows_sum[flows_sum.country == flows_sum.partner]
#     flows_sum.sort_values(['value_m3'], inplace=True, ascending=False)
#     flows_sum.value_m3.sum() / 1e9
#     return flows_sum


def get_flows_raw(date_from='2022-01-01',
                  date_to=dt.date.today(),
                  country_iso2=None,
                  remove_pipe_in_pipe=True,
                  remove_operators=[],
                  remove_point_labels=[],
                  remove_point_ids=[],
                  # remove_point_labels=['Dornum GASPOOL',
                  #                      'VIP Waidhaus NCG',
                  #                      'Haiming 2 7F/bn'],
                  # remove_point_ids=['5DE-TSO-0016ITP-00452exitCZ-TSO-0001',
                  #                   '5DE-TSO-0016ITP-00452entryCZ-TSO-0001'],
                  use_csv_selection=True):

    opd = get_operator_point_directions()
    opd = fix_opd_countries(opd)

    if country_iso2:
        opd = opd.loc[opd.country.isin(to_list(country_iso2))]

    if use_csv_selection:
        to_remove = pd.read_csv('assets/entsog/opd_to_remove.csv')
        # Do an antijoin
        outer_join = opd.merge(to_remove, how='outer', indicator=True)
        opd = outer_join[(outer_join._merge == 'left_only')].drop('_merge', axis=1)

    if remove_pipe_in_pipe:
        opd = opd.loc[opd.isPipeInPipe.isnull() |  ~opd.isPipeInPipe \
         | (opd.isPipeInPipe & opd.isDoubleReporting.isnull())]

    if remove_operators:
        opd = opd.loc[~opd.operatorKey.isin(to_list(remove_operators))]

    if remove_point_labels:
        opd = opd.loc[~opd.pointLabel.isin(to_list(remove_point_labels))]

    if remove_point_ids:
        opd = opd.loc[~opd.id.isin(to_list(remove_point_ids))]


    entry_points = opd.loc[
        opd.pointType.str.contains('Cross-Border Transmission') \
        & (opd.directionKey == 'entry')]

    storage_entry_points = opd.loc[
        opd.pointType.str.startswith('Storage') \
        & (opd.directionKey == 'entry')]

    lng_entry_points = opd.loc[
        opd.pointType.str.contains('LNG Entry point') \
        & (opd.directionKey == 'entry')]

    transmission_entry_points = opd.loc[
        opd.pointType.str.startswith('Transmission') \
        & (opd.directionKey == 'entry')]

    production_points = opd.loc[
        opd.pointType.str.contains('production') \
        & (opd.directionKey == 'entry')]

    consumption_points = opd.loc[
        opd.pointType.str.contains('Consumers') \
        & (opd.directionKey == 'exit')]

    transmission_exit_points = opd.loc[
        opd.pointType.str.startswith('Transmission') \
        & (opd.directionKey == 'exit')]

    distribution_points = opd.loc[
        opd.pointType.str.contains('Distribution') \
        & (opd.directionKey == 'exit')]

    exit_points = opd.loc[
        opd.pointType.str.contains('Cross-Border Transmission') \
        & (opd.directionKey == 'exit')]

    storage_exit_points = opd.loc[
        opd.pointType.str.startswith('Storage') \
        & (opd.directionKey == 'exit')]

    lng_exit_points = opd.loc[
        opd.pointType.str.contains('LNG Entry point') \
        & (opd.directionKey == 'exit')]

    # Check all are uniques
    all = pd.concat([entry_points,
               storage_entry_points,
               lng_entry_points,
               transmission_entry_points,
               production_points,
               consumption_points,
               transmission_exit_points,
               distribution_points,
               exit_points,
               storage_exit_points,
               lng_exit_points
                     ], axis=0)
    assert len(all.index) == len(all.index.unique())

    def keep_unique(x):
        return x[['pointKey', 'operatorKey']].drop_duplicates()

    def add_countries(x):
        if x is None:
            return None

        return x.merge(
            opd[['pointKey', 'operatorKey', 'directionKey', 'country', 'partner']] \
                .drop_duplicates())

    #########################
    # Get raw flow data
    #########################
    flows_import_raw = get_physical_flows(
        operator_key=keep_unique(entry_points).operatorKey.to_list(),
        point_key=keep_unique(entry_points).pointKey.to_list(),
        direction="entry",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    flows_import_lng_raw = get_physical_flows(
        operator_key=keep_unique(lng_entry_points).operatorKey.to_list(),
        point_key=keep_unique(lng_entry_points).pointKey.to_list(),
        direction="entry",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    flows_production_raw = get_physical_flows(
        operator_key=keep_unique(production_points).operatorKey.to_list(),
        point_key=keep_unique(production_points).pointKey.to_list(),
        direction="entry",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    flows_consumption_raw = get_physical_flows(
        operator_key=keep_unique(consumption_points).operatorKey.to_list(),
        point_key=keep_unique(consumption_points).pointKey.to_list(),
        direction="exit",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    flows_distribution_raw = get_physical_flows(
        operator_key=keep_unique(distribution_points).operatorKey.to_list(),
        point_key=keep_unique(distribution_points).pointKey.to_list(),
        direction="exit",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    flows_export_raw = get_physical_flows(
        operator_key=keep_unique(exit_points).operatorKey.to_list(),
        point_key=keep_unique(exit_points).pointKey.to_list(),
        direction="exit",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    flows_export_lng_raw = get_physical_flows(
        operator_key=keep_unique(lng_exit_points).operatorKey.to_list(),
        point_key=keep_unique(lng_exit_points).pointKey.to_list(),
        direction="exit",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    flows_storage_entry_raw = get_physical_flows(
        operator_key=keep_unique(storage_entry_points).operatorKey.to_list(),
        point_key=keep_unique(storage_entry_points).pointKey.to_list(),
        direction="entry",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    flows_storage_exit_raw = get_physical_flows(
        operator_key=keep_unique(storage_exit_points).operatorKey.to_list(),
        point_key=keep_unique(storage_exit_points).pointKey.to_list(),
        direction="exit",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    flows_transmission_entry_raw = get_physical_flows(
        operator_key=keep_unique(transmission_entry_points).operatorKey.to_list(),
        point_key=keep_unique(transmission_entry_points).pointKey.to_list(),
        direction="entry",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    flows_transmission_exit_raw = get_physical_flows(
        operator_key=keep_unique(transmission_exit_points).operatorKey.to_list(),
        point_key=keep_unique(transmission_exit_points).pointKey.to_list(),
        direction="exit",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    return (add_countries(flows_import_raw),
            add_countries(flows_import_lng_raw),
            add_countries(flows_export_raw),
            add_countries(flows_export_lng_raw),
            add_countries(flows_production_raw),
            add_countries(flows_consumption_raw),
            add_countries(flows_distribution_raw),
            add_countries(flows_storage_entry_raw),
            add_countries(flows_storage_exit_raw),
            add_countries(flows_transmission_entry_raw),
            add_countries(flows_transmission_exit_raw),
            )


def process_non_crossborder_flows(flows_distribution_raw,
                                  flows_consumption_raw,
                                  flows_storage_entry_raw,
                                  flows_storage_exit_raw,
                                  flows_transmission_entry_raw,
                                  flows_transmission_exit_raw
                            ):

    flows_distribution_raw['type'] = base.ENTSOG_DISTRIBUTION
    flows_consumption_raw['type'] = base.ENTSOG_CONSUMPTION
    flows_storage_entry_raw['type'] = base.ENTSOG_STORAGE_ENTRY
    flows_storage_exit_raw['type'] = base.ENTSOG_STORAGE_EXIT
    flows_transmission_entry_raw['type'] = base.ENTSOG_TRANSMISSION_ENTRY
    flows_transmission_exit_raw['type'] = base.ENTSOG_TRANSMISSION_EXIT

    flows = pd.concat([flows_distribution_raw,
                       flows_consumption_raw,
                       flows_storage_entry_raw,
                       flows_storage_exit_raw,
                       flows_transmission_entry_raw,
                       flows_transmission_exit_raw
                       ],
                      axis=0) \
        .groupby(['country', 'partner', 'date', 'type']) \
        .agg(value=('value', np.nansum)) \
        .reset_index() \
        .rename(columns={'country': 'destination_iso2',
                         'partner': 'departure_iso2',
                         'value': 'value_kwh'}) \
        .reset_index()

    return flows


def process_crossborder_flows(flows_import_raw,
                                  flows_import_lng_raw,
                                  flows_export_raw,
                                  flows_export_lng_raw,
                                  flows_production_raw,
                                  keep_confirmed_only=False,
                                  auto_confirmed_only=True,
                                  flows_import_agg_cols=None,
                                  flows_export_agg_cols=None,
                                  remove_operators=[],
                                  save_intermediary_to_file=False,
                                  intermediary_filename=None,
                                  save_to_file=False,
                                  filename=None):

    flows_import = flows_import_raw

    # Adding LNG
    if flows_import_lng_raw is not None:
        flows_import_lng = flows_import_lng_raw
        flows_import_lng['partner'] = 'lng'  # Making LNG a country
        flows_import = pd.concat([flows_import, flows_import_lng], axis=0)

    # Adding Production
    if flows_production_raw is not None:
        flows_production = flows_production_raw
        flows_import = pd.concat([flows_import, flows_production], axis=0)

    if keep_confirmed_only:
        flows_import = flows_import.loc[flows_import.flowStatus == 'Confirmed']

    if flows_export_raw is None:
        flows_export_raw = pd.DataFrame({'pointKey': pd.Series(dtype='str'),
                                         'operatorKey': pd.Series(dtype='int'),
                                         'value': pd.Series(dtype='float'),
                                         'date': pd.Series(dtype='str'),
                                         'type': pd.Series(dtype='str')})

    flows_export = flows_export_raw

    if flows_export_lng_raw is not None:
        flows_export_lng = flows_export_lng_raw
        flows_export_lng['country'] = 'lng'  # Making LNG a country
        flows_export = pd.concat([flows_export, flows_export_lng], axis=0)

    if flows_import_agg_cols:
        flows_import = flows_import.groupby(flows_import_agg_cols, dropna=False) \
            .agg(value=('value', np.nanmean)).reset_index()

    if flows_export_agg_cols:
        flows_export = flows_export.groupby(flows_export_agg_cols, dropna=False) \
            .agg(value=('value', np.nanmean)).reset_index()

    flows = flows_import.merge(flows_export,
                               left_on=['pointKey', 'date', 'country', 'partner'],
                               right_on=['pointKey', 'date', 'partner', 'country'],
                               how='outer',
                               suffixes=['_import', '_export']
                               )

    if remove_operators:
        flows = flows.loc[~flows.operatorKey_import.isin(remove_operators)]

    def process_pt_op_date(df):

        df = df.loc[(df.value_import > 0) |
                    # Or an export point for which we don't have import
                    (pd.isna(df.value_import) & (df.value_export > 0))]

        if auto_confirmed_only and 'Confirmed' in df.flowStatus_import.to_list():
            if (np.nansum(df.value_import) == 0) \
                        or (np.std(df.value_import) / np.nanmean(df.value_import)) < 0.2:
                df = df.loc[df.flowStatus_import == 'Confirmed']
            else:
                logger.warning("Several unmatching import flows")
                df = df.loc[df.flowStatus_import == 'Confirmed']

        if auto_confirmed_only and 'Confirmed' in df.flowStatus_export.to_list():
            if (np.nansum(df.value_export) == 0) \
                    or (np.std(df.value_export) / np.nanmean(df.value_export)) < 0.2:
                df = df.loc[df.flowStatus_export == 'Confirmed']
            else:
                if np.nansum(df.value_import) > 0:
                    # Take the one closest to import
                    df['diff'] = abs(df.value_export-df.value_import)
                    df = df.sort_values('diff', ascending=True).head(1).drop('diff', axis=1)
                else:
                    logger.warning("Several unmatching export flows")

        if len(df) == 0:
            return df

        # This function only manages the case were one import
        # is matching several exports
        # Or when there are only exports
        if not len(df[['pointKey', 'flowStatus_import', 'value_import']].drop_duplicates()) == 1:

            # Values have probably been updated later on
            # and both are confirmed
            if (np.std(df.value_import) / np.nanmean(df.value_import)) < 0.1 \
                    and all(df.value_export.isnull()):
                df = df.groupby([x for x in df.columns if x not in ['value_import', 'value_export']],
                                dropna=False) \
                    .agg(value_import=('value_import', np.nanmean),
                         value_export=('value_export', np.nanmean)) \
                    .reset_index()
            else:
                logger.warning("Flows are mismanaged")

        # Further checks
        if not all(df.country_export.isnull() | df.country_import.isnull() | (df.country_export == df.partner_import)):
            logger.warning("flows aren't matching")
            print(df[['pointKey', 'operatorKey_import', 'date']])

        # Keep only confirmed if it is there
        # One import was mathching two exports

        def nanmean(x):
            #Without warning if empty
            return np.NaN if np.all(x!=x) else np.nanmean(x)

        value_import = nanmean(df.value_import)
        value_export_sum = np.nansum(df.value_export)

        if value_export_sum > 0 and not pd.isna(value_import):
            df['value'] = df.value_export / value_export_sum * value_import / len(df)
        elif all(df.value_export.isnull()):
            df['value'] = value_import / len(df) # spread equally
        elif all(df.value_import.isnull()):
            df['value'] = df.value_export
        else:
            df['value'] = 0

        df['partner'] = np.where(df['country_export'].isnull(), df['partner_import'], df['country_export'])
        df['country'] = np.where(df['country_import'].isnull(), df['partner_export'], df['country_import'])
        return df.reset_index()

    flows_scaled = flows.groupby(['pointKey', 'operatorKey_import', 'date'],
                                 dropna=False).progress_apply(process_pt_op_date)
    flows_scaled = flows_scaled.reset_index(drop=True)

    if save_intermediary_to_file:
        intermediary_filename = intermediary_filename or "entsog_flows_intermediary.csv"
        flows_scaled.to_csv(intermediary_filename, index=False)

    flows_agg = flows_scaled.groupby(['country', 'partner', 'date']) \
        .agg(value=('value', np.nansum)) \
        .reset_index() \
        .rename(columns={'country': 'to_country',
                         'partner': 'from_country'}) \
        .reset_index()

    flows_agg.rename(columns={'from_country': 'departure_iso2',
                              'to_country': 'destination_iso2',
                              'value': 'value_kwh'},
                     inplace=True)

    flows_agg.loc[
        flows_agg.departure_iso2 != flows_agg.destination_iso2, 'type'] = base.ENTSOG_CROSSBORDER
    flows_agg.loc[
        flows_agg.departure_iso2 == flows_agg.destination_iso2, 'type'] = base.ENTSOG_PRODUCTION

    if save_to_file:
        filename = filename or "entsog_flows.csv"
        flows_agg.to_csv(filename, index=False)
    return flows_agg


def fix_kipi_flows(flows):
    # Bruegel: Finally, on Turkey, our assumption was to attribute:
    # • All of Kipi to Azerbaijan,
    # • All of Strandzha to Russia.
    # -> we remove TR -> GR
    idx = (flows.departure_iso2 == 'TR') & (flows.destination_iso2 == 'GR')
    flows.loc[idx, 'departure_iso2'] = 'AZ'
    return flows


def get_flows(date_from='2022-01-01',
              date_to=dt.date.today(),
              country_iso2=None,
              remove_pipe_in_pipe=True,
              save_intermediary_to_file=False,
              intermediary_filename=None,
              save_to_file=False,
              filename=None):

    # Get raw information from ENTSOG
    (flows_import_raw,
     flows_import_lng_raw,
     flows_export_raw,
     flows_export_lng_raw,
     flows_production_raw,
     flows_consumption_raw,
     flows_distribution_raw,
     flows_storage_entry_raw,
     flows_storage_exit_raw,
     flows_transmission_entry_raw,
     flows_transmission_exit_raw) = get_flows_raw(date_from=date_from,
                                                       date_to=date_to,
                                                       country_iso2=country_iso2,
                                                       remove_pipe_in_pipe=remove_pipe_in_pipe)

    # Process cross border & production
    flows_crossborder = process_crossborder_flows(flows_import_raw=flows_import_raw,
                                              flows_export_raw=flows_export_raw,
                                              flows_import_lng_raw=flows_import_lng_raw,
                                              flows_export_lng_raw=flows_export_lng_raw,
                                              flows_production_raw=flows_production_raw,
                                              keep_confirmed_only=False,
                                              auto_confirmed_only=True,
                                              remove_operators=[],
                                              save_intermediary_to_file=save_intermediary_to_file,
                                              intermediary_filename=intermediary_filename,
                                              save_to_file=save_to_file,
                                              filename=filename)

    # Consumption and distribution
    flows_cons_dist = process_non_crossborder_flows(flows_distribution_raw=flows_distribution_raw,
                                                    flows_consumption_raw=flows_consumption_raw,
                                                    flows_storage_entry_raw=flows_storage_entry_raw,
                                                    flows_storage_exit_raw=flows_storage_exit_raw,
                                                    flows_transmission_entry_raw=flows_transmission_entry_raw,
                                                    flows_transmission_exit_raw=flows_transmission_exit_raw)


    flows = pd.concat([flows_crossborder,
                       flows_cons_dist], axis=0)

    flows = fix_kipi_flows(flows)

    flows['value_m3'] = flows.value_kwh / base.GCV_KWH_PER_M3
    flows['value_tonne'] = flows.value_kwh / base.GCV_KWH_PER_M3 * base.KG_PER_M3 / 1000
    flows['value_mwh'] = flows.value_kwh / 1000
    flows['commodity'] = 'natural_gas'

    flows.drop(['value_kwh'], axis=1, inplace=True)
    flows.drop(['index'], axis=1, inplace=True)
    flows.replace({'departure_iso2': {'UK': 'GB'},
                   'destination_iso2': {'UK': 'GB'}},
                  inplace=True)

    return flows


def update(date_from=-7, date_to=dt.date.today(), filename=None, save_to_file=True, nodata_error_date_from=None):
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
                              save_to_file=save_to_file,
                              filename=filename)
        except TypeError:
            logger.warning("ENTSOG failed. Trying again")
            continue

    if flows is None:
        logger_slack.error("Failed to get ENTSOG data")
        raise ValueError("Failed to get ENTSOG data.")

    flows = flows[['commodity', 'departure_iso2', 'destination_iso2', 'date',
                   'value_tonne', 'value_mwh', 'value_m3', 'type']]

    upsert(df=flows, table=DB_TABLE_ENTSOGFLOW, constraint_name="unique_entsogflow")

    # Raise alert if no recent data was found
    if nodata_error_date_from is not None and flows.date.max() < to_datetime(nodata_error_date_from).date():
        logger_slack.error("No ENTSOG flow found after %s (most recent is %s)" % (to_datetime(nodata_error_date_from).date(), flows.date.max()))
