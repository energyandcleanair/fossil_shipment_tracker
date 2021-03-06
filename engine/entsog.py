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


import datetime as dt
import pandas as pd
import numpy as np
from collections import defaultdict
from tqdm import tqdm
import sqlalchemy as sa
import requests

import base
from base.db import session
from base.logger import logger, logger_slack
from base.utils import to_list, to_datetime
from base.db_utils import upsert
from base.models import DB_TABLE_ENTSOGFLOW, EntsogFlow


def split(x, f):
    res = defaultdict(list)
    for v, k in zip(x, f):
        res[k].append(v)
    return res


def api_req(url, params={}, limit=-1):
    params['limit'] = limit
    api_result = requests.get(url, params=params)

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
    dates_group = [x.year for x in dates]

    if len(set(dates_group)) > 1:
        logger.info("Splitting by dates")
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

    len_after = len(opd)
    assert len_after == len_before
    return opd


def get_crossborder_flows_raw(date_from='2022-01-01',
                              date_to=dt.date.today(),
                              country_iso2=None,
                              remove_pipe_in_pipe=True,
                              remove_operators=[],
                              remove_point_labels=['Dornum GASPOOL',
                                                   'VIP Waidhaus NCG',
                                                   'Haiming 2 7F/bn']
                              ):

    opd = get_operator_point_directions()
    opd = fix_opd_countries(opd)

    if country_iso2:
        opd = opd.loc[opd.country.isin(to_list(country_iso2))]

    if remove_pipe_in_pipe:
        opd = opd.loc[opd.isPipeInPipe.isnull() | ~opd.isPipeInPipe \
         | (opd.isPipeInPipe & opd.isDoubleReporting.isnull())]

    if remove_operators:
        opd = opd.loc[~opd.operatorKey.isin(to_list(remove_operators))]

    if remove_point_labels:
        opd = opd.loc[~opd.pointLabel.isin(to_list(remove_point_labels))]

    entry_points = opd.loc[
        opd.pointType.str.contains('Cross-Border') \
        & (opd.directionKey == 'entry')]

    lng_entry_points = opd.loc[
        opd.pointType.str.contains('LNG Entry point') \
        & (opd.directionKey == 'entry')]

    production_points = opd.loc[
        opd.pointType.str.contains('production') \
        & (opd.directionKey == 'entry')]

    exit_points = opd.loc[
        opd.pointType.str.contains('Cross-Border') \
        & (opd.directionKey == 'exit')]

    lng_exit_points = opd.loc[
        opd.pointType.str.contains('LNG Entry point') \
        & (opd.directionKey == 'exit')]

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

    return (add_countries(flows_import_raw),
            add_countries(flows_import_lng_raw),
            add_countries(flows_export_raw),
            add_countries(flows_export_lng_raw),
            add_countries(flows_production_raw))


def process_crossborder_flows_raw(flows_import_raw,
                                  flows_import_lng_raw,
                                  flows_export_raw,
                                  flows_export_lng_raw,
                                  flows_production_raw,
                                  keep_confirmed_only=False,
                                  auto_confirmed_only=True,
                                  flows_import_agg_cols=None,
                                  flows_export_agg_cols=None,
                                  remove_operators=[],
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
                                         'date': pd.Series(dtype='str')})

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
        df = df.loc[df.value_import > 0]

        if auto_confirmed_only and 'Confirmed' in df.flowStatus_import.to_list():
            df = df.loc[df.flowStatus_import == 'Confirmed']

        if len(df) == 0:
            return df

        # This function only manages the case were one import
        # is matching several exports
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
        if not all(df.country_export.isnull() | (df.country_export == df.partner_import)):
            logger.warning("flows aren't matching")
            print(df[['pointKey', 'operatorKey_import', 'date']])

        # Keep only confirmed if it is there
        # One import was mathching two exports
        value_import = np.nanmean(df.value_import)
        value_export_sum = np.nansum(df.value_export)
        if value_export_sum > 0:
            df['value'] = df.value_export / value_export_sum * value_import / len(df)
        elif all(df.value_export.isnull()):
            df['value'] = value_import / len(df) # spread equally
        else:
            df['value'] = 0

        df['partner'] = np.where(df['country_export'].isnull(), df['partner_import'], df['country_export'])
        return df.reset_index()

    flows_scaled = flows.groupby(['pointKey', 'operatorKey_import', 'date']).apply(process_pt_op_date)
    flows_scaled = flows_scaled.reset_index(drop=True)
    flows_agg = flows_scaled.groupby(['country_import', 'partner', 'date']) \
        .agg(value=('value', np.nansum)) \
        .reset_index() \
        .rename(columns={'country_import': 'to_country',
                         'partner': 'from_country'}) \
        .reset_index()

    if save_to_file:
        filename = filename or "entsog_flows.csv"
        flows_agg.to_csv(filename, index=False)
    return flows_agg


def fix_kipi_flows(flows):
    # Bruegel: Finally, on Turkey, our assumption was to attribute:
    # ??? All of Kipi to Azerbaijan,
    # ??? All of Strandzha to Russia.
    # -> we remove TR -> GR
    idx = (flows.departure_iso2 == 'TR') & (flows.destination_iso2 == 'GR')
    flows.loc[idx, 'departure_iso2'] = 'AZ'
    return flows


def get_crossborder_flows(date_from='2022-01-01',
                          date_to=dt.date.today(),
                          country_iso2=None,
                          remove_pipe_in_pipe=True,
                          save_to_file=False,
                          filename=None):
    (flows_import_raw,
     flows_import_lng_raw,
     flows_export_raw,
     flows_export_lng_raw,
     flows_production_raw) = get_crossborder_flows_raw(date_from=date_from,
                                                       date_to=date_to,
                                                       country_iso2=country_iso2,
                                                       remove_pipe_in_pipe=remove_pipe_in_pipe)

    ##########################
    # Combine, aggregate, etc
    ##########################
    flows_agg = process_crossborder_flows_raw(flows_import_raw=flows_import_raw,
                                              flows_export_raw=flows_export_raw,
                                              flows_import_lng_raw=flows_import_lng_raw,
                                              flows_export_lng_raw=flows_export_lng_raw,
                                              flows_production_raw=flows_production_raw,
                                              keep_confirmed_only=False,
                                              auto_confirmed_only=True,
                                              remove_operators=[],
                                              save_to_file=save_to_file,
                                              filename=filename)
    return flows_agg


def get_flows(date_from="2021-01-01", date_to=dt.date.today(), save_to_file=False, filename=None):

    flows = get_crossborder_flows(date_from=date_from,
                                  date_to=date_to,
                                  save_to_file=save_to_file,
                                  filename=filename)

    flows = flows.rename(columns={'from_country': 'departure_iso2',
                          'to_country': 'destination_iso2',
                          'value': 'value_kwh'})

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

    flows = get_flows(date_from=date_from,
                      date_to=date_to,
                      save_to_file=save_to_file,
                      filename=filename)

    upsert(df=flows, table=DB_TABLE_ENTSOGFLOW, constraint_name="unique_entsogflow")

    # Raise alert if no recent data was found
    if nodata_error_date_from is not None and flows.date.max() < to_datetime(nodata_error_date_from).date():
        logger_slack.error("No ENTSOG flow found after %s (most recent is %s)" % (to_datetime(nodata_error_date_from).date(), flows.date.max()))
