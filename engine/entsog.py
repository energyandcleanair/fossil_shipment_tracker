import datetime as dt
import pandas as pd
import numpy as np
from collections import defaultdict
from tqdm import tqdm

import base

from base.db import session
from base.logger import logger
from base.utils import to_list, to_datetime
from base.db_utils import upsert
from base.models import DB_TABLE_ENTSOGFLOW
from engine.datalastic import Datalastic
from engine.marinetraffic import Marinetraffic

import sqlalchemy as sa
import requests


def split(x, f):
    res = defaultdict(list)
    for v, k in zip(x, f):
        res[k].append(v)
    return res


def api_req(url, params={}, limit=-1):
    # logger.info(url)
    params['limit'] = limit
    api_result = requests.get(url, params=params)

    if api_result.status_code != 200:
        logger.warning("ENTSOG: Failed to query entsog %s %s" % (url, params))
        return None

    res = api_result.json()

    if res == {'message': 'No Data Available'}:
        return None

    try:
        if res["meta"]["total"] > res["meta"]["count"] * 1.2:
            # +2: for some reason, sometimes total is +1 or +2
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

    if d is None:
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


def get_crossborder_flows_raw(date_from='2022-01-01',
                          date_to=dt.date.today(),
                          country_iso2=None,
                          remove_pipe_in_pipe=True):

    opd = get_operator_point_directions()
    ic = get_interconnections()

    if country_iso2:
        opd = opd.loc[opd.tSOCountry.isin(to_list(country_iso2))]

    if remove_pipe_in_pipe:
        opd = opd.loc[opd.isPipeInPipe.isnull() | ~opd.isPipeInPipe |
                        (opd.isPipeInPipe & opd.isDoubleReporting.isnull())]


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
            opd[['pointKey', 'operatorKey', 'directionKey', 'tSOCountry', 'adjacentCountry']] \
                .drop_duplicates() \
                .rename(columns={'tSOCountry': 'country',
                                 'adjacentCountry': 'partner'}))

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


    # ic_import_lng = ic.loc[ic.fromInfrastructureTypeLabel.notnull() \
    #                        & ic.fromInfrastructureTypeLabel.str.contains('LNG Terminals') \
    #                        & (~ic.toPointKey.isnull())]

    # lng_entry_points_unique = lng_entry_points[['pointKey', 'operatorKey']].drop_duplicates()

    flows_import_lng_raw = get_physical_flows(
        operator_key=keep_unique(lng_entry_points).operatorKey.to_list(),
        point_key=keep_unique(lng_entry_points).pointKey.to_list(),
        direction="entry",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    # production_points_unique = production_points[['pointKey', 'operatorKey']].drop_duplicates()

    flows_production_raw = get_physical_flows(
        operator_key=keep_unique(production_points).operatorKey.to_list(),
        point_key=keep_unique(production_points).pointKey.to_list(),
        direction="entry",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    # Export side
    # ic_crossborder_export = ic.loc[(ic.toCountryKey != ic.fromCountryKey) & \
    #                                (~ic.toOperatorKey.isnull())][['toOperatorKey', 'toPointKey']] \
    #     .drop_duplicates()
    #
    # if remove_pipe_in_pipe:
    #     keep = opd.loc[(opd.directionKey == 'exit') & \
    #                    (opd.isPipeInPipe.isnull() | ~opd.isPipeInPipe |
    #                     (opd.isPipeInPipe & opd.isDoubleReporting.isnull()))][['operatorKey', 'pointKey']] \
    #         .rename(columns={'operatorKey': 'toOperatorKey',
    #                          'pointKey': 'toPointKey'}) \
    #         .drop_duplicates()
    #     ic_crossborder_export = ic_crossborder_export.merge(keep, how='inner')

    flows_export_raw = get_physical_flows(
        operator_key=keep_unique(exit_points).operatorKey.to_list(),
        point_key=keep_unique(exit_points).pointKey.to_list(),
        direction="exit",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    # ic_export_lng = ic.loc[ic.toInfrastructureTypeLabel.notnull() \
    #                        & ic.toInfrastructureTypeLabel.str.contains('LNG Terminals')]

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


def process_crossborder_flows_raw(ic,
                                  opd,
                                  flows_import_raw,
                                  flows_import_lng_raw,
                                  flows_export_raw,
                                  flows_export_lng_raw,
                                  flows_production_raw,
                                  keep_opd_only=False, keep_confirmed_only=False, auto_confirmed_only=True,
                                  flows_import_agg_cols=None,
                                  flows_export_agg_cols=None,
                                  flows_import_on=['importPointKey', 'date', 'importOperatorKey', 'exportOperatorKey'],
                                  flows_export_on=['exportPointKey', 'date', 'importOperatorKey', 'exportOperatorKey'],
                                  remove_operators=[],
                                  save_to_file=False,
                                  filename=None):


    # points_import = ic[
    #     ["toPointKey", "toOperatorKey", "toCountryLabel", 'fromCountryKey', "fromCountryLabel", 'fromOperatorKey']] \
    #     .rename(columns={'toPointKey': 'pointKey',
    #                      'toOperatorKey': 'operatorKey',
    #                      'toCountryLabel': 'country',
    #                      'fromCountryKey': 'adjacentCountry',
    #                      'fromCountryLabel': 'partner',
    #                      'fromOperatorKey': 'partner_operatorKey',
    #                      }) \
    #     [['pointKey', 'operatorKey', 'country', 'partner']] \
    #     .drop_duplicates()




    # if keep_opd_only:
    #     # Try to only keep feasible connections
    #     points_import = points_import.merge(opd[['pointKey', 'operatorKey', 'adjacentCountry']].drop_duplicates(),
    #                                         how='inner')

    flows_import = flows_import_raw
                        # pd.merge(flows_import_raw, points_import,
                        #     on=['pointKey', 'operatorKey'],
                        #     how='left')

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

    # points_export = ic[["fromPointKey", "fromOperatorKey", "fromCountryLabel", "toCountryLabel"]] \
    #     .drop_duplicates() \
    #     .rename(columns={'fromPointKey': 'pointKey',
    #                      'fromOperatorKey': 'operatorKey',
    #                      'fromCountryLabel': 'country',
    #                      'toCountryLabel': 'partner'
    #                      }) \
    #     [['pointKey', 'operatorKey', 'country', 'partner']] \
    #     .drop_duplicates()

    if flows_export_raw is None:
        flows_export_raw = pd.DataFrame({'pointKey': pd.Series(dtype='str'),
                                         'operatorKey': pd.Series(dtype='int'),
                                         # 'tsoItemIdentifier': pd.Series(dtype='str'),
                                         'value': pd.Series(dtype='float'),
                                         'date': pd.Series(dtype='str')})

    flows_export = flows_export_raw
    # pd.merge(flows_export_raw, points_export,
    #                         on=['pointKey', 'operatorKey'],
    #                         how='left')

    if flows_export_lng_raw is not None:
        flows_export_lng = flows_export_lng_raw
        # pd.merge(flows_export_lng_raw, points_export,
        #                             on=['pointKey', 'operatorKey'],
        #                             how='left')

        flows_export_lng['country'] = 'lng'  # Making LNG a country
        flows_export = pd.concat([flows_export, flows_export_lng], axis=0)

    if flows_import_agg_cols:
        flows_import = flows_import.groupby(flows_import_agg_cols, dropna=False) \
            .agg(value=('value', np.nanmean)).reset_index()

    if flows_export_agg_cols:
        flows_export = flows_export.groupby(flows_export_agg_cols, dropna=False) \
            .agg(value=('value', np.nanmean)).reset_index()

    # if join_flows_on_operator:
    #     left_on = ['pointKey', 'date', 'partner', 'partner_operatorKey']
    #     right_on = ['pointKey', 'date', 'country', 'operatorKey']
    # else:
    #     left_on = ['pointKey', 'date', 'partner']
    #     right_on = ['pointKey', 'date', 'country']
    flows_import_cols = ['pointKey', 'operatorKey', 'tsoItemIdentifier', 'value', 'date', 'country', 'partner',
                         'partner_operatorKey', 'flowStatus'] \
        if not flows_import_agg_cols else [*flows_import_agg_cols, 'value']
    flows_export_cols = ['pointKey', 'operatorKey', 'tsoItemIdentifier', 'value', 'date', 'country', 'partner',
                         'flowStatus'] \
        if not flows_export_agg_cols else [*flows_export_agg_cols, 'value']


    # Make point keys match
    # a = flows_import.rename(columns={'pointKey': 'importPointKey',
    #                             'operatorKey': 'importOperatorKey',
    #                                  'date': 'importDate'})
    #
    # b = flows_export.rename(columns={'pointKey': 'exportPointKey',
    #                             'operatorKey': 'exportOperatorKey',
    #                                  'date': 'exportDate'})
    #
    #
    # opd_merger = opd[['pointKey', 'directionKey', 'operatorKey', 'adjacentOperatorKey']]
    #
    # ic_merger = ic.loc[ic.toDirectionKey=='entry'][['toPointKey', 'toOperatorKey',
    #                                                 'fromPointKey', 'fromOperatorKey']] \
    #             .drop_duplicates() \
    #             .rename(columns={'toPointKey': 'importPointKey',
    #                              'toOperatorKey': 'importOperatorKey',
    #                              'fromPointKey': 'exportPointKey',
    #                              'fromOperatorKey': 'exportOperatorKey'})
    #
    # flows = a.merge(ic_merger,
    #         on=['importPointKey', 'importOperatorKey'],
    #         how='left') \
    #     .merge(b,
    #            left_on=['exportPointKey', 'exportOperatorKey', 'importDate', 'country', 'partner'],
    #            right_on=['exportPointKey', 'exportOperatorKey', 'exportDate', 'partner', 'country'],
    #            how='inner',
    #            suffixes=['_import', '_export'])


    flows = flows_import.merge(flows_export,
                               left_on=['pointKey', 'date', 'country', 'partner'],
                               right_on=['pointKey', 'date', 'partner', 'country'],
                               how='outer',
                               suffixes=['_import', '_export']
                               )

    # d = b.merge(ic_merger,
    #         on=['exportPointKey', 'exportOperatorKey'],
    #         how='left')

    # flows = pd.merge(
    #     flows_import[set(flows_import_cols).intersection(flows_import.columns)],
    #     flows_export[set(flows_export_cols).intersection(flows_export.columns)],
    #     left_on=flows_import_on,
    #     right_on=flows_export_on,
    #     suffixes=['_import', '_export'],
    #     how='outer') \
    #     .dropna(subset=['country_import'])

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
        # We need to
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

    # flows_scaled = flows.groupby(groupby_cols).apply(process_pt_op_date)
    flows_scaled = flows.groupby(['pointKey', 'operatorKey_import', 'date']).apply(process_pt_op_date)
    flows_scaled = flows_scaled.reset_index(drop=True)
    # flows_scaled = flows_scaled.rename(columns={'importDate':'date'})
    flows_agg = flows_scaled.groupby(['country_import', 'partner', 'date']) \
        .agg(value=('value', np.nansum)) \
        .reset_index() \
        .rename(columns={'country_import': 'to_country',
                         'partner': 'from_country'}) \
        .reset_index()

    if save_to_file:
        if not filename:
            filename = "entsog_flows.csv" #_{keep_opd_only}_{keep_confirmed_only}_{join_flows_on_operator}_{auto_confirmed_only}.csv"
        flows_agg.to_csv(filename, index=False)
    return flows_agg


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

    ic = get_interconnections()
    opd = get_operator_point_directions()

    ##########################
    # Combine, aggregate, etc
    ##########################
    keep_opd_only = False
    keep_confirmed_only = False
    join_flows_on_operator = False
    auto_confirmed_only = True
    remove_operators = []

    # remove_operators = ['DE-TSO-0009', 'DE-TSO-0002',  # FOR DOUBLE COUNTING WITH NORWAY
    #                     'DE-TSO-0016', 'DE-TSO-0017', 'DE-TSO-0018' # FOR DOUBLE COUNTING WITH RUSSIA
    #                     ]



    flows_agg = process_crossborder_flows_raw(ic=ic,
                                              opd=opd,
                                              flows_import_raw=flows_import_raw,
                                              flows_export_raw=flows_export_raw,
                                              flows_import_lng_raw=flows_import_lng_raw,
                                              flows_export_lng_raw=flows_export_lng_raw,
                                              flows_production_raw=flows_production_raw,
                                              keep_opd_only=False,
                                              keep_confirmed_only=False,
                                              auto_confirmed_only=True,
                                              remove_operators=[],
                                              save_to_file=save_to_file,
                                              filename=filename)
    return flows_agg



def get_gasprom_exports(date_from='2022-01-01', date_to=dt.date.today()):
    return


def distribute_to_producers_singledate():
    return


def get_flows(date_from="2021-01-01", date_to=dt.date.today(), save_to_file=False, filename=None):


    flows = get_crossborder_flows(date_from=date_from,
                                  date_to=date_to,
                                  save_to_file=save_to_file,
                                  filename=filename)

    flows = flows.rename(columns={'from_country':'departure_iso2',
                          'to_country':'destination_iso2',
                          'value':'value_kwh'})

    flows['value_m3'] = flows.value_kwh / base.GCV_KWH_PER_M3
    flows['value_tonne'] = flows.value_kwh / base.GCV_KWH_PER_M3 * base.KG_PER_M3 / 1000
    flows['value_mwh'] = flows.value_kwh / 1000
    flows['commodity'] = 'natural_gas'

    flows.drop(['value_kwh'], axis=1, inplace=True)
    flows.drop(['index'], axis=1, inplace=True)

    return flows


def update(date_from="2021-01-01", date_to=dt.date.today(), filename=None, save_to_file=True):
    flows = get_flows(date_from=date_from,
                      date_to=date_to,
                      save_to_file=save_to_file,
                      filename=filename)
    upsert(df=flows, table=DB_TABLE_ENTSOGFLOW, constraint_name="unique_entsogflow")
