import datetime as dt
import pandas as pd
import numpy as np
from collections import defaultdict
from tqdm import tqdm

import base

from base.db import session
from base.logger import logger
from base.utils import to_list, to_datetime
from base.models import Ship, PortCall
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
    logger.info(url)
    params['limit'] = limit
    api_result = requests.get(url, params=params)

    if api_result.status_code != 200:
        logger.warning("ENTSOG: Failed to query entsog %s %s" % (url, params))
        return None

    res = api_result.json()

    try:
        if res["meta"]["total"] > res["meta"]["count"] * 1.01:
            # +2: for some reason, sometimes total is +1 or +2
            logger.warning("More data available (%d/%d). Increase limit or implement a loop here...",
                           res["meta"]["total"], res["meta"]["count"])
    except KeyError as e:
        logger.warning("May have failed for: %s %s" %(url, params))
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
    url =  "https://transparency.entsog.eu/api/v1/interconnections"
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

    if operator_key:
        params['operatorKey'] = ','.join(list(set(to_list(operator_key))))

    if point_key:
        params['pointKey'] = ','.join(list(set(to_list(point_key))))

    if date_from:
        params['from'] = to_datetime(date_from).strftime("%Y-%m-%d")

    if date_to:
        params['to'] = to_datetime(date_to).strftime("%Y-%m-%d")

    if direction:
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



def get_crossborder_flows(date_from='2022-01-01',
                          date_to=dt.date.today(),
                          country_iso2=None,
                          partner_iso2=None,
                          remove_pipe_in_pipe=True):

    #TODO add LNG points
    # flows_aggregated = get_aggregated_physical_flows(date_from=date_from)
    # # flows = flows.loc[flows.value>0]
    # flows_aggregated.adjacentSystemsLabel.unique()

    opd = get_operator_point_directions()
    ic = get_interconnections()

    # a = opd.loc[~opd.isDoubleReporting.isnull()].sort_values(['pointKey'])
    b = opd.loc[opd.pointLabel.str.contains('Dornum') &
                # opd.operatorKey.str.contains('DE') &
                opd.directionKey.str.contains('entry') &
                opd.hasData
    ].drop(["isVirtualizedCommercially", "virtualizedOperationallySince", "virtualizedCommerciallySince",
            "isVirtualizedOperationally"],axis=1) \
           .sort_values(['pointKey'])

    if country_iso2:
        ic = ic.loc[(ic.toCountryKey.isin(to_list(country_iso2)))]

    if partner_iso2:
        ic = ic.loc[(ic.fromCountryKey.isin(to_list(partner_iso2)))]



    #########################
    # Get raw flow data
    #########################
    ic_crossborder_import = ic.loc[(ic.toCountryKey != ic.fromCountryKey) & \
                                   (~ic.toOperatorKey.isnull())][['toOperatorKey', 'toPointKey']] \
        .drop_duplicates()

    if remove_pipe_in_pipe:
        keep = opd.loc[(opd.directionKey=='entry') & \
                       (opd.isPipeInPipe.isnull() | ~opd.isPipeInPipe |
                            (opd.isPipeInPipe & opd.isDoubleReporting.isnull()))][['operatorKey', 'pointKey']] \
        .rename(columns={'operatorKey': 'toOperatorKey',
                         'pointKey': 'toPointKey'}) \
        .drop_duplicates()
        ic_crossborder_import = ic_crossborder_import.merge(keep, how='inner')

    flows_import_raw = get_physical_flows(
        operator_key=ic_crossborder_import.toOperatorKey.to_list(),
        point_key=ic_crossborder_import.toPointKey.to_list(),
        direction="entry",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    ).sort_values(['date'])




    ic_crossborder_export = ic.loc[(ic.toCountryKey != ic.fromCountryKey) & \
                                   (~ic.fromOperatorKey.isnull())][['fromOperatorKey', 'fromPointKey']] \
        .drop_duplicates()

    if remove_pipe_in_pipe:
        keep = opd.loc[(opd.directionKey == 'exit') & \
                       (opd.isPipeInPipe.isnull() | ~opd.isPipeInPipe |
                        (opd.isPipeInPipe & opd.isDoubleReporting.isnull()))][['operatorKey', 'pointKey']] \
            .rename(columns={'operatorKey': 'fromOperatorKey',
                             'pointKey': 'fromPointKey'}) \
            .drop_duplicates()
        ic_crossborder_export = ic_crossborder_export.merge(keep, how='inner')

    flows_export_raw = get_physical_flows(
        operator_key=ic_crossborder_export.fromOperatorKey.to_list(),
        point_key=ic_crossborder_export.fromPointKey.to_list(),
        direction="exit",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )


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

    def process_flows(ic, flows_import_raw, flows_export_raw,
                      keep_opd_only=False, keep_confirmed_only=False, auto_confirmed_only=True,
                      groupby_cols=['pointKey', 'operatorKey_import', 'date', 'country_import'],
                      flows_import_agg_cols=None,
                      flows_export_agg_cols=None,
                      flows_import_on=['pointKey', 'date', 'partner', 'country'],
                      flows_export_on=['pointKey', 'date', 'country', 'partner'],
                      remove_operators=[],
                      filename=None):

        points_import = ic[
            ["toPointKey", "toOperatorKey", "toCountryLabel", 'fromCountryKey', "fromCountryLabel", 'fromOperatorKey']] \
            .drop_duplicates() \
            .rename(columns={'toPointKey': 'pointKey',
                             'toOperatorKey': 'operatorKey',
                             'toCountryLabel': 'country',
                             'fromCountryKey': 'adjacentCountry',
                             'fromCountryLabel': 'partner',
                             'fromOperatorKey': 'partner_operatorKey',
                             })

        if keep_opd_only:
            # Try to only keep feasible connections
            points_import = points_import.merge(opd[['pointKey', 'operatorKey', 'adjacentCountry']].drop_duplicates(),
                                how='inner')

        flows_import = pd.merge(flows_import_raw, points_import,
                                on=['pointKey', 'operatorKey'],
                                how='left')

        if keep_confirmed_only:
            flows_import = flows_import.loc[flows_import.flowStatus=='Confirmed']


        points_export = ic[["fromPointKey", "fromOperatorKey", "fromCountryLabel", "toCountryLabel"]] \
            .drop_duplicates() \
            .rename(columns={'fromPointKey': 'pointKey',
                             'fromOperatorKey': 'operatorKey',
                             'fromCountryLabel': 'country',
                             'toCountryLabel': 'partner'
                             })

        if flows_export_raw is None:
            flows_export_raw = pd.DataFrame({'pointKey': pd.Series(dtype='str'),
                              'operatorKey': pd.Series(dtype='int'),
                              'tsoItemIdentifier': pd.Series(dtype='str'),
                              'value': pd.Series(dtype='float'),
                              'date': pd.Series(dtype='str')})


        flows_export = pd.merge(flows_export_raw, points_export,
                                on=['pointKey', 'operatorKey'],
                                how='left')

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
        flows_import_cols = ['pointKey', 'operatorKey', 'tsoItemIdentifier', 'value', 'date', 'country', 'partner', 'partner_operatorKey', 'flowStatus'] \
            if not flows_import_agg_cols else [*flows_import_agg_cols, 'value']
        flows_export_cols = ['pointKey', 'operatorKey', 'tsoItemIdentifier', 'value', 'date', 'country', 'partner',
                              'flowStatus'] \
            if not flows_export_agg_cols else [*flows_export_agg_cols, 'value']

        flows = pd.merge(
            flows_import[flows_import_cols],
            flows_export[flows_export_cols],
            left_on=flows_import_on,
            right_on=flows_export_on,
            suffixes=['_import', '_export'],
            how='outer') \
            .dropna(subset=['country_import'])

        if remove_operators:
            flows = flows.loc[~flows.operatorKey_import.isin(remove_operators)]

        def process_pt_op_date(df):
            df = df.loc[df.value_import > 0]

            if auto_confirmed_only and 'Confirmed' in df.flowStatus_import.to_list():
                df = df.loc[df.flowStatus_import == 'Confirmed']

            if auto_confirmed_only and 'Confirmed' in df.flowStatus_export.to_list():
                df = df.loc[df.flowStatus_export == 'Confirmed']

            if len(df) == 0:
                return(df)

            # This function only manages the case were one import
            # is matching several exports
            if not len(df[['pointKey', 'flowStatus_import', 'value_import']].drop_duplicates()) == 1:
                print(df)
                logger.warning("Flows are mismanaged")


            # Further checks
            assert all(df.country_export.isnull() | (df.country_export == df.partner_import))


            # Keep only confirmed if it is there
            # One import was mathching two exports
            # We need to
            value_import = np.nanmean(df.value_import)
            value_export_sum = np.nansum(df.value_export)
            if value_export_sum > 0:
                df['value'] = df.value_export / value_export_sum * value_import
            elif all(df.value_export.isnull()) and len(df)==1:
                df['value'] = value_import
            else:
                df['value'] = 0

            df['partner'] = np.where(df['country_export'].isnull(), df['partner_import'], df['country_export'])
            return df.reset_index()





        # df2 = flows.groupby(['tsoItemIdentifier_import', 'date',
        #                 'country_import', 'country_export', 'partner'], dropna=False) \
        #     .agg(value_import=('value_import', np.nanmean),
        #          value_export=('value_export', np.nanmean),
        #          ).reset_index()

        # # Sum by point. date doublecounting (two operators for one actual physical flow)
        # df3 = df2.groupby(['pointKey', 'date',
        #                  'country_import', 'country_export', 'partner'], dropna=False) \
        #     .agg(value_import=('value_import', np.nansum),
        #          value_export=('value_export', np.nansum),
        #          ).reset_index()

        # a = flows.loc[flows.value_export==4150785]
        # b = ic.loc[ic.toPointKey.isin(a.pointKey)]


        flows_scaled = flows.groupby(groupby_cols).apply(process_pt_op_date)
        flows_scaled = flows_scaled.reset_index(drop=True)

        #  a = flows.loc[(flows.partner=='Russia') &
        #                       (flows.country_import=='Germany') \
        #                      & (flows.date==dt.date(2022,1,1))].sort_values(['value_import'])
        #
        #
        # b = df2.loc[(df2.partner == 'Russia') &
        #           (df2.country_import == 'Germany') \
        #           & (df2.date == dt.date(2022, 1, 1))].sort_values(['value_import'])

        #
        # b = flows_scaled.loc[(flows_scaled.partner=='Russia') &
        #                      (flows_scaled.country_import=='Germany') \
        #                     & (flows_scaled.date==dt.date(2022,1,1))].sort_values(['date'])

        flows_agg = flows_scaled.groupby(['country_import', 'partner', 'date']) \
            .agg(value=('value', np.nansum)) \
            .reset_index() \
            .rename(columns={'country_import': 'to_country',
                             'partner': 'from_country'}) \
            .reset_index()

        if not filename:
            filename = f"entsog_flows_{keep_opd_only}_{keep_confirmed_only}_{join_flows_on_operator}_{auto_confirmed_only}.csv"
        flows_agg.to_csv(filename, index=False)
        return flows_agg


    # process_flows(ic, flows_import_raw, flows_export_raw,
    #         True, True, True)
    #
    # process_flows(ic, flows_import_raw, flows_export_raw,
    #               True, True, False)
    #
    # process_flows(ic, flows_import_raw, flows_export_raw,
    #               True, False, True)
    #
    # process_flows(ic, flows_import_raw, flows_export_raw,
    #               False, True, True)
    #
    # process_flows(ic, flows_import_raw, flows_export_raw,
    #               False, False, True)
    #
    # process_flows(ic, flows_import_raw, flows_export_raw,
    #               False, True, False)
    #
    # process_flows(ic, flows_import_raw, flows_export_raw,
    #               True, False, False)
    #
    # process_flows(ic, flows_import_raw, flows_export_raw,
    #               False, False, False)


    flows_agg = process_flows(ic, flows_import_raw, flows_export_raw,
                              keep_opd_only=False, keep_confirmed_only=False, auto_confirmed_only=True,
                              remove_operators=[])


    return flows_agg



def distribute_to_producers_singledate():


    return

def get_flows(date_from="2021-01-01", date_to=dt.date.today()):

    flows = get_crossborder_flows(date_from=date_from, date_to=date_to)

    date = flows.date.iloc[0]
    flows_date = flows.loc[flows.date==date]

    matrix_date = flows_date[['country', 'partner', 'value']]\
        .pivot(index='country',
                                                    columns='partner',
                                                    values='value')


    return flows


def update(date_from="2021-01-01"):
    flows_russia = get_flows(date_from=date_from)

