import datetime as dt
import pandas as pd
import numpy as np
from collections import defaultdict

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
        for operator_key, point_keys in splitted.items():
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



def get_crossborder_flows(date_from='2021-01-01', date_to=dt.date.today()):

    #TODO add LNG points
    # flows_aggregated = get_aggregated_physical_flows(date_from=date_from)
    # # flows = flows.loc[flows.value>0]
    # flows_aggregated.adjacentSystemsLabel.unique()

    bz = get_balancing_zones()
    ic = get_interconnections()

    # Export import
    ic_crossborder_import = ic.loc[(ic.toCountryKey != ic.fromCountryKey) & \
                                   (~ic.toOperatorKey.isnull())]

    flows_import = get_physical_flows(
        operator_key=ic_crossborder_import.toOperatorKey.to_list(),
        point_key=ic_crossborder_import.toPointKey.to_list(),
        direction="entry",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    points_import = ic[["toPointKey", "toOperatorKey", "fromCountryLabel", "toCountryLabel"]] \
        .drop_duplicates() \
        .rename(columns={'toPointKey': 'pointKey',
                         'toOperatorKey': 'operatorKey',
                         'fromCountryLabel': 'from_country',
                         'toCountryLabel': 'to_country'
                         })

    flows_import_agg = flows_import.merge(points_import, how='left')
    flows_import_agg['from_country'] = flows_import_agg.from_country.fillna('Unknown')
    flows_import_agg['to_country'] = flows_import_agg.to_country.fillna('Unknown')
    
    flows_import_agg = flows_import_agg \
        .groupby(['date','from_country', 'to_country']) \
        .agg(value=('value', np.nansum)) \
        .reset_index() \
        .sort_values(['value'], ascending=False)

    flows_import_agg["direction"] = "import"
    flows_import_agg.to_csv('entsog_imports.csv', index=False)


    # Export
    ic_crossborder_export = ic.loc[(ic.toCountryKey != ic.fromCountryKey) & \
                                   (~ic.fromOperatorKey.isnull())]
    flows_export = get_physical_flows(
        operator_key=ic_crossborder_export.fromOperatorKey.to_list(),
        point_key=ic_crossborder_export.fromPointKey.to_list(),
        direction="exit",
        date_from=to_datetime(date_from),
        date_to=to_datetime(date_to),
    )

    points_export = ic[["fromPointKey", "fromOperatorKey", "fromCountryLabel", "toCountryLabel"]] \
        .drop_duplicates() \
        .rename(columns={'fromPointKey': 'pointKey',
                         'fromOperatorKey': 'operatorKey',
                         'fromCountryLabel': 'from_country',
                         'toCountryLabel': 'to_country'
                         })

    flows_export_agg = flows_export.merge(points_export, how='left')
    flows_export_agg['from_country'] = flows_export_agg.from_country.fillna('Unknown')
    flows_export_agg['to_country'] = flows_export_agg.to_country.fillna('Unknown')

    flows_export_agg = flows_export_agg \
        .groupby(['date','from_country', 'to_country']) \
        .agg(value=('value', np.nansum)) \
        .reset_index() \
        .sort_values(['value'], ascending=False)
    flows_export_agg["direction"] = "export"
    flows_export_agg.to_csv('entsog_exports.csv', index=False)


    return pd.concat([flows_import_agg, flows_export_agg], axis=0)


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

