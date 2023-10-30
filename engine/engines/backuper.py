import pandas as pd
import datetime as dt
import os
from google.cloud import storage
import pyreadr

import base
from base.logger import logger_slack
from base.env import get_env
from api.routes.overland import PipelineFlowResource
from api.routes.voyage import VoyageResource
from api.routes.counter import RussiaCounterResource


def update(bucket="russia_fossil_tracker", folder="backup"):
    logger_slack.info("=== Creating backup in %s/%s ===" % (bucket, folder))
    now = dt.datetime.now()

    try:
        client = storage.Client(project=get_env("PROJECT_ID"))
        client_bucket = client.bucket(bucket)
        backup_voyages(client_bucket=client_bucket, folder=folder, now=now)
        backup_overland(client_bucket=client_bucket, folder=folder, now=now)
        backup_counter(client_bucket=client_bucket, folder=folder, now=now)
        logger_slack.info("=== Creating backup done ===")
    except Exception as e:
        logger_slack.error(
            "=== Creating backup FAILED ===",
            stack_info=True,
            exc_info=True,
        )


def upload(df, client_bucket, folder, filename, exts=["RDS", "csv.gz"]):
    for ext in exts:
        filepath = filename + "." + ext

        if ext == "RDS":
            pyreadr.write_rds(filepath, df, compress="gzip")
        if ext == "csv.gz":
            df.to_csv(filepath, compression="gzip")

        blob = client_bucket.blob("%s/%s" % (folder, filepath))
        blob.upload_from_filename(filepath)
        os.remove(filepath)


def backup_voyages(client_bucket, folder, now):
    params = {
        "date_from": "2021-01-01",
        "commodity_grouping": "default",
        "currency": ["USD", "EUR"],
        "pricing_scenario": base.PRICING_DEFAULT,
        "format": "json",
    }
    resp = VoyageResource().get_from_params(params=params)
    voyages_df = pd.DataFrame(resp.json)
    filename = "voyages_%s" % (now.strftime("%Y%m%d_%H%M"))
    upload(df=voyages_df, client_bucket=client_bucket, folder=folder, filename=filename)


def backup_overland(client_bucket, folder, now):
    params = {
        "date_from": "2021-01-01",
        "commodity_grouping": "default",
        "currency": ["USD", "EUR"],
        "keep_zeros": False,
        "pricing_scenario": base.PRICING_DEFAULT,
        "format": "json",
    }
    resp = PipelineFlowResource().get_from_params(params=params)
    overland_df = pd.DataFrame(resp.json)
    filename = "overland_%s" % (now.strftime("%Y%m%d_%H%M"))
    upload(df=overland_df, client_bucket=client_bucket, folder=folder, filename=filename)


def backup_counter(client_bucket, folder, now):
    params = {
        "date_from": "2021-01-01",
        "commodity_grouping": "default",
        "currency": ["USD", "EUR"],
        "keep_zeros": False,
        "format": "json",
        "pricing_scenario": base.PRICING_DEFAULT,
    }
    resp = RussiaCounterResource().get_from_params(params=params)
    counter_df = pd.DataFrame(resp.json)
    filename = "counter_%s" % (now.strftime("%Y%m%d_%H%M"))
    upload(df=counter_df, client_bucket=client_bucket, folder=folder, filename=filename)
