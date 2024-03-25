import pandas as pd
import datetime as dt
import os
from google.cloud import storage
import pyreadr

import base
from base.logger import logger_slack
from base.env import get_env

import tempfile

from engines import api_client


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
        with tempfile.TemporaryDirectory() as temp_dir:
            filepath = os.path.join(temp_dir, filename + "." + ext)

            if ext == "RDS":
                pyreadr.write_rds(filepath, df, compress="gzip")
            if ext == "csv.gz":
                df.to_csv(filepath, compression="gzip")

            blob = client_bucket.blob("%s/%s" % (folder, filepath))
            blob.upload_from_filename(filepath)


def backup_voyages(client_bucket, folder, now):

    voyages_df = api_client.get_voyages(
        date_from="2021-01-01",
        commodity_grouping="default",
        currency=["USD", "EUR"],
        pricing_scenario=base.PRICING_DEFAULT,
    )
    filename = "voyages_%s" % (now.strftime("%Y%m%d_%H%M"))
    upload(df=voyages_df, client_bucket=client_bucket, folder=folder, filename=filename)


def backup_overland(client_bucket, folder, now):

    overland_df = api_client.get_overland(
        date_from="2021-01-01",
        commodity_grouping="default",
        currency=["USD", "EUR"],
        keep_zeros=False,
        pricing_scenario=base.PRICING_DEFAULT,
    )
    filename = "overland_%s" % (now.strftime("%Y%m%d_%H%M"))
    upload(df=overland_df, client_bucket=client_bucket, folder=folder, filename=filename)


def backup_counter(client_bucket, folder, now):

    counter_df = api_client.get_counter(
        date_from="2021-01-01",
        commodity_grouping="default",
        currency=["USD", "EUR"],
        keep_zeros=False,
        pricing_scenario=base.PRICING_DEFAULT,
    )

    filename = "counter_%s" % (now.strftime("%Y%m%d_%H%M"))
    upload(df=counter_df, client_bucket=client_bucket, folder=folder, filename=filename)
