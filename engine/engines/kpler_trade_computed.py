from enum import Enum
import logging
import warnings
import numpy as np
from tqdm import tqdm
import pandas as pd
import datetime as dt

from sqlalchemy import (
    func,
    cast,
    String,
    Integer,
)
from sqlalchemy.dialects.postgresql import array, ARRAY
import sqlalchemy as sa
from sqlalchemy import func

from sqlalchemy.sql.expression import delete, insert

from base.db import session, Base
from base.logger import logger_slack, logger
from engines.insurance_scraper import *
from base.models import (
    KplerProduct,
    KplerTrade,
    KplerTradeComputed,
)

from sqlalchemy import Column, String, Integer, Numeric, BigInteger

from tqdm.contrib.logging import logging_redirect_tqdm
import os


class KplerTradeComputedUpdateSteps(Enum):
    RECREATE_PRECOMPUTATION_TABLES = "RECRAETE_PRECOMPUTATION_TABLES"
    RELOAD_DATA = "RELOAD_DATA"


def update(
    steps=[
        KplerTradeComputedUpdateSteps.RECREATE_PRECOMPUTATION_TABLES,
        KplerTradeComputedUpdateSteps.RELOAD_DATA,
    ]
):
    logger_slack.info("=== Updating kpler computed table ===")

    if KplerTradeComputedUpdateSteps.RECREATE_PRECOMPUTATION_TABLES in steps:
        recreate_precomputation_tables()
        check_precomputation_tables()

    if KplerTradeComputedUpdateSteps.RELOAD_DATA in steps:
        try:
            logger.info(f"Starting transaction for updating kpler computed table")
            with session.begin_nested():
                logger.info(f"Deleting all entries from {KplerTradeComputed.__tablename__}")
                session.execute(delete(KplerTradeComputed))

                logger.info(
                    f"Copying all entries from ktc_kpler_trade_computed to {KplerTradeComputed.__tablename__}"
                )
                update_kpler_trade_computed_table_from_view()

                check_invalid_trade_computed()

            logger.info(f"Committing transaction for updating kpler computed table")
            session.commit()

        except Exception as e:
            logger_slack.error(
                f"Updating kpler computed table failed",
                stack_info=True,
                exc_info=True,
            )


def recreate_precomputation_tables():
    logger.info("Updating precomputation tables")

    delete_ktc_views()

    for sql, id in get_all_view_creation_sql():
        logger.info(f"Running {id}")
        session.execute(sql)
    logger.info("Precomputation tables updated")


def check_precomputation_tables():
    tables = get_existing_ktc_view_names()
    assert len(tables) > 0, "No precomputation tables created"

    for table in tables:
        logger.info(f"Checking {table}")
        count = session.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        assert count > 0, f"Table {table} is empty"


def update_kpler_trade_computed_table_from_view():
    logger.info("Updating kpler_trade_computed table")
    session.execute(get_final_update_sql())


def delete_ktc_views():
    view_names = get_existing_ktc_view_names()
    for view_name in view_names:
        logger.info(f"Dropping {view_name}")
        session.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")


def check_invalid_trade_computed():
    """
    Inspect the computed trades that have no associated pricing
    and confirm that this is expected. Throw an error if not
    """
    logger.info("Checking for invalid computed trades")
    ignorable_commodities = [
        "kpler_clean_condensate",
        "kpler_bitumen_asphalt",
        "kpler_cbfs",
        "kpler_coal_tar",
        "kpler_pitch",
        "kpler_specialities",
        "kpler_cutter_stock",
        "kpler_resids",
        "kpler_blendings",
        "kpler_cycle_oil",
    ]
    # Not all commodities have old pricing
    date_from = dt.datetime(2015, 1, 1)

    # Commodity used for pricing
    commodity_id_field = build_commodity_id_field()

    missing_trades = pd.DataFrame(
        session.query(
            KplerTrade.departure_date_utc,
            KplerTrade.id.label("trade_id"),
            KplerTrade.product_id,
            commodity_id_field.label("kpler_product_commodity_id"),
        )
        .join(KplerProduct, KplerTrade.product_id == KplerProduct.id)
        .outerjoin(
            KplerTradeComputed,
            (KplerTrade.id == KplerTradeComputed.trade_id)
            & (KplerTrade.product_id == KplerTradeComputed.product_id)
            & (KplerTrade.flow_id == KplerTradeComputed.flow_id),
        )
        .filter(
            sa.and_(
                KplerTradeComputed.trade_id == None,
                KplerTrade.is_valid == True,
                KplerTrade.product_id != None,
                commodity_id_field != None,
                commodity_id_field.notin_(ignorable_commodities),
                KplerTrade.departure_date_utc >= date_from,
            )
        )
        .all()
    )

    if any(missing_trades):
        logger_slack.error(f"Computed trades without pricing found")
        raise Exception(f"Computed trades without pricing found:\n{missing_trades}")


def get_existing_ktc_view_names():
    names = session.execute(
        """
select matviewname as view_name
from pg_matviews
order by view_name;
        """
    )

    return [name[0] for name in names if name[0].startswith("ktc_")]


def get_final_update_sql():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    update_file = f"{current_dir}/kpler_trade_computed/update.sql"
    with open(update_file, "r") as file:
        sql = file.read()
        return sql


def get_all_view_creation_sql():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    views_dir = f"{current_dir}/kpler_trade_computed/views/"
    files = list(os.listdir(views_dir))
    sorted_files = sorted(files)
    for filename in sorted_files:
        if filename.endswith(".sql"):
            filepath = os.path.join(views_dir, filename)
            with open(filepath, "r") as file:
                sql = file.read()
                yield sql, filename


def build_commodity_id_field():
    return "kpler_" + sa.func.replace(
        sa.func.replace(
            sa.func.lower(func.coalesce(KplerProduct.commodity_name, KplerProduct.group_name)),
            " ",
            "_",
        ),
        "/",
        "_",
    )
