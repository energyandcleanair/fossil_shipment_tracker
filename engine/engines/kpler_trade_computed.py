from enum import Enum
import logging
from typing import Iterable
import warnings
import numpy as np
import psycopg2
import sqlalchemy
import sqlalchemy.exc
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
from sqlalchemy.schema import DropConstraint, AddConstraint
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
    KplerTradeComputedShips,
)

from sqlalchemy import Column, String, Integer, Numeric, BigInteger

from tqdm.contrib.logging import logging_redirect_tqdm
import os


def update():
    logger_slack.info("=== Updating kpler computed table ===")

    with session.begin_nested() as savepoint:
        drop_old_ktc_temp_tables()
        create_new_temp_computation_tables()
        drop_old_ktc_tables()
        switch_temp_to_actual()
        check_precomputation_tables()
        check_invalid_trade_computed()

        savepoint.commit()

    session.commit()


def drop_old_ktc_temp_tables():
    tables = get_temp_existing_ktc_names()
    for table in tables:
        logger.info(f"Dropping {table}")
        session.execute(f"DROP MATERIALIZED VIEW IF EXISTS {table} CASCADE")


def drop_old_ktc_tables():
    tables = get_actual_existing_ktc_names()
    for table in tables:
        logger.info(f"Dropping {table}")
        session.execute(f"DROP MATERIALIZED VIEW IF EXISTS {table} CASCADE")


def switch_temp_to_actual():
    temp_tables = get_temp_existing_ktc_names()
    for table in temp_tables:
        new_table_name = table.replace("_temp", "")
        logger.info(f"Renaming {table} to {new_table_name}")
        result = session.execute(f"ALTER MATERIALIZED VIEW {table} RENAME TO {new_table_name}")


def create_new_temp_computation_tables():
    logger.info("Updating precomputation tables")

    for sql, id in get_all_view_creation_sql():
        start_time = dt.datetime.now()
        logger.info(f"Running {id}")
        session.execute(sql)
        end_time = dt.datetime.now()
        execution_time_seconds = (end_time - start_time).total_seconds()
        logger.info(f"View {id} took {execution_time_seconds}")
    logger.info("Precomputation tables updated")


def check_precomputation_tables():
    tables = get_actual_existing_ktc_names()
    assert len(tables) > 0, "No precomputation tables created"

    for table in tables:
        logger.info(f"Checking {table}")
        count = session.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        assert count > 0, f"Table {table} is empty"

    # Check for duplicates in the ktc_kpler_trade_computed table
    trade_duplicates = session.execute(
        """
        WITH duplicate_rows AS (
            SELECT trade_id, product_id, flow_id, pricing_scenario, count(*)
            FROM ktc_kpler_trade_computed
            GROUP BY trade_id, product_id, flow_id, pricing_scenario
            HAVING count(*) > 1
        )
        SELECT count(*) FROM duplicate_rows
        """
    ).fetchone()[0]

    assert trade_duplicates == 0, f"Found {trade_duplicates} duplicates in ktc_kpler_trade_computed"

    ships_duplicates = session.execute(
        """
        WITH duplicate_rows AS (
            SELECT
                trade_id,
                flow_id,
                product_id,
                pricing_scenario,
                vessel_imo,
                step_in_trade,
                count(*)
            FROM ktc_kpler_trade_computed_ships
            GROUP BY
                trade_id,
                flow_id,
                product_id,
                pricing_scenario,
                vessel_imo,
                step_in_trade
            HAVING count(*) > 1
        )
        SELECT count(*) FROM duplicate_rows
        """
    ).fetchone()[0]

    assert (
        ships_duplicates == 0
    ), f"Found {ships_duplicates} duplicates in ktc_kpler_trade_computed_ships"


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
        "kpler_coke",
    ]
    # Not all commodities have old pricing
    date_from = dt.datetime(2015, 1, 1)
    date_now = dt.datetime.now()

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
                KplerTrade.departure_date_utc <= date_now,
            )
        )
        .all()
    )

    if any(missing_trades):
        logger_slack.error(f"Computed trades without pricing found")
        raise Exception(f"Computed trades without pricing found:\n{missing_trades}")


def get_actual_existing_ktc_names():
    names = session.execute(
        """
select matviewname as view_name
from pg_matviews
order by view_name;
        """
    )

    return [
        name[0] for name in names if name[0].startswith("ktc_") and not name[0].endswith("_temp")
    ]


def get_temp_existing_ktc_names():
    names = session.execute(
        """
select matviewname as view_name
from pg_matviews
order by view_name;
        """
    )

    return [name[0] for name in names if name[0].startswith("ktc_") and name[0].endswith("_temp")]


def get_all_view_creation_sql() -> Iterable[tuple[str, str]]:
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
