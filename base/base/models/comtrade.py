from sqlalchemy import (
    Column,
    String,
    Date,
    DateTime,
    Integer,
    Numeric,
    BigInteger,
    Boolean,
    Time,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import validates
from sqlalchemy import UniqueConstraint, CheckConstraint, ForeignKey, Index, func
from geoalchemy2 import Geometry
import datetime as dt

import base
from base.db import Base
from base.logger import logger

from .table_names import *

CURRENT_DATA_SCHEMA_VERSION = "v2"


class ComtradeSyncHistory(Base):
    __tablename__ = DB_TABLE_COMTRADE_SYNC_HISTORY

    id = Column(BigInteger, autoincrement=True, primary_key=True)
    reporter_iso2 = Column(String, nullable=False)
    commodity_code = Column(String, nullable=False)
    period = Column(Date, nullable=False)
    last_updated = Column(DateTime, nullable=False)
    data_version = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint("reporter_iso2", "period", "commodity_code"),
        Index("comtrade_sync_history_reporter_iso2_idx", "reporter_iso2"),
        Index("comtrade_sync_history_commodity_code_idx", "commodity_code"),
        Index("comtrade_sync_history_period_idx", "period"),
    )


class ComtradeHsTradeRecord(Base):
    __tablename__ = DB_TABLE_COMTRADE_HS_TRADE_RECORD

    id = Column(BigInteger, autoincrement=True, primary_key=True)
    reporter_iso2 = Column(String, nullable=False)
    partner_iso2 = Column(String, nullable=False)
    commodity_code = Column(String, nullable=False)
    flow_direction = Column(String, nullable=False)
    period = Column(Date, nullable=False)
    value_kg = Column(Numeric)
    value_kg_estimated = Column(Boolean)
    value_usd = Column(Numeric)

    __table_args__ = (
        UniqueConstraint(
            "reporter_iso2",
            "partner_iso2",
            "commodity_code",
            "flow_direction",
            "period",
            name="comtrade_hs_record_unique",
        ),
        Index("comtrade_hs_record_reporter_iso2_idx", "reporter_iso2"),
        Index("comtrade_hs_record_partner_iso2_idx", "partner_iso2"),
        Index("comtrade_hs_record_commodity_code_idx", "commodity_code"),
        Index("comtrade_hs_record_flow_direction_idx", "flow_direction"),
        Index("comtrade_hs_record_period_idx", "period"),
    )
