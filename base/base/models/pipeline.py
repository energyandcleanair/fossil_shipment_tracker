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


# ENTSOG data after outer join flows
# But before filtering them
# Used to prevent querying ENTSOG all the time when
# selecting OPDs
class EntsogFlowRaw(Base):
    id = Column(String, primary_key=True)
    date = Column(DateTime(timezone=False))
    periodFrom = Column(DateTime(timezone=False))
    periodTo = Column(DateTime(timezone=False))
    pointKey = Column(String)
    operatorKey = Column(String)
    directionKey = Column(String)
    flowStatus = Column(String)
    value_kwh = Column(Numeric)
    gcv_kwh_m3 = Column(Numeric)

    updated_on = Column(DateTime, server_default=func.now(), server_onupdate=func.now())
    __tablename__ = DB_TABLE_ENTSOGFLOW_RAW
    __table_args__ = (Index("idx_entsogflow_raw_pointkey", "pointKey"),)


# Entsog flows: before processing
# Mainly used to communicate between Python and R
# And also avoid recollecting everytime
class EntsogFlow(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    commodity = Column(String, ForeignKey(DB_TABLE_COMMODITY + ".id"), nullable=False)
    departure_iso2 = Column(String)
    destination_iso2 = Column(String)
    date = Column(DateTime(timezone=False))
    value_tonne = Column(Numeric)
    value_mwh = Column(Numeric)
    value_m3 = Column(Numeric)

    type = Column(String, nullable=False)

    updated_on = Column(DateTime, server_default=func.now(), server_onupdate=func.now())

    __tablename__ = DB_TABLE_ENTSOGFLOW
    __table_args__ = (
        UniqueConstraint(
            "date",
            "commodity",
            "departure_iso2",
            "destination_iso2",
            "type",
            name="unique_entsogflow",
        ),
    )


class PipelineFlow(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    commodity = Column(String, ForeignKey(DB_TABLE_COMMODITY + ".id"), nullable=False)

    departure_iso2 = Column(String)
    destination_iso2 = Column(String)

    date = Column(DateTime(timezone=False))
    value_tonne = Column(Numeric)
    value_mwh = Column(Numeric)
    value_m3 = Column(Numeric)

    updated_on = Column(DateTime, server_default=func.now(), server_onupdate=func.now())

    __tablename__ = DB_TABLE_PIPELINEFLOW
    __table_args__ = (
        UniqueConstraint(
            "date",
            "commodity",
            "departure_iso2",
            "destination_iso2",
            name="unique_pipelineflow",
        ),
    )
