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


class GlobalCache(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(JSONB)

    __tablename__ = DB_TABLE_GLOBAL_CACHE


class EndpointCache(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    endpoint = Column(String)
    params = Column(JSONB)
    response = Column(JSONB)
    updated_on = Column(
        DateTime(timezone=False),
        default=dt.datetime.utcnow,
        onupdate=dt.datetime.utcnow,
    )

    __tablename__ = DB_TABLE_ENDPOINTCACHE
    __table_args__ = (UniqueConstraint("endpoint", "params", name="unique_endpointcache"),)
