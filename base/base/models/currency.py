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


class Currency(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    date = Column(Date)
    currency = Column(String)
    per_eur = Column(Numeric)  # All currencies
    estimated = Column(Boolean)  # Whether it is actual value or estimated (useful when API is down)

    __table_args__ = (UniqueConstraint("date", "currency", name="unique_currency"),)
    __tablename__ = DB_TABLE_CURRENCY
