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


class Commodity(Base):
    id = Column(String, primary_key=True)
    equivalent_id = Column(String)  # Used for kpler commodities to have a generic equivalent
    transport = Column(String)
    name = Column(String)
    pricing_commodity = Column(String)
    group = Column(String)  # coal, oil, gas
    group_name = Column(String)  # Coal, Oil, Gas

    # To allow different grouping
    alternative_groups = Column(JSONB, nullable=True)  # default, split_gas

    __tablename__ = DB_TABLE_COMMODITY
    __table_args__ = (UniqueConstraint("id", name="unique_commodity"),)
