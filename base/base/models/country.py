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


class Country(Base):
    iso2 = Column(String, unique=True, primary_key=True)
    iso3 = Column(String)
    name_official = Column(String)
    name = Column(String)
    name_local = Column(String)
    region = Column(String)
    regions = Column(ARRAY(String))

    __tablename__ = DB_TABLE_COUNTRY
    __table_args__ = (UniqueConstraint("iso2", name="unique_country"),)
