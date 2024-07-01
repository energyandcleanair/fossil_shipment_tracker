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


class FlaringFacility(Base):
    """Geometries of Oil/Gas related installations"""

    id = Column(BigInteger, unique=True, primary_key=True)
    type = Column(String)
    name = Column(String)
    name_en = Column(String)
    url = Column(String)
    commodity = Column(String)
    geometry = Column(Geometry("GEOMETRY", srid=4326))

    __tablename__ = DB_TABLE_FLARING_FACILITY


class Flaring(Base):
    """Geometries of Oil/Gas related installations"""

    id = Column(BigInteger, unique=True, primary_key=True)
    facility_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_FLARING_FACILITY + ".id", ondelete="CASCADE"),
        nullable=False,
    )
    date = Column(Date)
    unit = Column(String, nullable=False)
    value = Column(Numeric)
    buffer_km = Column(Numeric)

    __table_args__ = (
        UniqueConstraint("facility_id", "date", "buffer_km", "unit", name="unique_flaring"),
    )
    __tablename__ = DB_TABLE_FLARING
