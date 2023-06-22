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

from . import DB_TABLE_KPLER_PRODUCT
from . import DB_TABLE_KPLER_FLOW
from . import DB_TABLE_KPLER_VESSEL
from . import DB_TABLE_KPLER_TRADE
from . import DB_TABLE_KPLER_TRADE_FLOW
from . import DB_TABLE_KPLER_ZONE


class KplerFlow(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)

    from_iso2 = Column(String)
    to_iso2 = Column(String)

    from_zone_id = Column(String, nullable=False)
    from_zone_name = Column(String)
    from_split = Column(String, nullable=False)

    to_zone_id = Column(String, nullable=False)
    to_zone_name = Column(String)
    to_split = Column(String, nullable=False)

    date = Column(Date, nullable=False)
    unit = Column(String, nullable=False)

    grade = Column(String, nullable=True)
    commodity = Column(String, nullable=True)
    group = Column(String, nullable=True)
    family = Column(String, nullable=True)

    product = Column(String, nullable=False)

    platform = Column(String, nullable=False)

    value = Column(Numeric, nullable=False)

    updated_on = Column(DateTime, server_default=func.now(), onupdate=func.now())
    is_valid = Column(Boolean, default=True)

    __tablename__ = DB_TABLE_KPLER_FLOW
    __table_args__ = (
        Index("idx_kpler_flow_product", "product"),
        Index("idx_kpler_flow_commodity", "commodity"),
        Index("idx_kpler_flow_group", "group"),
        Index("idx_kpler_flow_from_iso2", "from_iso2"),
        Index("idx_kpler_flow_from_split", "from_split"),
        Index("idx_kpler_flow_to_split", "to_split"),
        Index("idx_kpler_flow_date", "date"),
        UniqueConstraint(
            "from_zone_id",
            "to_zone_id",
            "date",
            "unit",
            "product",
            "to_split",
            "from_split",
            name="unique_kpler_flow",
        ),
    )


class KplerVessel(Base):
    id = Column(BigInteger, primary_key=True)
    imo = Column(String)
    mmsi = Column(ARRAY(String))
    name = Column(ARRAY(String))
    type = Column(String)
    dwt = Column(Numeric)  # in tonnes

    country_iso2 = Column(String)
    country_name = Column(String)
    others = Column(JSONB)

    __tablename__ = DB_TABLE_KPLER_VESSEL
    __table_args__ = (Index("idx_kple_vessel_imo", "imo"),)


class KplerTrade(Base):
    id = Column(BigInteger, primary_key=True)
    product_id = Column(Integer, primary_key=True)
    product_name = Column(String)

    departure_date = Column(DateTime, nullable=False)
    arrival_date = Column(DateTime)
    status = Column(String)
    vessel_id = Column(Integer)
    vessel_imo = Column(String)  # Redundant, but just in case

    departure_zone_id = Column(Integer)
    departure_zone_name = Column(String)
    departure_installation_id = Column(Integer)
    departure_installation_name = Column(String)

    arrival_zone_id = Column(Integer)
    arrival_zone_name = Column(String)
    arrival_installation_id = Column(Integer)
    arrival_installation_name = Column(String)

    value_tonne = Column(Numeric)
    value_m3 = Column(String)

    others = Column(JSONB)

    __tablename__ = DB_TABLE_KPLER_TRADE


class KplerTradeFlow(Base):
    id = Column(BigInteger, primary_key=True)
    trade_id = Column(BigInteger, primary_key=True)
    product_id = Column(BigInteger, primary_key=True)
    mass = Column(Numeric)
    volume = Column(Numeric)
    energy = Column(Numeric)
    volume_gas = Column(Numeric)

    __tablename__ = DB_TABLE_KPLER_TRADE_FLOW


class KplerProduct(Base):
    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    full_name = Column(String)
    type = Column(String)
    grade_id = Column(BigInteger)
    grade_name = Column(String)
    commodity_id = Column(BigInteger)
    commodity_name = Column(String)
    group_id = Column(BigInteger)
    group_name = Column(String)
    family_id = Column(BigInteger)
    family_name = Column(String)
    __tablename__ = DB_TABLE_KPLER_PRODUCT


class KplerZone(Base):
    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    type = Column(String)
    port_id = Column(BigInteger)
    port_name = Column(String)
    country_id = Column(BigInteger)
    country_name = Column(String)
    country_iso2 = Column(String)

    __tablename__ = DB_TABLE_KPLER_ZONE
