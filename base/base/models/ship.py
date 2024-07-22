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


class Ship(Base):
    imo = Column(String, primary_key=True)
    others = Column(JSONB)

    # The rest of these fields may have values but are no longer updated.
    mmsi = Column(ARRAY(String))
    name = Column(ARRAY(String))
    type = Column(String)
    subtype = Column(String)
    dwt = Column(Numeric)  # in tonnes

    country_iso2 = Column(String)
    country_name = Column(String)
    home_port = Column(String)
    liquid_gas = Column(Numeric)
    liquid_oil = Column(Numeric)

    # Estimated commodity, quantity etc
    commodity = Column(String)
    quantity = Column(Numeric)
    unit = Column(String)

    __tablename__ = DB_TABLE_SHIP
    __table_args__ = (Index("idx_ship_imo", "imo"),)

    @validates("liquid_oil")
    def validate_liquid_oil(self, key, liquid_oil):
        try:
            return float(liquid_oil)
        except (ValueError, TypeError):
            return None

    @validates("liquid_gas")
    def validate_liquid_gas(self, key, liquid_gas):
        try:
            return float(liquid_gas)
        except (ValueError, TypeError):
            return None


class ShipInsurer(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    imo = Column(String)
    ship_imo = Column(
        String, ForeignKey(DB_TABLE_SHIP + ".imo", onupdate="CASCADE"), nullable=False
    )
    date_from_insurer = Column(DateTime(timezone=False))
    date_from_equasis = Column(DateTime(timezone=False))
    company_raw_name = Column(String, nullable=False)  # Name indicated by Equasis
    company_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_COMPANY + ".id", onupdate="CASCADE"),
        nullable=False,
    )  # Link to cleaned list of companies
    updated_on = Column(DateTime, server_default=func.now())
    updated_on_insurer = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())
    checked_on = Column(DateTime, server_default=func.now())
    consecutive_failures = Column(BigInteger, nullable=False, default=0)

    is_valid = Column(Boolean, nullable=False, default=True)

    __tablename__ = DB_TABLE_SHIP_INSURER
    __table_args__ = (
        Index("idx_ship_insurer_ship_imo", "ship_imo"),
        UniqueConstraint("ship_imo", "company_raw_name", name="unique_ship_insurer"),
    )


class ShipOwner(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    imo = Column(String)
    ship_imo = Column(
        String, ForeignKey(DB_TABLE_SHIP + ".imo", onupdate="CASCADE"), nullable=False
    )
    date_from = Column(DateTime(timezone=False))  # Most likely null, not indicated by Equasis
    company_raw_name = Column(String, nullable=False)  # Name indicated by Equasis
    company_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_COMPANY + ".id", onupdate="CASCADE"),
        nullable=False,
    )  # Link to cleaned list of companies
    updated_on = Column(DateTime, server_default=func.now(), server_onupdate=func.now())

    __tablename__ = DB_TABLE_SHIP_OWNER
    __table_args__ = (
        Index("idx_ship_owner_ship_imo", "ship_imo"),
        UniqueConstraint("ship_imo", "company_raw_name", "date_from", name="unique_ship_owner"),
    )


class ShipManager(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    imo = Column(String)
    ship_imo = Column(
        String, ForeignKey(DB_TABLE_SHIP + ".imo", onupdate="CASCADE"), nullable=False
    )
    date_from = Column(DateTime(timezone=False))  # Most likely null, not indicated by Equasis
    company_raw_name = Column(String, nullable=False)  # Name indicated by Equasis
    company_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_COMPANY + ".id", onupdate="CASCADE"),
        nullable=True,
    )  # Link to cleaned list of companies
    updated_on = Column(DateTime, server_default=func.now(), server_onupdate=func.now())

    __tablename__ = DB_TABLE_SHIP_MANAGER
    __table_args__ = (
        UniqueConstraint("ship_imo", "company_raw_name", "date_from", name="unique_ship_manager"),
    )


class ShipFlag(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    imo = Column(String)
    flag_iso2 = Column(String)
    first_seen = Column(DateTime)
    updated_on = Column(DateTime, server_default=func.now(), server_onupdate=func.now())

    __tablename__ = DB_TABLE_SHIP_FLAG


class ShipInspection(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    ship_imo = Column(
        String, ForeignKey(DB_TABLE_SHIP + ".imo", onupdate="CASCADE"), nullable=False
    )
    authority = Column(String)
    port_of_inspection = Column(String)
    date_of_report = Column(DateTime(timezone=False))
    detention = Column(Boolean)
    psc_organisation = Column(String)
    type_of_inspection = Column(String)
    duration_days = Column(Integer)
    number_of_deficiencies = Column(Integer)

    updated_on = Column(DateTime, server_default=func.now(), server_onupdate=func.now())

    __tablename__ = DB_TABLE_SHIP_INSPECTIONS
    __table_args__ = (Index("idx_ship_inspection_ship_imo", "ship_imo"),)
