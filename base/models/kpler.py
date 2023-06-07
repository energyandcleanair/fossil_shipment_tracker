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
from . import DB_TABLE_KPLER_INSTALLATION
from . import DB_TABLE_KPLER_ZONE


class KplerProduct(Base):
    name = Column(String, primary_key=True)
    platform = Column(String, primary_key=True)
    group = Column(String)
    family = Column(String)

    __tablename__ = DB_TABLE_KPLER_PRODUCT


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
    product = Column(String, nullable=False)
    platform = Column(String, nullable=False)

    value = Column(Numeric, nullable=False)

    updated_on = Column(DateTime, server_default=func.now(), onupdate=func.now())
    is_valid = Column(Boolean, default=True)

    __tablename__ = DB_TABLE_KPLER_FLOW
    __table_args__ = (
        Index("idx_kpler_flow_product", "product"),
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


class KplerZone(Base):
    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    isPort = Column(Boolean)
    isSupplyDemand = Column(Boolean)
    # geo = Column(Geometry("POINT", srid=4326))
    continent = Column(JSONB)
    export = Column(JSONB)
    parentZones = Column(JSONB)
    range = Column(Numeric)
    subcontinent = Column(JSONB)
    # shape = Column(Geometry("GEOMETRY", srid=4326))
    type = Column(String)
    import_info = Column(JSONB)
    isStorageSelected = Column(Boolean)
    fullname = Column(String)
    platform = Column(String)

    __tablename__ = DB_TABLE_KPLER_ZONE
    __table_args__ = (UniqueConstraint("id", name="unique_kpler_zone"),)
