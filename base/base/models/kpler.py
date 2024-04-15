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

from . import DB_TABLE_COMMODITY, DB_TABLE_KPLER_PRODUCT
from . import DB_TABLE_KPLER_FLOW
from . import DB_TABLE_KPLER_VESSEL
from . import DB_TABLE_KPLER_TRADE
from . import DB_TABLE_KPLER_TRADE_FLOW
from . import DB_TABLE_KPLER_ZONE
from . import DB_TABLE_KPLER_INSTALLATION
from . import DB_TABLE_KPLER_TRADE_COMPUTED
from . import DB_TABLE_KPLER_SYNC_HISTORY


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
    mmsi = Column(String)
    name = Column(String)
    type = Column(String)
    dwt = Column(Numeric)  # in tonnes

    country_iso2 = Column(String)
    country_name = Column(String)
    others = Column(JSONB)

    __tablename__ = DB_TABLE_KPLER_VESSEL
    __table_args__ = (Index("idx_kple_vessel_imo", "imo"),)


class KplerTrade(Base):
    id = Column(BigInteger, primary_key=True)
    flow_id = Column(BigInteger, primary_key=True)

    status = Column(String)

    departure_date_utc = Column(DateTime, nullable=False)
    departure_zone_id = Column(Integer, nullable=False)
    departure_installation_id = Column(Integer)
    departure_installation_name = Column(String)
    departure_berth_id = Column(Integer)
    departure_berth_name = Column(String)
    departure_sts = Column(Boolean)

    arrival_date_utc = Column(DateTime)
    arrival_zone_id = Column(Integer)
    arrival_installation_id = Column(Integer)
    arrival_installation_name = Column(String)
    arrival_berth_id = Column(Integer)
    arrival_berth_name = Column(String)
    arrival_sts = Column(Boolean)

    vessel_ids = Column(ARRAY(Integer))
    vessel_imos = Column(ARRAY(String))

    step_zone_ids = Column(ARRAY(String))

    buyer_ids = Column(ARRAY(Integer))
    buyer_names = Column(ARRAY(String))

    seller_ids = Column(ARRAY(Integer))
    seller_names = Column(ARRAY(String))

    step_zone_ids = Column(ARRAY(Integer))

    product_id = Column(Integer, primary_key=True)
    value_tonne = Column(Numeric)
    value_m3 = Column(Numeric)
    value_energy = Column(Numeric)
    value_gas_m3 = Column(Numeric)

    others = Column(JSONB)

    updated_on = Column(DateTime, server_default=func.now(), onupdate=func.now())
    is_valid = Column(Boolean, default=True)

    __table_args__ = (
        Index("idx_kpler_trade_product_id", "product_id"),
        Index("idx_kpler_flow_departure_zone_id", "departure_zone_id"),
        Index("idx_kpler_flow_arrival_zone_id", "arrival_zone_id"),
    )

    __tablename__ = DB_TABLE_KPLER_TRADE


class KplerTradeComputed(Base):
    trade_id = Column(
        ForeignKey(DB_TABLE_KPLER_TRADE + ".id"),
        primary_key=True,
    )
    flow_id = Column(ForeignKey(DB_TABLE_KPLER_TRADE + ".flow_id"), primary_key=True)
    product_id = Column(ForeignKey(DB_TABLE_KPLER_TRADE + ".product_id"), primary_key=True)
    pricing_scenario = Column(String, primary_key=True)

    eur_per_tonne = Column(Numeric)
    pricing_commodity = Column(ForeignKey(DB_TABLE_COMMODITY + ".id"))
    kpler_product_commodity_id = Column(ForeignKey(DB_TABLE_COMMODITY + ".id"))
    ship_insurer_names = Column(ARRAY(String))
    ship_insurer_iso2s = Column(ARRAY(String))
    ship_insurer_regions = Column(ARRAY(String))
    ship_owner_names = Column(ARRAY(String))
    ship_owner_iso2s = Column(ARRAY(String))
    ship_owner_regions = Column(ARRAY(String))
    ownership_sanction_coverage = Column(String)
    step_zone_names = Column(ARRAY(String))
    step_zone_iso2s = Column(ARRAY(String))
    step_zone_regions = Column(ARRAY(String))
    step_zone_ids = Column(ARRAY(Numeric))

    __table_args__ = (
        Index(
            "kpler_trade_computed_ownership_sanction_coverage_idx", "ownership_sanction_coverage"
        ),
        Index("kpler_trade_computed_pricing_scenario_idx", "pricing_scenario"),
        Index("kpler_trade_computed_pricing_commodity_idx", "pricing_commodity"),
        Index("kpler_trade_computed_kpler_product_commodity_id_idx", "kpler_product_commodity_id"),
    )

    __tablename__ = DB_TABLE_KPLER_TRADE_COMPUTED


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
    name = Column(String, nullable=False)

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


class KplerInstallation(Base):
    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    fullname = Column(String)
    type = Column(String)
    port_id = Column(BigInteger)

    __tablename__ = DB_TABLE_KPLER_INSTALLATION


class KplerSyncHistory(Base):
    id = Column(BigInteger, primary_key=True)
    date = Column(Date, nullable=False)
    country_iso2 = Column(String, nullable=False)
    last_updated = Column(String, nullable=False)
    is_valid = Column(Boolean)
    last_checked = Column(DateTime)

    __tablename__ = DB_TABLE_KPLER_SYNC_HISTORY
