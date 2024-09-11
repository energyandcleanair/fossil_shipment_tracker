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
from base.models.table_names import (
    DB_TABLE_KPLER_TRADE_COMPUTED_SHIPS,
    DB_TABLE_COMMODITY,
    DB_TABLE_KPLER_PRODUCT,
    DB_TABLE_KPLER_FLOW,
    DB_TABLE_KPLER_VESSEL,
    DB_TABLE_KPLER_TRADE,
    DB_TABLE_KPLER_TRADE_FLOW,
    DB_TABLE_KPLER_ZONE,
    DB_TABLE_KPLER_INSTALLATION,
    DB_TABLE_KPLER_TRADE_COMPUTED,
    DB_TABLE_KPLER_SYNC_HISTORY,
    DB_TABLE_KPLER_EXTENSION_ZONE_INDONESIA,
)


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
    build_date = Column(Date)

    type_class_name = Column(String)
    class_name = Column(String)
    type_name = Column(String)
    capacity_cm = Column(Numeric)

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


# This is a (materialised) view, not a table.
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
    ship_flag_iso2s = Column(ARRAY(String))
    flag_sanction_coverage = Column(String)
    step_zone_names = Column(ARRAY(String))
    step_zone_iso2s = Column(ARRAY(String))
    step_zone_regions = Column(ARRAY(String))
    step_zone_ids = Column(ARRAY(Numeric))

    vessel_types = Column(ARRAY(String))
    vessel_capacities_cm = Column(ARRAY(Numeric))
    largest_vessel_type = Column(String)
    largest_vessel_capacity_cm = Column(Numeric)

    vessel_ages = Column(ARRAY(Numeric))
    avg_vessel_age = Column(Numeric)

    crea_designations = Column(ARRAY(String))

    n_inspections_2y = Column(ARRAY(Numeric))
    detentions_per_inspection_2y = Column(ARRAY(Numeric))
    deficiencies_per_inspection_2y = Column(ARRAY(Numeric))

    avg_n_inspections_2y = Column(Numeric)
    avg_detentions_per_inspection_2y = Column(Numeric)
    avg_deficiencies_per_inspection_2y = Column(Numeric)
    avg_n_detentions_2y = Column(Numeric)

    __tablename__ = DB_TABLE_KPLER_TRADE_COMPUTED


# This is a (materialised) view, not a table.
class KplerTradeComputedShips(Base):
    trade_id = Column(BigInteger, primary_key=True)
    flow_id = Column(BigInteger, primary_key=True)
    product_id = Column(BigInteger, primary_key=True)
    pricing_scenario = Column(String, primary_key=True)
    ownership_sanction_coverage = Column(String)
    pricing_commodity = Column(BigInteger)
    kpler_product_commodity_id = Column(BigInteger)
    flag_sanction_coverage = Column(String)
    vessel_imo = Column(String)
    ship_insurer_name = Column(String)
    ship_insurer_iso2 = Column(String)
    ship_insurer_region = Column(String)
    ship_owner_name = Column(String)
    ship_owner_iso2 = Column(String)
    ship_owner_region = Column(String)
    vessel_age = Column(Numeric)
    ship_flag_iso2 = Column(String)
    eur_per_tonne = Column(Numeric)
    crea_designation = Column(String)

    step_in_trade = Column(Numeric)
    total_steps_in_trade = Column(Numeric)

    vessel_type = Column(String)
    vessel_capacity_cm = Column(Numeric)

    n_inspections_2y = Column(Numeric)
    detentions_per_inspection_2y = Column(Numeric)
    deficiencies_per_inspection_2y = Column(Numeric)
    n_detentions_2y = Column(Numeric)

    start_sts_zone_id = Column(BigInteger)
    start_sts_zone_name = Column(String)
    start_sts_iso2 = Column(String)
    start_sts_country = Column(String)
    start_sts_region = Column(String)

    end_sts_zone_id = Column(BigInteger)
    end_sts_zone_name = Column(String)
    end_sts_iso2 = Column(String)
    end_sts_country = Column(String)
    end_sts_region = Column(String)

    __tablename__ = DB_TABLE_KPLER_TRADE_COMPUTED_SHIPS


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
    geometry = Column(Geometry("POINT", srid=4326))
    area = Column(String)

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


class KplerZoneExtensionIndonesiaIsland(Base):

    zone_id = Column(ForeignKey(DB_TABLE_KPLER_ZONE + ".id"), primary_key=True)
    island_name = Column(String)
    region_name = Column(String)

    __tablename__ = DB_TABLE_KPLER_EXTENSION_ZONE_INDONESIA
