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
    others = Column(JSONB)

    # owner = Column(String)
    # manager = Column(String)
    # insurer = Column(String)

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


class Port(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    unlocode = Column(String, unique=True, nullable=True)
    marinetraffic_id = Column(String, unique=True, nullable=True)
    datalastic_id = Column(String, unique=True, nullable=True)
    name = Column(String)
    iso2 = Column(String)
    check_departure = Column(Boolean)
    check_arrival = Column(Boolean)
    geometry = Column(Geometry("POINT", srid=4326))
    others = Column(JSONB)
    area = Column(String)

    __tablename__ = DB_TABLE_PORT
    __table_args__ = (
        Index("idx_port_unlocode", "unlocode"),
        Index("idx_port_name_lower", func.lower(name)),
        UniqueConstraint("unlocode", name="unique_port"),
    )


class Terminal(Base):
    id = Column(String, unique=True, primary_key=True)
    port_unlocode = Column(String, ForeignKey(DB_TABLE_PORT + ".unlocode"))
    name = Column(String)
    commodity = Column(String)

    __tablename__ = DB_TABLE_TERMINAL


class Berth(Base):
    id = Column(String, unique=True, primary_key=True)
    port_unlocode = Column(String, ForeignKey(DB_TABLE_PORT + ".unlocode"))
    name = Column(String)
    commodity = Column(String)
    owner = Column(String)
    geometry = Column(Geometry("GEOMETRY", srid=4326))

    __tablename__ = DB_TABLE_BERTH


class STSLocation(Base):
    id = Column(String, unique=True, primary_key=True)
    name = Column(String)
    geometry = Column(Geometry("GEOMETRY", srid=4326))

    __tablename__ = DB_TABLE_STS_LOCATIONS


class ShipmentArrivalLocationSTS(Base):
    """
    For each shipment, lists the berth detected as well as the method used to find it
    """

    id = Column(BigInteger, autoincrement=True, primary_key=True)
    shipment_id = Column(BigInteger, unique=True)
    sts_location_id = Column(
        String,
        ForeignKey(DB_TABLE_STS_LOCATIONS + ".id", onupdate="CASCADE", ondelete="CASCADE"),
    )

    # Optional
    event_id = Column(BigInteger, ForeignKey(DB_TABLE_EVENT + ".id", onupdate="CASCADE"))
    method_id = Column(String)

    __tablename__ = DB_TABLE_STSARRIVALLOCATION
    __table_args__ = (
        UniqueConstraint(
            "shipment_id", "sts_location_id", name="unique_shipmentstsarrivallocation"
        ),
    )


class ShipmentDepartureLocationSTS(Base):
    """
    For each shipment, lists the berth detected as well as the method used to find it
    """

    id = Column(BigInteger, autoincrement=True, primary_key=True)
    shipment_id = Column(BigInteger, unique=True)
    sts_location_id = Column(
        String,
        ForeignKey(DB_TABLE_STS_LOCATIONS + ".id", onupdate="CASCADE", ondelete="CASCADE"),
    )

    # Optional
    event_id = Column(BigInteger, ForeignKey(DB_TABLE_EVENT + ".id", onupdate="CASCADE"))
    method_id = Column(String)

    __tablename__ = DB_TABLE_STSDEPARTURELOCATION
    __table_args__ = (
        UniqueConstraint(
            "shipment_id", "sts_location_id", name="unique_shipmentstsdeparturelocation"
        ),
    )


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


class ShipmentDepartureBerth(Base):
    """
    For each shipment, lists the berth detected as well as the method used to find it
    """

    id = Column(BigInteger, autoincrement=True, primary_key=True)
    shipment_id = Column(BigInteger, unique=True)
    berth_id = Column(
        String,
        ForeignKey(DB_TABLE_BERTH + ".id", onupdate="CASCADE", ondelete="CASCADE"),
    )

    # Optional
    position_id = Column(BigInteger, ForeignKey(DB_TABLE_POSITION + ".id", onupdate="CASCADE"))
    method_id = Column(String)

    __tablename__ = DB_TABLE_SHIPMENTDEPARTUREBERTH
    __table_args__ = (
        UniqueConstraint("shipment_id", "berth_id", name="unique_shipmentdepartureberth"),
    )


class ShipmentArrivalBerth(Base):
    """
    For each shipment, lists the berth detected as well as the method used to find it
    """

    id = Column(BigInteger, autoincrement=True, primary_key=True)
    shipment_id = Column(BigInteger, unique=True)
    berth_id = Column(
        String,
        ForeignKey(DB_TABLE_BERTH + ".id", onupdate="CASCADE", ondelete="CASCADE"),
    )

    # Optional
    position_id = Column(BigInteger, ForeignKey(DB_TABLE_POSITION + ".id", onupdate="CASCADE"))
    method_id = Column(String)

    __tablename__ = DB_TABLE_SHIPMENTARRIVALBERTH
    __table_args__ = (
        UniqueConstraint("shipment_id", "berth_id", name="unique_shipmentarrivalberth"),
    )


class Departure(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    port_id = Column(BigInteger, ForeignKey(DB_TABLE_PORT + ".id"))
    ship_imo = Column(
        String,
        ForeignKey(DB_TABLE_SHIP + ".imo", onupdate="CASCADE", ondelete="CASCADE"),
    )
    date_utc = Column(DateTime(timezone=False))
    method_id = Column(String)  # Method through which we detected the departure
    portcall_id = Column(BigInteger, ForeignKey(DB_TABLE_PORTCALL + ".id"), unique=True)
    event_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_EVENT + ".id", onupdate="CASCADE"),
        nullable=True,
    )

    __tablename__ = DB_TABLE_DEPARTURE

    __table_args__ = (UniqueConstraint("port_id", "ship_imo", "date_utc", name="unique_departure"),)


class Arrival(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    departure_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_DEPARTURE + ".id", onupdate="CASCADE"),
        unique=False,
    )
    date_utc = Column(DateTime(timezone=False))
    method_id = Column(String)
    port_id = Column(BigInteger, ForeignKey(DB_TABLE_PORT + ".id"))
    event_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_EVENT + ".id", onupdate="CASCADE"),
        nullable=True,
        unique=True,
    )

    portcall_id = Column(BigInteger, ForeignKey(DB_TABLE_PORTCALL + ".id"), unique=False)

    # We add next departure to have a detected date
    nextdeparture_portcall_id = Column(
        BigInteger, ForeignKey(DB_TABLE_PORTCALL + ".id"), unique=False
    )

    __tablename__ = DB_TABLE_ARRIVAL


class Shipment(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    departure_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_DEPARTURE + ".id", onupdate="CASCADE"),
        unique=True,
    )
    arrival_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_ARRIVAL + ".id", onupdate="CASCADE"),
        unique=True,
    )
    last_position_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_POSITION + ".id", onupdate="CASCADE"),
        unique=True,
    )

    last_destination_name = Column(String)
    status = Column(String)

    # Storing all distinct destinations
    destination_names = Column(ARRAY(String))
    destination_dates = Column(ARRAY(DateTime(timezone=False)))
    destination_iso2s = Column(ARRAY(String))

    __tablename__ = DB_TABLE_SHIPMENT
    __table_args__ = (Index("idx_shipment_departure_id", "departure_id"),)


class ShipmentWithSTS(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    departure_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_DEPARTURE + ".id", onupdate="CASCADE"),
        unique=False,
    )
    arrival_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_ARRIVAL + ".id", onupdate="CASCADE"),
        unique=False,
    )
    last_position_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_POSITION + ".id", onupdate="CASCADE"),
        unique=False,
    )

    last_destination_name = Column(String)
    status = Column(String)

    # Storing all distinct destinations
    destination_names = Column(ARRAY(String))
    destination_dates = Column(ARRAY(DateTime(timezone=False)))
    destination_iso2s = Column(ARRAY(String))

    __tablename__ = DB_TABLE_SHIPMENT_WITH_STS
    __table_args__ = (
        Index("idx_shipment_with_sts_departure_id", "departure_id"),
        Index("idx_shipment_with_sts_arrival_id", "arrival_id"),
    )


class Company(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    imo = Column(String)
    name = Column(String, nullable=False)
    names = Column(ARRAY(String))
    address = Column(String)
    addresses = Column(ARRAY(String))
    country_iso2 = Column(String, ForeignKey(DB_TABLE_COUNTRY + ".iso2"))
    registration_country_iso2 = Column(String, ForeignKey(DB_TABLE_COUNTRY + ".iso2"))

    __tablename__ = DB_TABLE_COMPANY
    __table_args__ = (UniqueConstraint("imo", name="unique_company_imo"),)


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


class Position(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    ship_imo = Column(String, ForeignKey(DB_TABLE_SHIP + ".imo", onupdate="CASCADE"))
    date_utc = Column(
        DateTime(timezone=False)
    )  # Departure time for departure, Arrival time for arrival
    geometry = Column(Geometry("POINT", srid=4326))
    navigation_status = Column(String)
    speed = Column(Numeric)
    destination_name = Column(String)
    destination_port_id = Column(BigInteger, ForeignKey(DB_TABLE_PORT + ".id", onupdate="CASCADE"))

    __tablename__ = DB_TABLE_POSITION
    __table_args__ = (
        Index("idx_position_ship_imo", "ship_imo"),
        UniqueConstraint("ship_imo", "date_utc", name="unique_position"),
    )


class Destination(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    name = Column(String)
    source = Column(String)
    port_id = Column(BigInteger, ForeignKey(DB_TABLE_PORT + ".id", onupdate="CASCADE"))
    iso2 = Column(String)
    method = Column(String)
    type = Column(String)

    __tablename__ = DB_TABLE_DESTINATION
    __table_args__ = (
        Index("idx_destination_name", "name"),
        UniqueConstraint("name", name="unique_destination"),
    )


class Trajectory(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    shipment_id = Column(BigInteger, unique=True)
    geometry = Column(Geometry("MULTILINESTRING", srid=4326))
    geometry_routed = Column(Geometry("MULTILINESTRING", srid=4326))
    routing_date = Column(DateTime(timezone=False))  # time where geometry_routed was built

    __tablename__ = DB_TABLE_TRAJECTORY
    __table_args__ = (Index("idx_trajectory_shipment", "shipment_id"),)


class PortCall(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    ship_mmsi = Column(String)  # , ForeignKey(DB_TABLE_SHIP + '.mmsi', onupdate="CASCADE"))
    ship_imo = Column(String, ForeignKey(DB_TABLE_SHIP + ".imo", onupdate="CASCADE"))
    date_utc = Column(
        DateTime(timezone=False)
    )  # Departure time for departure, Arrival time for arrival
    date_lt = Column(DateTime(timezone=False))  # local time

    port_id = Column(BigInteger, ForeignKey(DB_TABLE_PORT + ".id", onupdate="CASCADE"))

    load_status = Column(String)  # (0 : N/A, 1 : In Ballast, 2 : Partially Laden, 3 : Fully Laden)
    move_type = Column(String)  # "1": "departure", "0":"arrival"
    port_operation = Column(String)  # (0: N / A, 1: load, 2: discharge, 3: both, 4: none)
    draught = Column(Numeric)

    # Optional
    terminal_id = Column(String, ForeignKey(DB_TABLE_TERMINAL + ".id", onupdate="CASCADE"))
    berth_id = Column(String, ForeignKey(DB_TABLE_BERTH + ".id", onupdate="CASCADE"))

    # To store the whole repsonse in case we missed something
    others = Column(JSONB)

    created_at = Column(DateTime(timezone=False), default=dt.datetime.utcnow)

    __tablename__ = DB_TABLE_PORTCALL
    __table_args__ = (
        UniqueConstraint("ship_imo", "date_utc", "move_type", name="unique_portcall"),
        Index("idx_portcall_ship_imo", "ship_imo"),
    )

    @validates("port_unlocode")
    def validate_port_unlocode(self, key, port_unlocode):
        if port_unlocode == "":
            port_unlocode = None
        return port_unlocode

    @validates("load_status")
    def validate_load_status(self, key, load_status):
        corr = {
            "0": "na",
            "1": base.IN_BALLAST,
            "2": base.PARTIALLY_LADEN,
            "3": base.FULLY_LADEN,
        }
        if load_status is None:
            return None
        if load_status in corr.values():
            return load_status
        if not load_status in corr.keys():
            logger.warning("Unknown load status: %s" % (load_status,))
        return corr.get(load_status, load_status)

    @validates("move_type")
    def validate_move_type(self, key, move_type):
        corr = {"0": "arrival", "1": "departure"}
        if move_type is None:
            return None
        if move_type in corr.values():
            return move_type
        if not move_type in corr.keys():
            logger.warning("Unknown move type: %s" % (move_type,))
        return corr.get(move_type, move_type)

    @validates("port_operation")
    def validate_port_operation(self, key, port_operation):
        corr = {"0": "na", "1": "load", "2": "discharge", "3": "both", "4": None}
        if port_operation is None:
            return None
        if port_operation in corr.values():
            return port_operation
        if not port_operation in corr.keys():
            logger.warning("Unknown port_operation: %s" % (port_operation,))
        return corr.get(port_operation, port_operation)


class MTVoyageInfo(Base):
    """{
        "MMSI": "310627000",
        "DESTINATION": "TORQUAY",
        "LAST_PORT_ID": "106",
        "LAST_PORT": "SOUTHAMPTON",
        "LAST_PORT_UNLOCODE": "GBSOU",
        "LAST_PORT_TIME": "2020-10-14T17:00:00.000Z",
        "NEXT_PORT_ID": "10379",
        "NEXT_PORT_NAME": "TORQUAY",
        "NEXT_PORT_UNLOCODE": "GBTOR",
        "ETA": "2020-10-14T13:00:00.000Z",
        "ETA_CALC": ""
    }"""

    id = Column(BigInteger, autoincrement=True, primary_key=True)
    ship_mmsi = Column(String)  # , ForeignKey(DB_TABLE_SHIP + '.mmsi', onupdate="CASCADE"))
    ship_imo = Column(String, ForeignKey(DB_TABLE_SHIP + ".imo", onupdate="CASCADE"))
    queried_date_utc = Column(
        DateTime(timezone=False)
    )  # Departure time for departure, Arrival time for arrival
    destination_name = Column(String)
    next_port_name = Column(String)
    next_port_unlocode = Column(String)
    others = Column(JSONB)

    __tablename__ = DB_TABLE_MTVOYAGEINFO


class Price(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)

    commodity = Column(String, ForeignKey(DB_TABLE_COMMODITY + ".id"), nullable=False)
    date = Column(DateTime(timezone=False))
    eur_per_tonne = Column(Numeric)
    scenario = Column(String, nullable=False)

    destination_iso2s = Column(ARRAY(String))
    departure_port_ids = Column(ARRAY(BigInteger))
    ship_owner_iso2s = Column(ARRAY(String))
    ship_insurer_iso2s = Column(ARRAY(String))

    updated_on = Column(DateTime, server_default=func.now(), server_onupdate=func.now())

    __tablename__ = DB_TABLE_PRICE
    __table_args__ = (
        UniqueConstraint(
            "destination_iso2s",
            "departure_port_ids",
            "ship_owner_iso2s",
            "ship_insurer_iso2s",
            "date",
            "commodity",
            "scenario",
            name="unique_price",
        ),
        CheckConstraint("eur_per_tonne >= 0", name="price_positive"),
        Index("idx_price_commodity", "commodity"),
        Index("idx_price_date", "date"),
        Index("idx_price_date_commodity", "date", "commodity"),
        Index("idx_price_destination_iso2s", "destination_iso2s", postgresql_using="gin"),
        Index("idx_price_departure_port_ids", "departure_port_ids", postgresql_using="gin"),
        Index("idx_price_ship_owner_iso2s", "ship_owner_iso2s", postgresql_using="gin"),
        Index("idx_price_ship_insurer_iso2s", "ship_insurer_iso2s", postgresql_using="gin"),
        # To add in SqlAlchemy format
        # CREATE INDEX IF NOT EXISTS idx_price_date_noshipinfo
        #     ON public.price USING btree
        #       (date ASC NULLS LAST,
        # 		(departure_port_ids = ARRAY[NULL::bigint]) ASC NULLS LAST,
        # 	    (ship_insurer_iso2s = ARRAY[NULL::varchar]) ASC NULLS LAST,
        # 	    (ship_owner_iso2s = ARRAY[NULL::varchar]) ASC NULLS LAST
        # 	)
        #     TABLESPACE pg_default;
    )


class PriceScenario(Base):
    id = Column(String, primary_key=True)
    name = Column(String)
    others = Column(JSONB)

    __tablename__ = DB_TABLE_PRICE_SCENARIO


# ENTSOG data after outer join flows
# But before filtering them
# Used to prevent querying ENTSOG all the time when
# selecting OPDs
class EntsogFlowRaw(Base):
    id = Column(String, primary_key=True)
    date = Column(DateTime(timezone=False))
    periodFrom = Column(DateTime(timezone=False))
    periodTo = Column(DateTime(timezone=False))
    pointKey = Column(String)
    operatorKey = Column(String)
    directionKey = Column(String)
    flowStatus = Column(String)
    value_kwh = Column(Numeric)
    gcv_kwh_m3 = Column(Numeric)

    updated_on = Column(DateTime, server_default=func.now(), server_onupdate=func.now())
    __tablename__ = DB_TABLE_ENTSOGFLOW_RAW
    __table_args__ = (Index("idx_entsogflow_raw_pointkey", "pointKey"),)


# Entsog flows: before processing
# Mainly used to communicate between Python and R
# And also avoid recollecting everytime
class EntsogFlow(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    commodity = Column(String, ForeignKey(DB_TABLE_COMMODITY + ".id"), nullable=False)
    departure_iso2 = Column(String)
    destination_iso2 = Column(String)
    date = Column(DateTime(timezone=False))
    value_tonne = Column(Numeric)
    value_mwh = Column(Numeric)
    value_m3 = Column(Numeric)

    type = Column(String, nullable=False)

    updated_on = Column(DateTime, server_default=func.now(), server_onupdate=func.now())

    __tablename__ = DB_TABLE_ENTSOGFLOW
    __table_args__ = (
        UniqueConstraint(
            "date",
            "commodity",
            "departure_iso2",
            "destination_iso2",
            "type",
            name="unique_entsogflow",
        ),
    )


class PipelineFlow(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    commodity = Column(String, ForeignKey(DB_TABLE_COMMODITY + ".id"), nullable=False)

    departure_iso2 = Column(String)
    destination_iso2 = Column(String)

    date = Column(DateTime(timezone=False))
    value_tonne = Column(Numeric)
    value_mwh = Column(Numeric)
    value_m3 = Column(Numeric)

    updated_on = Column(DateTime, server_default=func.now(), server_onupdate=func.now())

    __tablename__ = DB_TABLE_PIPELINEFLOW
    __table_args__ = (
        UniqueConstraint(
            "date",
            "commodity",
            "departure_iso2",
            "destination_iso2",
            name="unique_pipelineflow",
        ),
    )


class Counter(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    commodity = Column(String, ForeignKey(DB_TABLE_COMMODITY + ".id"), nullable=False)
    destination_iso2 = Column(String, ForeignKey(DB_TABLE_COUNTRY + ".iso2"))
    date = Column(DateTime(timezone=False))
    value_tonne = Column(Numeric)
    value_eur = Column(Numeric)
    pricing_scenario = Column(String)
    updated_on = Column(DateTime, server_default=func.now(), onupdate=func.now())
    version = Column(String, nullable=False)

    __tablename__ = DB_TABLE_COUNTER
    __table_args__ = (
        UniqueConstraint(
            "date",
            "commodity",
            "destination_iso2",
            "pricing_scenario",
            "version",
            name="unique_counter",
        ),
        Index("idx_counter_date", "date"),
        Index("idx_counter_commodity", "commodity"),
        Index("idx_counter_pricing_scenario", "pricing_scenario"),
    )


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


class MarineTrafficCall(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    method = Column(String)
    records = Column(Integer)
    credits = Column(Integer)
    date_utc = Column(DateTime(timezone=False))
    params = Column(JSONB)
    key = Column(String)
    status = Column(String)

    __tablename__ = DB_TABLE_MARINETRAFFICCALL


class Currency(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    date = Column(Date)
    currency = Column(String)
    per_eur = Column(Numeric)  # All currencies
    estimated = Column(Boolean)  # Whether it is actual value or estimated (useful when API is down)

    __table_args__ = (UniqueConstraint("date", "currency", name="unique_currency"),)
    __tablename__ = DB_TABLE_CURRENCY


class MarineTrafficEventType(Base):
    id = Column(String, unique=True, primary_key=True)
    name = Column(String)
    description = Column(String)
    availability = Column(String)

    __table_args__ = (UniqueConstraint("id", name="unique_event_type_id"),)
    __tablename__ = DB_TABLE_MTEVENT_TYPE


class Event(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    ship_name = Column(String)
    ship_imo = Column(String, ForeignKey(DB_TABLE_SHIP + ".imo", onupdate="CASCADE"))
    ship_closest_position = Column(Geometry("POINT", srid=4326))
    interacting_ship_name = Column(String)
    interacting_ship_imo = Column(String, ForeignKey(DB_TABLE_SHIP + ".imo", onupdate="CASCADE"))
    interacting_ship_closest_position = Column(Geometry("POINT", srid=4326))
    interacting_ship_details = Column(JSONB)  # details about ship from datalastic query
    date_utc = Column(DateTime(timezone=False))
    created_at = Column(DateTime(timezone=False), default=dt.datetime.utcnow)
    type_id = Column(String, ForeignKey(DB_TABLE_MTEVENT_TYPE + ".id", onupdate="CASCADE"))
    content = Column(JSONB)
    source = Column(String, default="marinetraffic")

    __table_args__ = (
        UniqueConstraint("ship_imo", "interacting_ship_imo", "date_utc", name="unique_event"),
    )
    __tablename__ = DB_TABLE_EVENT


#############################
# Alert related models
#############################


class AlertConfig(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    name = Column(String, nullable=False)
    frequency = Column(String)
    sending_time_utc = Column(Time)
    sending_day_of_week = Column(Integer)  # 1: Monday 7: Sunday

    __tablename__ = DB_TABLE_ALERT_CONFIG


class AlertRecipient(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    recipient = Column(String)
    type = Column(String)  # MAIL, SMS, SLACK
    others = Column(JSONB)  # In case specific parameters are required

    __tablename__ = DB_TABLE_ALERT_RECIPIENT


class AlertRecipientAssociation(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    config_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_ALERT_CONFIG + ".id", ondelete="CASCADE"),
        nullable=False,
    )
    recipient_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_ALERT_RECIPIENT + ".id", ondelete="CASCADE"),
        nullable=False,
    )

    __tablename__ = DB_TABLE_ALERT_RECIPIENT_ASSOC


class AlertCriteria(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    # List of conditions joined by AND operation
    # If null, then ignored
    commodity = Column(ARRAY(String))
    min_dwt = Column(Numeric)
    new_destination_iso2 = Column(ARRAY(String))
    new_destination_name_pattern = Column(ARRAY(String))

    departure_port_ids = Column(ARRAY(BigInteger))  # If null, all ports considered
    shipment_status = Column(ARRAY(String))  # If null, all statuses are included

    __tablename__ = DB_TABLE_ALERT_CRITERIA


class AlertCriteriaAssociation(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    config_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_ALERT_CONFIG + ".id", ondelete="CASCADE"),
        nullable=False,
    )
    criteria_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_ALERT_CRITERIA + ".id", ondelete="CASCADE"),
        nullable=False,
    )

    __tablename__ = DB_TABLE_ALERT_CRITERIA_ASSOC


class AlertInstance(Base):
    """Instances of alert content sent to user"""

    id = Column(BigInteger, autoincrement=True, primary_key=True)
    config_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_ALERT_CONFIG + ".id", ondelete="CASCADE"),
        nullable=False,
    )
    recipient_id = Column(
        BigInteger,
        ForeignKey(DB_TABLE_ALERT_RECIPIENT + ".id", ondelete="CASCADE"),
        nullable=False,
    )
    ship_imos = Column(ARRAY(String))
    content = Column(JSONB)
    date_utc = Column(DateTime(timezone=False), default=dt.datetime.utcnow)

    sent_date_utc = Column(DateTime(timezone=False))

    __tablename__ = DB_TABLE_ALERT_INSTANCE


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


class ApiKey(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    key = Column(String)
    user_id = Column(BigInteger)
    endpoints = Column(ARRAY(String))

    __tablename__ = DB_TABLE_API_KEY


class GlobalCache(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(JSONB)

    __tablename__ = DB_TABLE_GLOBAL_CACHE
