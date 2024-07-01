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
