from sqlalchemy import Column, String, DateTime, Numeric, BigInteger, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import validates
from sqlalchemy import UniqueConstraint, ForeignKey
from geoalchemy2 import Geometry

from base.db import Base
from base.logger import logger

from . import DB_TABLE_PORTCALL
from . import DB_TABLE_DEPARTURE
from . import DB_TABLE_ARRIVAL
from . import DB_TABLE_SHIP
from . import DB_TABLE_PORT
from . import DB_TABLE_TERMINAL
from . import DB_TABLE_BERTH
from . import DB_TABLE_POSITION
from . import DB_TABLE_FLOW


class Ship(Base):
    imo = Column(String, primary_key=True)
    mmsi = Column(String)
    name = Column(String)
    type = Column(String)
    subtype = Column(String)
    dwt = Column(Numeric) # in tonnes

    country_iso2 = Column(String)
    country_name = Column(String)
    home_port = Column(String)
    liquid_gas = Column(Numeric)
    liquid_oil = Column(Numeric)
    others = Column(JSONB)

    # Estimated commodity, quantity etc
    commodity = Column(String)
    quantity = Column(Numeric)
    unit = Column(String)

    __tablename__ = DB_TABLE_SHIP

    @validates('liquid_oil')
    def validate_liquid_oil(self, key, liquid_oil):
        try:
            return float(liquid_oil)
        except (ValueError, TypeError):
            return None

    @validates('liquid_gas')
    def validate_liquid_gas(self, key, liquid_gas):
        try:
            return float(liquid_gas)
        except (ValueError, TypeError):
            return None


class Port(Base):
    unlocode = Column(String, primary_key=True)
    name = Column(String)
    iso2 = Column(String)
    check_departure = Column(Boolean)
    check_arrival = Column(Boolean)
    geometry = Column(Geometry('POINT', srid=4326))

    __tablename__ = DB_TABLE_PORT


class Terminal(Base):
    id = Column(String, unique=True, primary_key=True)
    port_unlocode = Column(String, ForeignKey(DB_TABLE_PORT + '.unlocode'))
    name = Column(String)
    commodity = Column(String)

    __tablename__ = DB_TABLE_TERMINAL


class Berth(Base):
    id = Column(String, unique=True, primary_key=True)
    port_unlocode = Column(String, ForeignKey(DB_TABLE_PORT + '.unlocode'))
    name = Column(String)
    commodity = Column(String)

    __tablename__ = DB_TABLE_BERTH


class Departure(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    port_unlocode = Column(String, ForeignKey(DB_TABLE_PORT + '.unlocode'))
    ship_imo = Column(String, ForeignKey(DB_TABLE_SHIP + '.imo'))
    date_utc = Column(DateTime(timezone=False))
    method_id = Column(String) # Method through which we detected the departure
    portcall_id = Column(BigInteger, ForeignKey(DB_TABLE_PORTCALL + '.id'))

    __tablename__ = DB_TABLE_DEPARTURE

    __table_args__ = (UniqueConstraint('port_unlocode', 'ship_imo', 'date_utc', name='unique_departure'),
                      )


class Arrival(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    departure_id = Column(BigInteger, ForeignKey(DB_TABLE_DEPARTURE + '.id', onupdate="CASCADE"))
    date_utc = Column(DateTime(timezone=False))
    method_id = Column(String)
    port_unlocode = Column(String, ForeignKey(DB_TABLE_PORT + '.unlocode'))

    # Optional
    portcall_id = Column(BigInteger, ForeignKey(DB_TABLE_PORTCALL + '.id'))
    __tablename__ = DB_TABLE_ARRIVAL


class Flow(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    departure_id = Column(BigInteger, ForeignKey(DB_TABLE_DEPARTURE + '.id', onupdate="CASCADE"))
    arrival_id = Column(BigInteger, ForeignKey(DB_TABLE_ARRIVAL + '.id', onupdate="CASCADE"))

    __tablename__ = DB_TABLE_FLOW


class Position(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    ship_imo = Column(String, ForeignKey(DB_TABLE_SHIP + '.imo', onupdate="CASCADE"))
    date_utc = Column(DateTime(timezone=False))  # Departure time for departure, Arrival time for arrival
    geometry = Column(Geometry('POINT', srid=4326))

    __tablename__ = DB_TABLE_POSITION


# MarineTraffic only
class PortCall(Base):
    """
    Copied from MarineTraffic. Could also be Berth call
    Example of returned data:

   {
        "MMSI": "244770588",
        "SHIPNAME": "PIETERNELLA",
        "SHIP_ID": "3351323",
        "TIMESTAMP_LT": "2020-10-20T12:15:00.000Z",
        "TIMESTAMP_UTC": "2020-10-20T10:15:00.000Z",
        "MOVE_TYPE": "0",
        "TYPE_NAME": "Inland, Unknown",
        "PORT_ID": "1766",
        "PORT_NAME": "AMSTERDAM",
        "UNLOCODE": "NLAMS",
        "DRAUGHT": "59",
        "LOAD_STATUS": "0",
        "PORT_OPERATION": "0",
        "INTRANSIT": "0",
        "DISTANCE_TRAVELLED": "0",
        "VOYAGE_SPEED_AVG": null,
        "VOYAGE_SPEED_MAX": null,
        "VOYAGE_IDLE_TIME_MINS": null,
        "ELAPSED_NOANCH": "672",
        "DISTANCE_LEG": null,
        "COMFLEET_GROUPEDTYPE": "DRY BREAKBULK",
        "SHIPCLASS": null
    }
    """

    id = Column(BigInteger, autoincrement=True, primary_key=True)
    ship_mmsi = Column(String) #, ForeignKey(DB_TABLE_SHIP + '.mmsi', onupdate="CASCADE"))
    ship_imo = Column(String, ForeignKey(DB_TABLE_SHIP + '.imo', onupdate="CASCADE"))
    date_utc = Column(DateTime(timezone=False)) # Departure time for departure, Arrival time for arrival
    port_unlocode = Column(String, ForeignKey(DB_TABLE_PORT + '.unlocode', onupdate="CASCADE"))

    load_status = Column(String)  # (0 : N/A, 1 : In Ballast, 2 : Partially Laden, 3 : Fully Laden)
    move_type = Column(String)  # "1": "departure", "0":"arrival"
    port_operation = Column(String) # (0: N / A, 1: load, 2: discharge, 3: both, 4: none)


    # Optional
    terminal_id = Column(String, ForeignKey(DB_TABLE_TERMINAL + '.id', onupdate="CASCADE"))
    berth_id = Column(String, ForeignKey(DB_TABLE_BERTH + '.id', onupdate="CASCADE"))

    # To store the whole repsonse in case we missed something
    others = Column(JSONB)

    __tablename__ = DB_TABLE_PORTCALL
    __table_args__ = (UniqueConstraint('ship_imo', 'date_utc', 'move_type', name='unique_portcall'),)


    @validates('port_unlocode')
    def validate_port_unlocode(self, key, port_unlocode):
        if port_unlocode == "":
            port_unlocode = None
        return port_unlocode

    @validates('load_status')
    def validate_load_status(self, key, load_status):
        corr = {
            "0": "na",
            "1": "in_ballast",
            "2": "partially_laden",
            "3": "fully_laden",
        }
        if load_status is None:
            return None

        if not load_status in corr.keys():
            logger.warning("Unknown load status: %s" % (load_status,))
        return corr.get(load_status, load_status)

    @validates('move_type')
    def validate_move_type(self, key, move_type):
        corr = {
            "0": "arrival",
            "1": "departure"
        }
        if move_type is None:
            return None

        if not move_type in corr.keys():
            logger.warning("Unknown move type: %s" % (move_type,))
        return corr.get(move_type, move_type)

    @validates('port_operation')
    def validate_port_operation(self, key, port_operation):
        corr = {
            "0": "na",
            "1": "load",
            "2": "discharge",
            "3": "both",
            "4": None
        }
        if port_operation is None:
            return None

        if not port_operation in corr.keys():
            logger.warning("Unknown port_operation: %s" % (port_operation,))
        return corr.get(port_operation, port_operation)

