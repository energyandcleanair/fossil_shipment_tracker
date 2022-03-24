from sqlalchemy import Column, String, DateTime, Numeric, BigInteger, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import validates
from sqlalchemy import UniqueConstraint, ForeignKey
from geoalchemy2 import Geometry

from base.db import Base

from . import DB_TABLE_PORTCALL
from . import DB_TABLE_DEPARTURE
from . import DB_TABLE_ARRIVAL
from . import DB_TABLE_SHIP
from . import DB_TABLE_PORT
from . import DB_TABLE_TERMINAL
from . import DB_TABLE_BERTH
from . import DB_TABLE_POSITION


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
    port_unlocode = Column(String, ForeignKey(DB_TABLE_TERMINAL + '.id'))
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
    method_id = Column(String)

    __tablename__ = DB_TABLE_ARRIVAL


class PortCall(Base):
    """
    Copied from MarineTraffic. Could also be Berth call
    Example of returned data:


    "SHIP_ID": "5567750",
    "MMSI": "237710500",
    "IMO": "0",
    "DOCK_TIMESTAMP_LT": "2020-10-20T06:22:00.000Z",
    "DOCK_TIMESTAMP_UTC": "2020-10-20T03:22:00.000Z",
    "DOCK_TIMESTAMP_OFFSET": "3.000000",
    "UNDOCK_TIMESTAMP_LT": "2020-10-20T06:45:00.000Z",
    "UNDOCK_TIMESTAMP_UTC": "2020-10-20T03:45:00.000Z",
    "UNDOCK_TIMESTAMP_OFFSET": "3.000000",
    "SHIPNAME": "PILOT BOAT PY56",
    "TYPE_NAME": "Pilot Vessel",
    "DWT": null,
    "GRT": null,
    "FLAG": "GR",
    "YEAR_BUILT": null,
    "BERTH_ID": "6",
    "BERTH_NAME": "Container Terminal",
    "TERMINAL_ID": "978",
    "TERMINAL_NAME": "Container Terminal",
    "PORT_ID": "1",
    "PORT_NAME": "PIRAEUS",
    "UNLOCODE": "GRPIR",
    "COUNTRY_CODE": "GR",
    "DESTINATION_ID": "1",
    "DESTINATION": "PIRAEUS"
    """

    id = Column(BigInteger, autoincrement=True, primary_key=True)
    ship_mmsi = Column(String) #, ForeignKey(DB_TABLE_SHIP + '.mmsi', onupdate="CASCADE"))
    ship_imo = Column(String, ForeignKey(DB_TABLE_SHIP + '.imo', onupdate="CASCADE"))
    date_utc = Column(DateTime(timezone=False)) # Departure time for departure, Arrival time for arrival
    move_type = Column(String) # "departure" or "arrival"
    port_unlocode = Column(String, ForeignKey(DB_TABLE_PORT + '.unlocode', onupdate="CASCADE"))

    # Optional
    terminal_id = Column(String, ForeignKey(DB_TABLE_TERMINAL + '.id', onupdate="CASCADE"))
    berth_id = Column(String, ForeignKey(DB_TABLE_BERTH + '.id', onupdate="CASCADE"))

    __tablename__ = DB_TABLE_PORTCALL
    __table_args__ = (UniqueConstraint('ship_imo', 'date_utc', 'move_type', name='unique_portcall'),)


class Position(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    ship_imo = Column(String, ForeignKey(DB_TABLE_SHIP + '.imo', onupdate="CASCADE"))
    date_utc = Column(DateTime(timezone=False))  # Departure time for departure, Arrival time for arrival
    geometry = Column(Geometry('POINT', srid=4326))

    __tablename__ = DB_TABLE_POSITION
