from sqlalchemy import Column, String, DateTime, Numeric, BigInteger
from sqlalchemy import UniqueConstraint, ForeignKey
from geoalchemy2 import Geometry

from base.db import Base

from . import DB_TABLE_PORTCALL
from . import DB_TABLE_DEPARTURE
from . import DB_TABLE_ARRIVAL
from . import DB_TABLE_SHIP
from . import DB_TABLE_PORT
from . import DB_TABLE_TERMINAL

class Ship(Base):
    mmsi = Column(String, unique=True, primary_key=True)
    name = Column(String)
    imo = Column(String)
    type = Column(String)
    dwt = Column(Numeric) # in tonnes
    martinetraffic_id = Column(String)

    __table_args__ = {'extend_existing': True}
    __tablename__ = DB_TABLE_SHIP


class Port(Base):
    unlocode = Column(String, unique=True, primary_key=True)
    name = Column(String)
    iso2 = Column(String)
    geometry = Column(Geometry('POINT', srid=4326))


    __table_args__ = {'extend_existing': True}
    __tablename__ = DB_TABLE_PORT


class Terminal(Base):
    id = Column(String, unique=True, primary_key=True)
    port_id = Column(String, ForeignKey(DB_TABLE_PORT + '.unlocode'))
    name = Column(String)
    commodity = Column(String)

    __table_args__ = {'extend_existing': True}
    __tablename__ = DB_TABLE_TERMINAL


class Departure(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    port_id = Column(String, ForeignKey(DB_TABLE_PORT + '.unlocode'))
    ship_mmsi = Column(String, ForeignKey(DB_TABLE_SHIP + '.mmsi'))
    date_utc = Column(DateTime(timezone=False))
    method_id = Column(String) # Method through which we detected the departure
    portcall_id = Column(BigInteger, ForeignKey(DB_TABLE_PORTCALL + '.id'))

    __tablename__ = DB_TABLE_DEPARTURE

    __table_args__ = (UniqueConstraint('port_id', 'ship_mmsi', 'date_utc', name='unique_departure'),{'extend_existing':True}
                      )


class Arrival(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    departure_id = Column(BigInteger, ForeignKey(DB_TABLE_DEPARTURE + '.id', onupdate="CASCADE"))
    method_id = Column(String)

    __table_args__ = {'extend_existing': True}
    __tablename__ = DB_TABLE_ARRIVAL


class PortCall(Base):
    """
    Copied from MarineTraffic
    Example of returned data:

    "MMSI": "244690666",
    "SHIPNAME": "BRABANT",
    "SHIP_ID": "241767",
    "TIMESTAMP_LT": "2020-10-20T12:14:00.000Z",
    "TIMESTAMP_UTC": "2020-10-20T10:14:00.000Z",
    "MOVE_TYPE": "1",
    "TYPE_NAME": "Inland, Motor Freighter",
    "PORT_ID": "1766",
    "PORT_NAME": "AMSTERDAM",
    "UNLOCODE": "NLAMS"
    """
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    ship_mmsi = Column(String, ForeignKey(DB_TABLE_SHIP + '.mmsi', onupdate="CASCADE"))
    date_lt = Column(DateTime(timezone=False))
    date_utc = Column(DateTime(timezone=False))
    move_type = Column(String)
    type_name = Column(String)  # iso2
    port_unlocode = Column(String, ForeignKey(DB_TABLE_PORT + '.unlocode', onupdate="CASCADE"))

    __tablename__ = DB_TABLE_PORTCALL
    __table_args__ = (UniqueConstraint('ship_mmsi', 'date_lt', name='unique_portcall'),{'extend_existing':True}
                      )
