from sqlalchemy import Column, String, DateTime, Numeric
from sqlalchemy import UniqueConstraint, ForeignKey


from base.db import Base
from . import DB_TABLE_PORTCALL
from . import DB_TABLE_DEPARTURE
from . import DB_TABLE_ARRIVAL


class Ship(Base):
    mmsi = Column(String, unique=True, primary_key=True)
    name = Column(String)
    imo = Column(String)
    type = Column(Numeric)
    dwt = Column(Numeric) # in tonnes
    martinetraffic_id = Column(String)
    pass


class Port(Base):
    pass


class Berth(Base):
    pass


class Departure(Base):
    method_id = Column(String)
    port_id = Column(String, ForeignKey(DB_TABLE_DEPARTURE + '.id'))
    pass


class Arrival(Base):
    departure_id = Column(String, ForeignKey(DB_TABLE_ARRIVAL + '.id', onupdate="CASCADE"))
    method_id = Column(String)
    pass


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

    MMSI = Column(String)
    SHIPNAME = Column(String)
    SHIP_ID = Column(String)
    TIMESTAMP_LT = Column(DateTime(timezone=False))
    TIMESTAMP_UTC = Column(DateTime(timezone=False))
    MOVE_TYPE = Column(String)
    TYPE_NAME = Column(String)  # iso2
    PORT_ID = Column(String)
    PORT_NAME = Column(String)
    UNLOCODE = Column(String)

    __tablename__ = DB_TABLE_PORTCALL

    __table_args__ = (UniqueConstraint('MMSI', 'TIMESTAMP_LT', name='unique_portcall'),
                      )