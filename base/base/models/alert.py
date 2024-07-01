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
