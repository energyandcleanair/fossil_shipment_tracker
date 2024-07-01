from sqlalchemy import (
    Column,
    String,
    BigInteger,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import validates
from sqlalchemy import UniqueConstraint, ForeignKey

from base.db import Base

from .table_names import *


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
