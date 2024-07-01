from sqlalchemy import Column, String, DateTime, Numeric, BigInteger
from sqlalchemy.orm import validates
from sqlalchemy import UniqueConstraint, ForeignKey, Index, func

from base.db import Base

from .table_names import *


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
