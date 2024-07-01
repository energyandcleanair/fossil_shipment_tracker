from sqlalchemy import (
    Column,
    String,
    DateTime,
    Numeric,
    BigInteger,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy import UniqueConstraint, CheckConstraint, ForeignKey, Index, func

from base.db import Base

from .table_names import *


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
