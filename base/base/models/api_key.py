from sqlalchemy import (
    Column,
    String,
    BigInteger,
)
from sqlalchemy.dialects.postgresql import ARRAY
from base.db import Base

from .table_names import *


class ApiKey(Base):
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    key = Column(String)
    user_id = Column(BigInteger)
    endpoints = Column(ARRAY(String))

    __tablename__ = DB_TABLE_API_KEY
