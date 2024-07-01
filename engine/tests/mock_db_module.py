import sys

from unittest.mock import Mock
import types

from sqlalchemy.ext.declarative import declarative_base

module_name = "base.db"
module = types.ModuleType(module_name)
sys.modules[module_name] = module
module.engine = Mock(name="base.db.engine")
module.session = Mock(name="base.db.session")
module.environment = Mock(name="base.db.environment")
module.meta = Mock(name="base.db.meta")
module.check_if_table_exists = Mock(name="base.db.check_if_table_exists")
module.Base = declarative_base()
