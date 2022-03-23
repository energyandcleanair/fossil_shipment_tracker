import os
import pytest


os.environ["ENVIRONMENT"] = "test"
from base.db import init_db




@pytest.fixture
def test_db():
    assert os.environ.get('ENVIRONMENT') == "test"

    # Erase all content and create tables
    init_db(drop_first=True)