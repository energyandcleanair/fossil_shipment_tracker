import os
import pytest


# os.environ["ENVIRONMENT"] = "development"


@pytest.fixture
def app():
    from app import app
    return app