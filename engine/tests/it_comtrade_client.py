from base.env import get_env
import pandas as pd
import pytest

from engines.comtrade import ComtradeClient

from datetime import date


@pytest.mark.integration
def test_ComtradeClient_get_data_availability():
    client = ComtradeClient(api_key=get_env("COMTRADE_API_KEY"))

    availability = client.get_data_availability(start="2021-01-01", end="2021-03-31")

    assert not availability.empty
    assert availability["period"].min() == pd.Period("2021-01", freq="M")
    assert availability["period"].max() == pd.Period("2021-03", freq="M")
    assert ["reporterISO", "period", "lastReleased"] == availability.columns.tolist()
    # Assert reporterISO character lengths are 2
    assert all(availability["reporterISO"].str.len() == 2)
    # Assert that period type is pd.Period
    assert all(availability["period"].apply(type) == pd.Period)
    # Assert that lastReleased is Date or None
    assert all(availability["lastReleased"].apply(lambda x: x is None or isinstance(x, date)))
