from engines.company_scraper.accounts import EquasisAccount
from engines.company_scraper.session_management import (
    EquasisSessionPoolExhausted,
    OnDemandEquasisSessionManager,
)
from .mock_db_module import *

from datetime import date
from unittest.mock import MagicMock
from engines.company_scraper import (
    EquasisClient,
    EquasisSession,
    EquasisFixedInitialisationSessionPool,
    EquasisSessionUnavailable,
)
import pytest
import responses
import requests

from responses.registries import OrderedRegistry
import pandas as pd
from pandas.testing import assert_frame_equal


@pytest.fixture(scope="session")
def account_locked_body():
    with open("tests/equasis_responses/account_locked.html") as f:
        return f.read()


@pytest.fixture(scope="session")
def login_succeeded_body():
    with open("tests/equasis_responses/login_succeeded.html") as f:
        return f.read()


@pytest.fixture(scope="session")
def session_expired_body():
    with open("tests/equasis_responses/session_expired.html") as f:
        return f.read()


@pytest.fixture(scope="session")
def session_cancelled_body():
    with open("tests/equasis_responses/session_cancelled.html") as f:
        return f.read()


@pytest.fixture(scope="session")
def ship_details_body():
    with open("tests/equasis_responses/ship_details.html") as f:
        return f.read()


@pytest.fixture(scope="session")
def ship_inspection_body():
    with open("tests/equasis_responses/ship_inspection.html") as f:
        return f.read()


@pytest.fixture(scope="session")
def ship_inspection_other_date_format_body():
    with open("tests/equasis_responses/ship_inspection_other_date_format.html") as f:
        return f.read()


@pytest.fixture(scope="session")
def ship_inspection_expected_data():
    with open("tests/equasis_data/ship_inspection.csv") as f:
        results = pd.read_csv(f).drop(columns=["Details"])
        results["Date of report"] = pd.to_datetime(results["Date of report"])
        return results


@pytest.fixture(scope="session")
def ship_inspection_multiple_entries_per_row():
    with open("tests/equasis_responses/ship_inspection_multiple_entries_per_row.html") as f:
        return f.read()


@pytest.fixture(scope="session")
def ship_inspection_multiple_entries_per_row_expected_data():
    with open("tests/equasis_data/ship_inspection_multiple_entries_per_row.csv") as f:
        results = pd.read_csv(f).drop(columns=["Details"])
        results["Date of report"] = pd.to_datetime(results["Date of report"])
        return results


example_url = "https://www.equasis.org/example"


@responses.activate
def test_EquasisSession_make_request__account_locked(account_locked_body):

    responses.post(
        "https://www.equasis.org/EquasisWeb/authen/HomePage?fs=HomePage",
        status=200,
        body=account_locked_body,
    )

    session = EquasisSession("test", "password")

    with pytest.raises(EquasisSessionUnavailable) as exception:
        session.make_request(example_url, {})

    assert exception != False


@responses.activate
def test_EquasisSession_make_request__max_retries_timeouts(login_succeeded_body):

    responses.post(
        "https://www.equasis.org/EquasisWeb/authen/HomePage?fs=HomePage",
        status=200,
        body=login_succeeded_body,
    )
    for _ in range(EquasisSession.max_retries):
        responses.post(
            example_url,
            body=requests.exceptions.ReadTimeout(),
        )

    session = EquasisSession("test", "password")
    with pytest.raises(EquasisSessionUnavailable) as exception:
        session.make_request(example_url)

    assert exception != False


@responses.activate(registry=OrderedRegistry)
def test_EquasisSession_make_request__max_retries_server_error(login_succeeded_body):

    responses.post(
        "https://www.equasis.org/EquasisWeb/authen/HomePage?fs=HomePage",
        status=200,
        body=login_succeeded_body,
    )
    for _ in range(EquasisSession.max_retries):
        responses.post(
            example_url,
            status=500,
        )

    session = EquasisSession("test", "password")
    with pytest.raises(EquasisSessionUnavailable) as exception:
        session.make_request(example_url)

    assert exception != False


@responses.activate(registry=OrderedRegistry)
def test_EquasisSession_make_request__session_cancelled_then_succeed(
    login_succeeded_body,
    session_cancelled_body,
):
    expected_response = "expected_response"
    responses.post(
        "https://www.equasis.org/EquasisWeb/authen/HomePage?fs=HomePage",
        status=200,
        body=login_succeeded_body,
    )
    responses.post(example_url, status=200, body=session_cancelled_body)
    responses.post(
        "https://www.equasis.org/EquasisWeb/authen/HomePage?fs=HomePage",
        status=200,
        body=login_succeeded_body,
    )
    responses.post(example_url, status=200, body=expected_response)

    session = EquasisSession("test", "password")
    actual_response = session.make_request(example_url)

    assert actual_response == expected_response


@responses.activate(registry=OrderedRegistry)
def test_EquasisSession_make_request__session_expired_then_succeed(
    login_succeeded_body,
    session_expired_body,
):
    expected_response = "expected_response"
    responses.post(
        "https://www.equasis.org/EquasisWeb/authen/HomePage?fs=HomePage",
        status=200,
        body=login_succeeded_body,
    )
    responses.post(example_url, status=200, body=session_expired_body)
    responses.post(
        "https://www.equasis.org/EquasisWeb/authen/HomePage?fs=HomePage",
        status=200,
        body=login_succeeded_body,
    )
    responses.post(example_url, status=200, body=expected_response)

    session = EquasisSession("test", "password")
    actual_response = session.make_request(example_url)

    assert actual_response == expected_response


@responses.activate(registry=OrderedRegistry)
def test_EquasisSession_make_request__server_error_then_succeed(login_succeeded_body):
    expected_response = "expected_response"
    responses.post(
        "https://www.equasis.org/EquasisWeb/authen/HomePage?fs=HomePage",
        status=200,
        body=login_succeeded_body,
    )
    responses.post(
        example_url,
        status=500,
    )
    responses.add(responses.POST, example_url, status=200, body=expected_response)

    session = EquasisSession("test", "password")

    actual_response = session.make_request(example_url)
    assert actual_response == "expected_response"


@responses.activate(registry=OrderedRegistry)
def test_EquasisSession_make_request__immediately_succeeds(login_succeeded_body):
    expected_response = "expected_response"
    responses.post(
        "https://www.equasis.org/EquasisWeb/authen/HomePage?fs=HomePage",
        status=200,
        body=login_succeeded_body,
    )
    responses.add(responses.POST, example_url, status=200, body=expected_response)

    session = EquasisSession("test", "password")

    actual_response = session.make_request(example_url)
    assert actual_response == "expected_response"


class MockSession:
    def __init__(self, username, func):
        self.username = username
        self.func = func

    def make_request(self, url, data):
        return self.func(url, data)


def raise_session_unavailable_error(url, data):
    raise EquasisSessionUnavailable()


def create_mock_session_response(response):
    def mock_session_function_response(url, data):
        return response

    return mock_session_function_response


@responses.activate
def test_EquasisFixedInitialisationSessionPool_make_request__all_sessions_unavailable():

    responses.get("https://www.equasis.org/", status=200)

    sessions = [
        MockSession("test1", raise_session_unavailable_error),
        MockSession("test2", raise_session_unavailable_error),
    ]

    session_pool = EquasisFixedInitialisationSessionPool(sessions)

    with pytest.raises(Exception) as exception:
        session_pool.make_request(example_url, {})

    assert "No more sessions available." in str(exception)


@responses.activate
def test_EquasisFixedInitialisationSessionPool_make_request__sessions_return_value_use_first():

    responses.get("https://www.equasis.org/", status=200)

    sessions = [
        MockSession("test1", create_mock_session_response("test1 response")),
        MockSession("test2", create_mock_session_response("test2 response")),
    ]

    session_pool = EquasisFixedInitialisationSessionPool(sessions)

    response = session_pool.make_request(example_url, {})
    assert response == "test1 response"


@responses.activate
def test_EquasisFixedInitialisationSessionPool_make_request__sessions_only_2nd_one_works():

    responses.get("https://www.equasis.org/", status=200)

    sessions = [
        MockSession("test1", raise_session_unavailable_error),
        MockSession("test2", create_mock_session_response("test2 response")),
    ]

    session_pool = EquasisFixedInitialisationSessionPool(sessions)

    response = session_pool.make_request(example_url, {})
    assert response == "test2 response"


@responses.activate
def test_EquasisFixedInitialisationSessionPool_make_request__can_use_more_sessions_more_than_once():

    responses.get("https://www.equasis.org/", status=200)

    sessions = [
        MockSession("test1", raise_session_unavailable_error),
        MockSession("test2", create_mock_session_response("test2 response")),
    ]

    session_pool = EquasisFixedInitialisationSessionPool(sessions)

    response = session_pool.make_request(example_url, {})
    assert response == "test2 response"

    response = session_pool.make_request(example_url, {})
    assert response == "test2 response"


@responses.activate
def test_OnDemandEquasisSessionManager_make_request__good_response():

    session_pool = OnDemandEquasisSessionManager(
        account_generator=lambda: EquasisAccount("test1", "password"),
        session_factory=lambda account: MockSession(
            "test1", create_mock_session_response("test1 response")
        ),
    )

    response = session_pool.make_request(example_url, {})
    assert response == "test1 response"


@responses.activate
def test_OnDemandEquasisSessionManager_make_request__first_fails():

    sessions = [
        MockSession("test1", raise_session_unavailable_error),
        MockSession("test2", create_mock_session_response("test2 response")),
    ]

    def session_factory(account):
        return sessions.pop(0)

    session_pool = OnDemandEquasisSessionManager(
        account_generator=lambda: EquasisAccount("test1", "password"),
        session_factory=session_factory,
    )

    response = session_pool.make_request(example_url, {})
    assert response == "test2 response"


@responses.activate
def test_OnDemandEquasisSessionManager_make_request__three_failures_gives_up():

    sessions = [
        MockSession("test1", raise_session_unavailable_error),
        MockSession("test2", raise_session_unavailable_error),
        MockSession("test3", raise_session_unavailable_error),
        MockSession("test4", create_mock_session_response("test4 response")),
    ]

    def session_factory(account):
        return sessions.pop(0)

    session_pool = OnDemandEquasisSessionManager(
        account_generator=lambda: EquasisAccount("test1", "password"),
        session_factory=session_factory,
    )

    with pytest.raises(EquasisSessionPoolExhausted) as exception:
        response = session_pool.make_request(example_url, {})


def test_Equasis_get_ship_infos__has_ship_infos(ship_details_body):

    mocked_pool = MagicMock()

    mocked_pool.make_request.return_value = ship_details_body

    equasis = EquasisClient(session_manager=mocked_pool)

    actual = equasis.get_ship_infos("example_imo")

    assert actual["imo"] == "example_imo"
    assert len(actual["insurers"]) == 1
    assert actual["insurers"][0]["name"] == "Japan Ship Owners' P&I Association"
    assert actual["insurers"][0]["date_from"] == date(2024, 6, 7)

    assert actual["manager"]["name"] == "NORSTAR SHIP MANAGEMENT PTE"
    assert actual["manager"]["imo"] == "5441828"
    assert (
        actual["manager"]["address"]
        == "13-01, Singapore Post Centre, 10, Eunos Road 8, Singapore 408600"
    )
    assert actual["manager"]["date_from"].date() == date(2020, 7, 27)

    assert actual["owner"]["name"] == "YAOKI SHIPPING & MH PROGRESS"
    assert actual["owner"]["imo"] == "5994067"
    assert (
        actual["owner"]["address"]
        == "Care of Uni-Tankers A/S , Turbinevej 10, 5500 Middelfart, Denmark."
    )
    assert actual["owner"]["date_from"].date() == date(2017, 8, 9)

    assert actual["current_flag"] == "Panama"

    mocked_pool.make_request.assert_called_once_with(
        "https://www.equasis.org/EquasisWeb/restricted/ShipInfo?fs=Search", {"P_IMO": "example_imo"}
    )


def test_Equasis_get_inspections__has_inspection_details(
    ship_inspection_body, ship_inspection_expected_data
):

    mocked_pool = MagicMock()
    mocked_pool.make_request.return_value = ship_inspection_body
    equasis = EquasisClient(session_manager=mocked_pool)

    actual = equasis.get_inspections("example_imo")

    assert actual["imo"] == "example_imo"
    assert len(actual["inspections"]) == 18
    assert isinstance(actual["inspections"], pd.DataFrame)

    assert_frame_equal(actual["inspections"], ship_inspection_expected_data)

    mocked_pool.make_request.assert_called_once_with(
        "https://www.equasis.org/EquasisWeb/restricted/ShipInspection?fs=ShipInfo",
        {"P_IMO": "example_imo"},
    )


def test_Equasis_get_inspections__has_inspection_details_other_date_format(
    ship_inspection_other_date_format_body, ship_inspection_expected_data
):
    mocked_pool = MagicMock()
    mocked_pool.make_request.return_value = ship_inspection_other_date_format_body
    equasis = EquasisClient(session_manager=mocked_pool)

    actual = equasis.get_inspections("example_imo")

    assert actual["imo"] == "example_imo"
    assert len(actual["inspections"]) == 18
    assert isinstance(actual["inspections"], pd.DataFrame)

    assert_frame_equal(actual["inspections"], ship_inspection_expected_data)

    mocked_pool.make_request.assert_called_once_with(
        "https://www.equasis.org/EquasisWeb/restricted/ShipInspection?fs=ShipInfo",
        {"P_IMO": "example_imo"},
    )


def test_Equasis_get_inspections__has_inspection_details_multiple_entries_per_row(
    ship_inspection_multiple_entries_per_row, ship_inspection_multiple_entries_per_row_expected_data
):

    mocked_pool = MagicMock()
    mocked_pool.make_request.return_value = ship_inspection_multiple_entries_per_row
    equasis = EquasisClient(session_manager=mocked_pool)

    actual = equasis.get_inspections("example_imo")

    assert actual["imo"] == "example_imo"
    assert_frame_equal(
        actual["inspections"], ship_inspection_multiple_entries_per_row_expected_data
    )

    mocked_pool.make_request.assert_called_once_with(
        "https://www.equasis.org/EquasisWeb/restricted/ShipInspection?fs=ShipInfo",
        {"P_IMO": "example_imo"},
    )
