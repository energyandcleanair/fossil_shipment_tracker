from datetime import date
from unittest.mock import MagicMock
from engines.company_scraper.equasis import (
    Equasis,
    EquasisSession,
    EquasisSessionPool,
    EquasisSessionUnavailable,
)
import pytest
import responses
import requests

from responses.registries import OrderedRegistry


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


example_url = "https://www.equasis.org/example"


@responses.activate
def test_EquasisSession__make_request__account_locked(account_locked_body):

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
def test_EquasisSession__make_request__max_retries_timeouts(login_succeeded_body):

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
def test_EquasisSession__make_request__max_retries_server_error(login_succeeded_body):

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
def test_EquasisSession__make_request__session_cancelled_then_succeed(
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
def test_EquasisSession__make_request__session_expired_then_succeed(
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
def test_EquasisSession__make_request__server_error_then_succeed(login_succeeded_body):
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
def test_EquasisSession__make_request__immediately_succeeds(login_succeeded_body):
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


class MockSessionPool:
    def __init__(self, username, func):
        self.username = username
        self.func = func

    def make_request(self, url, data):
        return self.func(url, data)


def mock_session_function_error(url, data):
    raise EquasisSessionUnavailable()


def create_mock_session_response(response):
    def mock_session_function_response(url, data):
        return response

    return mock_session_function_response


@responses.activate
def test_EquasisSessionPool__make_request__all_sessions_unavailable():

    responses.get("https://www.equasis.org/", status=200)

    sessions = [
        MockSessionPool("test1", mock_session_function_error),
        MockSessionPool("test2", mock_session_function_error),
    ]

    session_pool = EquasisSessionPool(sessions)

    with pytest.raises(Exception) as exception:
        session_pool.make_request(example_url, {})

    assert "No more sessions available." in str(exception)


@responses.activate
def test_EquasisSessionPool__make_request__sessions_return_value_use_first():

    responses.get("https://www.equasis.org/", status=200)

    sessions = [
        MockSessionPool("test1", create_mock_session_response("test1 response")),
        MockSessionPool("test2", create_mock_session_response("test2 response")),
    ]

    session_pool = EquasisSessionPool(sessions)

    response = session_pool.make_request(example_url, {})
    assert response == "test1 response"


@responses.activate
def test_EquasisSessionPool__make_request__sessions_only_2nd_one_works():

    responses.get("https://www.equasis.org/", status=200)

    sessions = [
        MockSessionPool("test1", mock_session_function_error),
        MockSessionPool("test2", create_mock_session_response("test2 response")),
    ]

    session_pool = EquasisSessionPool(sessions)

    response = session_pool.make_request(example_url, {})
    assert response == "test2 response"


@responses.activate
def test_EquasisSessionPool__make_request__can_use_more_sessions_more_than_once():

    responses.get("https://www.equasis.org/", status=200)

    sessions = [
        MockSessionPool("test1", mock_session_function_error),
        MockSessionPool("test2", create_mock_session_response("test2 response")),
    ]

    session_pool = EquasisSessionPool(sessions)

    response = session_pool.make_request(example_url, {})
    assert response == "test2 response"

    response = session_pool.make_request(example_url, {})
    assert response == "test2 response"


def test_Equasis__get_ship_infos__has_ship_infos(ship_details_body):

    mocked_pool = MagicMock()

    mocked_pool.make_request.return_value = ship_details_body

    equasis = Equasis(session_pool=mocked_pool)

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
