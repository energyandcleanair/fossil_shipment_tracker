import time
import uuid
import requests
from requests import RequestException
from base.logger import logger
from fake_useragent import UserAgent


SLEEP_PERIOD_AFTER_FAILURE = 5

agent_generator = UserAgent()


class EquasisSessionUnavailable(Exception):
    pass


class EquasisSessionLocked(EquasisSessionUnavailable):
    pass


class EquasisSessionTemporarilyUnavailable(Exception):
    pass


class EquasisSessionStatus:
    UNUSED = "UNUSED"
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"


class EquasisSession:

    max_retries = 3
    standard_headers = {"User-Agent": agent_generator.random}

    @staticmethod
    def check_connection():
        errors = []
        for _ in range(EquasisSession.max_retries):
            try:
                resp = requests.get(
                    "https://www.equasis.org/", headers=EquasisSession.standard_headers
                )
                if resp.status_code == 200:
                    return
            except RequestException as e:
                errors.append(e)

        raise Exception(f"Could not connect to Equasis:\n{errors}")

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.status = EquasisSessionStatus.UNUSED

    def make_request(self, url: str, data: dict[str, str] = {}):

        if self.status == EquasisSessionStatus.UNAVAILABLE:
            raise EquasisSessionUnavailable(
                f"The session for {self.username} is unavailable and can no longer be used."
            )

        if self.status == EquasisSessionStatus.UNUSED:
            self._login()

        try:
            response_body = self._handle_request(url, data)
            self.status = EquasisSessionStatus.AVAILABLE
            return response_body
        except EquasisSessionUnavailable as e:
            self.status = EquasisSessionStatus.UNAVAILABLE
            raise e

    def _login(self):
        url = "https://www.equasis.org/EquasisWeb/authen/HomePage?fs=HomePage"
        payload = {
            "j_email": self.username,
            "j_password": self.password,
            "submit": "Login",
        }

        errors = []
        for _ in range(EquasisSession.max_retries):
            try:
                resp = self.session.post(url, headers=EquasisSession.standard_headers, data=payload)
                body_text = resp.text
                if "Protected area, your access is denied" in body_text:
                    logger.info(f"The account {self.username} is locked.")
                    raise EquasisSessionLocked(f"The account {self.username} is locked.")
                elif resp.status_code == 200:
                    logger.info(f"Successfully logged in as {self.username}.")
                    return
                else:
                    logger.info(f"Could not log in as {self.username}: {resp.status_code}.")
                    errors.append(
                        {
                            "status_code": resp.status_code,
                            "content": body_text,
                        }
                    )
            except (RequestException, ConnectionError) as e:
                errors.append(e)
                time.sleep(SLEEP_PERIOD_AFTER_FAILURE)
                continue

        raise EquasisSessionUnavailable(f"Could get a login session for {self.username}:\n{errors}")

    def _handle_request(self, url, data):

        request_id = uuid.uuid4()

        for n_try in range(EquasisSession.max_retries):
            request_try_id = str(request_id) + "+try-" + str(n_try)
            try:
                logger.info(f"Request {request_try_id}: Requesting {url} as {self.username}.")
                resp = self.session.post(url, headers=EquasisSession.standard_headers, data=data)
                body_text = resp.text

                if "session has expired" in body_text or "session has been cancelled" in body_text:
                    logger.info(
                        f"Request {request_try_id}: {self.username} has expired, re-logging in."
                    )
                    self._login()
                elif resp.status_code == 200:
                    logger.info(f"Request {request_try_id}: success.")
                    return body_text
                else:
                    logger.info(f"Request {request_try_id}: Error {resp.status_code}.")

            except (RequestException, ConnectionError):
                logger.info(f"Request {request_try_id}: error.", exc_info=True)
                time.sleep(SLEEP_PERIOD_AFTER_FAILURE)
                continue

        raise EquasisSessionUnavailable(
            f"The account {self.username} is unavailable and can no longer be used."
        )
