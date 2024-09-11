from time import sleep

import requests

from datetime import datetime

from base.logger import logger


class AzCaptchaSolverError(Exception):
    pass


class SolverStartError(AzCaptchaSolverError):
    pass


class SolverTimeoutError(AzCaptchaSolverError):
    pass


class SolverUnexpectedError(AzCaptchaSolverError):
    pass


class SolutionError(AzCaptchaSolverError):
    pass


class InvalidSiteKeyError(SolutionError):
    pass


class UnsolvableError(SolutionError):
    pass


class AzCaptchaSolverClient:
    def __init__(
        self,
        api_key: str,
        initial_wait=20,
        backoff_interval=5,
        exponential_rate=1.5,
        timeout=5 * 60,
    ):
        self.api_key = api_key
        self.initial_wait = initial_wait
        self.backoff_interval = backoff_interval
        self.exponential_rate = exponential_rate
        self.timeout_seconds = timeout

    def solve_captcha(self, site_key: str, page_url: str):
        captcha_id = self._start_solver(site_key, page_url)

        return self._get_captcha_solution(captcha_id)

    def _start_solver(self, site_key, page_url):
        logger.info("Sending captcha to azcaptcha")
        response = requests.post(
            "https://azcaptcha.com/in.php",
            data={
                "method": "userrecaptcha",
                "googlekey": site_key,
                "key": self.api_key,
                "pageurl": page_url,
                "json": 1,
            },
        )

        if response.status_code != 200:
            raise SolverStartError("Failed to send captcha to azcaptcha")

        result = response.json()

        if result["status"] != 1:
            raise SolverStartError(f"Failed to send captcha to azcaptcha: {result['request']}")

        return result["request"]

    def _get_captcha_solution(self, captcha_id: str):
        # Stored for checking timeouts.
        start_time = datetime.now()
        # The service requests that we wait an initial period at the start.
        sleep(self.initial_wait)
        # Stored for exponential backoff.
        check_attempts = 0
        while True:
            response_data = self._check_captcha(captcha_id)

            # If the captcha has been solved, we exit
            if response_data["status"] == 1:
                return response_data["request"]

            seconds_elapsed = (datetime.now() - start_time).seconds
            # If we have been waiting too long, give up and raise an error.
            if seconds_elapsed > self.timeout_seconds:
                raise SolverTimeoutError(
                    f"Failed to solve captcha in {self.timeout_seconds} seconds and {check_attempts} checks"
                )

            response_error = response_data["request"]
            if response_error == "ERROR_INVALID_SITEKEY":
                raise InvalidSiteKeyError(f"Failed to solve captcha: {response_error}")

            if response_error == "ERROR_CAPTCHA_UNSOLVABLE":
                raise UnsolvableError(f"Failed to solve captcha: {response_error}")

            # If the solver returned any other error, except not ready, raise an error.
            if response_data["request"] != "CAPCHA_NOT_READY":
                raise SolverUnexpectedError(f"Failed to solve captcha: {response_error}")

            # Otherwise, wait and try again
            check_attempts += 1
            sleep(self.backoff_interval * (self.exponential_rate**check_attempts))

    def _check_captcha(self, captcha_id):
        logger.info("Getting captcha solution from azcaptcha")
        response = requests.get(
            "https://azcaptcha.com/res.php",
            params={
                "key": self.api_key,
                "action": "get",
                "id": captcha_id,
                "json": 1,
            },
        )

        if response.status_code != 200:
            raise ValueError("Failed to get captcha solution from azcaptcha")

        response_data = response.json()
        return response_data
