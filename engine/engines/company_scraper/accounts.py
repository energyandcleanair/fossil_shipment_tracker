from typing import Union

from decouple import config
from fake_useragent import UserAgent


from base.logger import logger


from engines.company_scraper.accounts_details import PasswordGenerator
from engines.company_scraper.accounts_email_generators import EmailManager, GmailEmailManager
from engines.company_scraper.accounts_email_reader import EquasisEmailClient
from engines.company_scraper.accounts_website_driver import EquasisWebsiteAccountDriver

from .accounts_captcha import SolutionError

USERNAME = config("EQUASIS_USERNAME_PATTERN")


class EquasisAccount:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    def __str__(self):
        return f"EquasisAccount(username={self.username}, password={self.password})"

    def __repr__(self):
        return self.__str__()


class EquasisAccountCreator:
    def __init__(
        self,
        *,
        client: EquasisEmailClient,
        driver: EquasisWebsiteAccountDriver,
        email_alias_manager: EmailManager,
        password_generator: PasswordGenerator,
    ):
        self.email_client = client
        self.website_driver = driver
        self.email_alias_manager = email_alias_manager
        self.password_generator = password_generator

    def create_account(self):
        alias = self.email_alias_manager.create_email()
        username = alias.alias
        password = self.password_generator.generate_password()

        try:
            # We start by creating the account
            self.website_driver.create_account(username, password)
            link, _ = self.email_client.get_verification_link_from_emails(username)
            self.website_driver.verify_account(link)

            # Then to get the account to work, we have to reset the password
            self.website_driver.forget_password(username)
            link, new_password = self.email_client.get_verification_link_from_emails(username)
            self.website_driver.verify_account(link)

            if new_password is None:
                raise ValueError("Failed to get new password")
            return EquasisAccount(username, new_password)
        finally:
            self.email_alias_manager.delete_email(alias)


class EquasisAccountCreatorError(Exception):
    pass


class EquasisAccountCreatorErrorHandler:
    def __init__(self):
        self._error_history: list[Union[Exception, None]] = []

    def register_success(self):
        self._error_history.append(None)

    def handle_error(self, error: Exception):
        self._error_history.append(error)

        last_three_errors = self._error_history[-3:]

        count_of_solution_errors = len(
            [e for e in last_three_errors if isinstance(e, SolutionError)]
        )
        total_count_of_successes = len([e for e in self._error_history if e is None])

        if count_of_solution_errors >= 3 and total_count_of_successes == 0:
            raise EquasisAccountCreatorError(
                "Failed to create account due to too many solution errors without success."
            )


def default_multiple_accounts_from_env_generator(n_accounts: int) -> list[EquasisAccount]:

    equasis_driver = EquasisWebsiteAccountDriver.create_from_env()
    email_client = EquasisEmailClient()
    email_alias_manager = GmailEmailManager(username_pattern=USERNAME)
    password_generator = PasswordGenerator(12)

    account_creator = EquasisAccountCreator(
        client=email_client,
        driver=equasis_driver,
        email_alias_manager=email_alias_manager,
        password_generator=password_generator,
    )

    accounts = []

    max_tries = 3

    # We keep track of the last 3 errors to detect if we are failing to create accounts due to
    # an error that can't be worked around.
    error_handler = EquasisAccountCreatorErrorHandler()

    for _ in range(n_accounts):
        for attempt in range(max_tries):
            try:
                account = account_creator.create_account()
                accounts.append(account)
                error_handler.register_success()
                break
            except Exception as e:
                logger.info(
                    f"Failed to create account: attempt {attempt+1}/{max_tries}",
                    stack_info=True,
                    exc_info=True,
                )
                error_handler.handle_error(e)
                pass

    equasis_driver.close()

    return accounts


def default_single_account_from_env_generator() -> EquasisAccount:

    equasis_driver = EquasisWebsiteAccountDriver.create_from_env()
    email_client = EquasisEmailClient.from_env()
    email_alias_manager = GmailEmailManager(username_pattern=USERNAME)
    password_generator = PasswordGenerator(12)

    account_creator = EquasisAccountCreator(
        client=email_client,
        driver=equasis_driver,
        email_alias_manager=email_alias_manager,
        password_generator=password_generator,
    )

    account = None

    max_tries = 3

    # We keep track of the last 3 errors to detect if we are failing to create accounts due to
    # an error that can't be worked around.
    error_handler = EquasisAccountCreatorErrorHandler()

    for attempt in range(max_tries):
        try:
            account = account_creator.create_account()
            error_handler.register_success()
            break
        except Exception as e:
            logger.info(
                f"Failed to create account: attempt {attempt+1}/{max_tries}",
                stack_info=True,
                exc_info=True,
            )
            error_handler.handle_error(e)
            pass

    equasis_driver.close()

    return account
