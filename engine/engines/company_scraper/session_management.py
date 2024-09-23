from abc import ABC, abstractmethod
from typing import Callable, Union

from .accounts import (
    EquasisAccount,
    default_multiple_accounts_from_env_generator,
    default_single_account_from_env_generator,
)
from .sessions import EquasisSession, EquasisSessionUnavailable

from base.logger import logger, logger_slack
from base.env import get_env

N_ACCOUNTS_TO_GENERATE = int(get_env("EQUASIS_N_ACCOUNTS_TO_GENERATE", "5"))


class EquasisSessionPoolExhausted(Exception):
    pass


class EquasisSessionManager(ABC):
    @abstractmethod
    def make_request(self, url: str, data: dict[str, str]):
        pass


class OnDemandEquasisSessionManager(EquasisSessionManager):
    def with_account_generator(
        generator: Callable[[], EquasisAccount] = default_single_account_from_env_generator,
    ):
        return OnDemandEquasisSessionManager(account_generator=generator)

    def __init__(
        self,
        *,
        account_generator: Callable[[], EquasisAccount],
        session_factory: Callable[
            [EquasisAccount], EquasisSession
        ] = lambda account: EquasisSession(account.username, account.password),
    ):
        self.account_generator = account_generator
        self.session: Union[EquasisSession, None] = None
        self.session_factory = session_factory

    def make_request(self, url, data):

        if self.session is None:
            self._move_to_next_session()

        max_new_account_tries = 3

        for _ in range(max_new_account_tries):
            try:
                return self.session.make_request(url, data)
            except EquasisSessionUnavailable as e:
                logger.info(
                    f"Equasis session {self.session.username} unavailable, moving to next.",
                    exc_info=True,
                    stack_info=True,
                )
                # This will throw an error if it can't create a new session.
                self._move_to_next_session()
            except e:
                logger.info("Equasis session had an error.", exc_info=True, stack_info=True)
                raise e

        raise EquasisSessionPoolExhausted(
            f"Failed to create new account after {max_new_account_tries} attempts."
        )

    def _move_to_next_session(self):
        next_account: EquasisAccount = self.account_generator()

        self.session = self.session_factory(next_account)


class EquasisFixedInitialisationSessionPool(EquasisSessionManager):
    @staticmethod
    def with_account_generator(
        n_accounts=N_ACCOUNTS_TO_GENERATE,
        generator: Callable[
            [int], list[EquasisAccount]
        ] = default_multiple_accounts_from_env_generator,
    ):
        accounts = generator(n_accounts)
        sessions = [EquasisSession(x.username, x.password) for x in accounts]
        return EquasisFixedInitialisationSessionPool(sessions)

    def __init__(self, sessions):
        self.sessions: list[EquasisSession] = sessions
        self.current_session_idx: int = -1

    def make_request(self, url, data):

        while len(self.sessions) > 0:
            if self.current_session_idx == 0:
                EquasisSession.check_connection()

            session = self._get_next_session()
            try:
                return session.make_request(url, data)
            except EquasisSessionUnavailable as e:
                logger.info(
                    f"Equasis session {session.username} unavailable, removing from pool.",
                    exc_info=True,
                    stack_info=True,
                )
                self.sessions.remove(session)
            except e:
                logger.info("Equasis session had an error.", exc_info=True, stack_info=True)

        raise EquasisSessionPoolExhausted("No more sessions available.")

    def _get_next_session(self) -> EquasisSession:
        self.current_session_idx += 1
        if self.current_session_idx >= len(self.sessions):
            self.current_session_idx = 0
        return self.sessions[self.current_session_idx]
