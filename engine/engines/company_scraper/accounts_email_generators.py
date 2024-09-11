from abc import ABC, abstractmethod

import requests

from decouple import config
from uuid import uuid4


class Alias:
    def __init__(self, *, alias: str, id: str):
        self.alias = alias
        self.id = id


class EmailManager(ABC):
    @abstractmethod
    def create_email(self) -> Alias:
        pass

    @abstractmethod
    def delete_email(self, alias: Alias):
        pass


class SimpleLoginEmailManager(EmailManager):
    @staticmethod
    def from_env():
        return SimpleLoginEmailManager(simple_login_api_key=config("SIMPLE_LOGIN_API_KEY"))

    BASE_URL = "https://app.simplelogin.io/api"

    def __init__(self, *, simple_login_api_key: str):
        self.simple_login_api_key = simple_login_api_key
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authentication": f"{simple_login_api_key}",
            }
        )

    def create_email(self) -> Alias:
        response = self.session.post(f"{SimpleLoginEmailManager.BASE_URL}/alias/random/new")

        response.raise_for_status()

        content = response.json()

        return Alias(
            alias=content["email"],
            id=content["id"],
        )

    def delete_email(
        self,
        alias: Alias,
    ):
        response = self.session.delete(f"{SimpleLoginEmailManager.BASE_URL}/aliases/{alias.id}")

        response.raise_for_status()

        content = response.json()
        if not content["deleted"]:
            raise RuntimeError(f"Failed to delete alias {alias}")

        return


class GmailEmailManager(EmailManager):
    def __init__(self, *, username_pattern: str):
        self._username_pattern = username_pattern

    def create_email(self) -> Alias:
        username = self._username_pattern % str(uuid4())
        return Alias(alias=username, id=username)

    def delete_email(self, alias: Alias):
        pass
