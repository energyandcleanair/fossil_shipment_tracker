from poplib import POP3, POP3_SSL
from time import sleep

from decouple import config
from fake_useragent import UserAgent
from uuid import uuid4

from datetime import datetime

from base.logger import logger


agent_generator = UserAgent()

RECEIVER_EMAIL_USERNAME = config("RECEIVER_EMAIL_USERNAME")
RECEIVER_EMAIL_PASSWORD = config("RECEIVER_EMAIL_PASSWORD")
RECEIVER_EMAIL_POP_SERVER = config("RECEIVER_EMAIL_POP_SERVER")
RECEIVER_EMAIL_POP_PORT = config("RECEIVER_EMAIL_POP_PORT")
RECEIVER_EMAIL_POP_SECURE = config("RECEIVER_EMAIL_POP_SECURE", default="true")

RECEIVER_EMAIL_POP_SECURE = RECEIVER_EMAIL_POP_SECURE.lower() == "true"


def create_pop_client_from_env():
    pop_client = (
        POP3_SSL(RECEIVER_EMAIL_POP_SERVER, RECEIVER_EMAIL_POP_PORT, timeout=5 * 60)
        if RECEIVER_EMAIL_POP_SECURE
        else POP3(RECEIVER_EMAIL_POP_SERVER, RECEIVER_EMAIL_POP_PORT, timeout=5 * 60)
    )

    pop_client.user(RECEIVER_EMAIL_USERNAME)
    pop_client.pass_(RECEIVER_EMAIL_PASSWORD)
    return pop_client


class EquasisEmailClient:
    @staticmethod
    def from_env():
        return EquasisEmailClient(client_generator=create_pop_client_from_env)

    def __init__(self, *, client_generator):
        self.generate_client = client_generator

    def get_verification_link_from_emails(self, username):
        logger.info(f"Waiting for email verification for {username}")
        start_time = datetime.now()
        max_wait = 2 * 60
        while True:
            sleep(5)

            verification_link = self.find_verification_link_in_mailbox(username)

            if verification_link != None:
                break

            if (datetime.now() - start_time).seconds > max_wait:
                raise TimeoutError("Email verification timeout")
        return verification_link

    def find_verification_link_in_mailbox(self, username: str):
        logger.info("Reading emails")

        client: POP3 | POP3_SSL = self.generate_client()

        all_emails: list[list[str]] = self.consume_all_emails(client)

        client.quit()

        for email_content in all_emails:
            if self.email_matches(username, email_content):
                return self.extract_verification_details(email_content)
        return None

    def consume_all_emails(self, client: POP3 | POP3_SSL):
        num_messages, _ = client.stat()
        all_emails = []
        for i in range(num_messages):
            email_index = i + 1
            all_emails.append(self.read_email_content(client, email_index))
            client.dele(email_index)
        return all_emails

    def read_email_content(self, client: POP3 | POP3_SSL, email_index: int):
        encoded_lines = client.retr(email_index)[1]
        lines = [line.decode("utf-8") for line in encoded_lines]
        return lines

    def delete_remaining_emails(self, client: POP3 | POP3_SSL):
        num_messages, _ = client.stat()
        for i in range(num_messages):
            client.dele(i + 1)

    def email_matches(self, username: str, email_content: list[str]):
        for line in email_content:
            if line.startswith("To: ") and username in line:
                return True
        return False

    def extract_verification_details(self, email_content: list[str]):
        link = self.extract_verification_link(email_content)
        password = self.extract_password(email_content)
        return (link, password)

    def extract_verification_link(self, email_content: list[str]):
        for line in email_content:
            if line.startswith("https://www.equasis.org/EquasisWeb/public/Activation?"):
                return line
        return None

    def extract_password(self, email_content: list[str]):
        for line in email_content:
            if line.startswith("Password : "):
                return line.split(" : ")[1].strip()
        return None
