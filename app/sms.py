import logging
from typing import Protocol


class SmsSender(Protocol):
    async def send_login_code(self, phone_number: str, code: str) -> None:
        ...


class MissingSmsSender:
    async def send_login_code(self, phone_number: str, code: str) -> None:
        raise RuntimeError("SMS provider is not configured")


class LoggingSmsSender:
    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)

    async def send_login_code(self, phone_number: str, code: str) -> None:
        self._logger.warning("SMS login code for %s: %s", phone_number, code)
