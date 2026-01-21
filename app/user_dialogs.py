import asyncio
import logging
from typing import Awaitable, Callable, Dict, Optional

from telethon import events

from .storage import Storage
from .user_sender import UserSender


PaymentCallback = Callable[[int, str], Awaitable[None]]


class UserDialogResponder:
    """Auto-responder for a connected user account to collect payment data."""

    def __init__(
        self,
        user_sender: UserSender,
        storage: Storage,
        *,
        welcome_message: str,
        card_prompt_message: str,
        card_name_prompt: str,
        thank_you_message: str,
        invalid_card_message: str,
        invalid_name_message: str,
        cancel_message: str,
        payment_created_callback: Optional[PaymentCallback] = None,
    ) -> None:
        self._user_sender = user_sender
        self._storage = storage
        self._welcome_message = welcome_message
        self._card_prompt = card_prompt_message
        self._card_name_prompt = card_name_prompt
        self._thank_you_message = thank_you_message
        self._invalid_card_message = invalid_card_message
        self._invalid_name_message = invalid_name_message
        self._cancel_message = cancel_message
        self._payment_created_callback = payment_created_callback
        self._logger = logging.getLogger(__name__)
        self._state_lock = asyncio.Lock()
        self._states: Dict[int, Dict[str, str]] = {}
        self._event_filter = events.NewMessage(incoming=True)
        self._handler_registered = False

    async def start(self) -> None:
        if self._handler_registered:
            return
        await self._user_sender.start()
        client = self._user_sender.client
        client.add_event_handler(self._handle_new_message, self._event_filter)
        self._handler_registered = True
        self._logger.info("UserDialogResponder started, personal account auto-replies enabled.")

    async def stop(self) -> None:
        if not self._handler_registered:
            return
        self._user_sender.client.remove_event_handler(self._handle_new_message, self._event_filter)
        self._handler_registered = False
        async with self._state_lock:
            self._states.clear()

    async def _handle_new_message(self, event: events.NewMessage.Event) -> None:
        if event.out or not event.is_private:
            return
        sender_id = event.sender_id
        if not sender_id:
            return
        text_raw = (event.raw_text or "").strip()
        lower_text = text_raw.lower()
        if lower_text == "/cancel":
            async with self._state_lock:
                self._states.pop(sender_id, None)
            await event.respond(self._cancel_message)
            return

        action: str = ""
        card_number_for_completion: Optional[str] = None
        async with self._state_lock:
            state = self._states.get(sender_id)
            if not state:
                self._states[sender_id] = {"step": "card_number"}
                action = "welcome"
            else:
                step = state.get("step")
                if step == "card_number":
                    digits = "".join(ch for ch in text_raw if ch.isdigit())
                    if not digits or len(digits) < 12 or len(digits) > 19:
                        action = "invalid_card"
                    else:
                        state["card_number"] = digits
                        state["step"] = "card_name"
                        action = "ask_name"
                elif step == "card_name":
                    card_number = state.get("card_number")
                    if not card_number:
                        self._states.pop(sender_id, None)
                        self._states[sender_id] = {"step": "card_number"}
                        action = "welcome"
                    elif len(text_raw) < 3:
                        action = "invalid_name"
                    else:
                        card_number_for_completion = card_number
                        self._states.pop(sender_id, None)
                        action = "complete"
                else:
                    self._states.pop(sender_id, None)
                    self._states[sender_id] = {"step": "card_number"}
                    action = "welcome"

        if action == "welcome":
            await event.respond(self._welcome_message)
            return
        if action == "invalid_card":
            await event.respond(self._invalid_card_message)
            return
        if action == "ask_name":
            await event.respond(self._card_name_prompt)
            return
        if action == "invalid_name":
            await event.respond(self._invalid_name_message)
            return
        if action == "complete" and card_number_for_completion:
            await self._finalize_payment(event, sender_id, card_number_for_completion, text_raw)
            return
        if not action:
            # Unknown branch, remind card prompt.
            await event.respond(self._card_prompt)

    async def _finalize_payment(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        card_number: str,
        card_name: str,
    ) -> None:
        sender = await event.get_sender()
        username = getattr(sender, "username", None)
        full_name = " ".join(filter(None, [getattr(sender, "first_name", None), getattr(sender, "last_name", None)]))
        if not full_name:
            full_name = username or str(sender_id)
        try:
            request_id = await self._storage.create_payment_request(
                user_id=sender_id,
                username=username,
                full_name=full_name,
                card_number=card_number,
                card_name=card_name.strip(),
            )
        except Exception:
            self._logger.exception("Failed to create payment request from user session.")
            await event.respond("Не удалось сохранить данные. Попробуйте ещё раз чуть позже.")
            return
        await event.respond(self._thank_you_message)
        if self._payment_created_callback:
            try:
                await self._payment_created_callback(sender_id, request_id)
            except Exception:
                self._logger.exception("Failed to notify admins about payment request from user session.")
