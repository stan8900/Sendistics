import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, time as datetime_time
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Set
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.handler import CancelHandler
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.utils import exceptions, executor
from aiogram.utils.markdown import hbold, quote_html
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from dotenv import load_dotenv

from app.account_manager import AccountManager, get_account_proxy
from app.audience_parser import AudienceParser
from app.auto_sender import AutoSender
from app.invite_engine import InviteEngine
from app.keyboards import (
    GROUPS_PAGE_SIZE,
    accounts_keyboard,
    auto_menu_keyboard,
    groups_keyboard,
    main_menu_keyboard,
    my_account_keyboard,
)
from app.pdf_reports import build_payments_pdf
from app.runtime_config import create_storage_from_env
from app.states import (
    AccountStates,
    AdminLoginStates,
    AdminManualPaymentStates,
    AutoCampaignStates,
    GroupParserStates,
    InviteStates,
    ParserStates,
    PaymentStates,
    SharedProxyStates,
)
from app.user_sender import UserSender, build_telethon_proxy
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
    PasswordHashInvalidError,
)
from telethon.sessions import StringSession


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPPORTED_PROXY_SCHEMES = {"socks5", "socks4", "http"}
PROXY_DISABLE_WORDS = {"off", "0", "none", "нет", "disable", "remove", "stop", "no"}
SUPPORT_AGENT_USERNAME = "@rasylon_support"
BOT_SLEEP_UNTIL_RAW = os.getenv("BOT_SLEEP_UNTIL") or os.getenv("SLEEP_UNTIL")
BOT_SLEEP_FROM_RAW = os.getenv("BOT_SLEEP_FROM", "00:00")
BOT_SLEEP_TO_RAW = os.getenv("BOT_SLEEP_TO", "09:00")
BOT_SLEEP_TIMEZONE_RAW = os.getenv("BOT_SLEEP_TIMEZONE", "Asia/Tashkent")
BOT_SLEEP_MESSAGE_TEMPLATE = os.getenv(
    "BOT_SLEEP_MESSAGE",
    "Бот находится в режиме спячки до {until}. Напишите позже.",
)


def get_sleep_timezone() -> ZoneInfo:
    try:
        return ZoneInfo(BOT_SLEEP_TIMEZONE_RAW)
    except ZoneInfoNotFoundError:
        logger.warning("Неизвестный BOT_SLEEP_TIMEZONE=%s. Используем Asia/Tashkent.", BOT_SLEEP_TIMEZONE_RAW)
        return ZoneInfo("Asia/Tashkent")


def parse_sleep_until(raw: Optional[str], now: Optional[datetime] = None) -> Optional[datetime]:
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None
    timezone = get_sleep_timezone()
    now = now or datetime.now(timezone)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone)
    else:
        now = now.astimezone(timezone)
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%d.%m.%Y %H:%M"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone)
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone)
        return parsed.astimezone(timezone)
    except ValueError:
        pass
    try:
        parsed_time = datetime.strptime(text, "%H:%M").time()
    except ValueError:
        logger.warning("BOT_SLEEP_UNTIL должен быть в формате HH:MM, YYYY-MM-DD HH:MM или DD.MM.YYYY HH:MM.")
        return None
    return datetime.combine(now.date(), parsed_time, tzinfo=timezone)


def parse_sleep_time(raw: Optional[str]) -> Optional[datetime_time]:
    if not raw:
        return None
    try:
        return datetime.strptime(raw.strip(), "%H:%M").time()
    except ValueError:
        logger.warning("Время режима спячки должно быть в формате HH:MM.")
        return None


def get_active_sleep_until(now: Optional[datetime] = None) -> Optional[datetime]:
    timezone = get_sleep_timezone()
    now = now or datetime.now(timezone)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone)
    else:
        now = now.astimezone(timezone)
    sleep_until = parse_sleep_until(BOT_SLEEP_UNTIL_RAW, now=now)
    if sleep_until and sleep_until > now:
        return sleep_until

    sleep_from = parse_sleep_time(BOT_SLEEP_FROM_RAW)
    sleep_to = parse_sleep_time(BOT_SLEEP_TO_RAW)
    if not sleep_from or not sleep_to or sleep_from == sleep_to:
        return None
    now_time = now.time()
    if sleep_from < sleep_to:
        if sleep_from <= now_time < sleep_to:
            return datetime.combine(now.date(), sleep_to, tzinfo=timezone)
        return None
    if now_time >= sleep_from:
        return datetime.combine(now.date() + timedelta(days=1), sleep_to, tzinfo=timezone)
    if now_time < sleep_to:
        return datetime.combine(now.date(), sleep_to, tzinfo=timezone)
    return None


def build_sleep_message(sleep_until: datetime) -> str:
    until_text = sleep_until.astimezone(get_sleep_timezone()).strftime("%d.%m.%Y %H:%M")
    try:
        return BOT_SLEEP_MESSAGE_TEMPLATE.format(until=until_text)
    except (KeyError, ValueError):
        logger.warning("BOT_SLEEP_MESSAGE содержит некорректный шаблон. Используем стандартный текст.")
        return f"Бот находится в режиме спячки до {until_text}. Напишите позже."


async def answer_sleep_message_if_needed(message: types.Message) -> bool:
    if message.chat.type != types.ChatType.PRIVATE:
        return False
    if message.from_user and await is_admin_user(message.from_user.id):
        return False
    sleep_until = get_active_sleep_until()
    if not sleep_until:
        return False
    await message.answer(build_sleep_message(sleep_until))
    return True


class SleepModeMiddleware(BaseMiddleware):
    async def on_pre_process_message(self, message: types.Message, data: Dict[str, Any]) -> None:
        if await answer_sleep_message_if_needed(message):
            raise CancelHandler()


def format_proxy_display(proxy: Dict[str, Any]) -> str:
    proxy_type = (proxy.get("type") or "socks5").lower()
    host = proxy.get("host") or "—"
    port = proxy.get("port") or "—"
    username = proxy.get("username") or ""
    password = proxy.get("password") or ""
    creds = ""
    if username:
        creds = username
        if password:
            creds += ":***"
        creds += "@"
    return f"{proxy_type}://{creds}{host}:{port}"


def build_shared_proxy_error_text(bot_obj: Bot) -> str:
    proxy = bot_obj.get("shared_proxy")
    source = bot_obj.get("shared_proxy_source")
    status = format_proxy_display(proxy) if proxy else "не задан"
    if source == "env":
        action = "замените или удалите TG_USER_PROXY в переменных окружения Pella и перезапустите бота"
    else:
        action = "откройте «🌐 Общий прокси», замените прокси или отправьте off"
    return (
        "Не удалось подключиться к Telegram через общий прокси.\n"
        f"Текущий прокси: {status}.\n"
        f"Что сделать: {action}.\n\n"
        "Пока прокси не отвечает, общий TG_USER_SESSION и добавление новых номеров через MTProto работать не будут."
    )


def parse_proxy_string(raw: str) -> Dict[str, Any]:
    if not raw:
        raise ValueError("Укажите адрес прокси.")
    text = raw.strip()
    scheme = "socks5"
    username: Optional[str] = None
    password: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    if "://" in text:
        parsed = urlparse(text)
        scheme = (parsed.scheme or "socks5").lower()
        if scheme not in SUPPORTED_PROXY_SCHEMES:
            raise ValueError("Поддерживаются только схемы socks5, socks4 и http.")
        host = parsed.hostname
        try:
            port = parsed.port
        except ValueError:
            raise ValueError("Порт должен быть числом.")
        username = parsed.username or None
        password = parsed.password or None
    else:
        parts = [part.strip() for part in text.split(":")]
        if len(parts) < 2:
            raise ValueError("Укажите хост и порт через двоеточие.")
        host = parts[0]
        try:
            port = int(parts[1])
        except ValueError:
            raise ValueError("Порт должен быть числом.")
        if len(parts) >= 3 and parts[2]:
            username = parts[2]
        if len(parts) >= 4 and parts[3]:
            password = parts[3]
    if not host or port is None:
        raise ValueError("Некорректный адрес прокси.")
    return {
        "type": scheme,
        "host": host,
        "port": port,
        "username": username,
        "password": password,
    }


async def sync_shared_proxy_from_storage(bot_obj: Bot) -> None:
    if bot_obj.get("shared_proxy_source") == "env":
        return
    stored_proxy = await storage.get_shared_proxy()
    bot_obj["shared_proxy"] = stored_proxy
    bot_obj["shared_proxy_source"] = "db" if stored_proxy else None


async def instantiate_user_sender(bot_obj: Bot) -> Optional[UserSender]:
    api_id = bot_obj.get("personal_api_id")
    api_hash = bot_obj.get("personal_api_hash")
    session = bot_obj.get("personal_session")
    if not api_id or not api_hash or not session:
        return None
    proxy = bot_obj.get("shared_proxy")
    sender = UserSender(api_id, api_hash, session, proxy=proxy)
    await sender.start()
    return sender


async def replace_bot_user_sender(bot_obj: Bot) -> Optional[UserSender]:
    existing: Optional[UserSender] = bot_obj.get("user_sender")
    if existing:
        try:
            await existing.stop()
        except Exception:
            logger.exception("Не удалось корректно остановить старую пользовательскую сессию.")
    try:
        sender = await instantiate_user_sender(bot_obj)
    except Exception:
        logger.exception(
            "Не удалось подключить пользовательскую сессию Telegram. Сообщения будут отправляться от имени бота."
        )
        sender = None
    bot_obj["user_sender"] = sender
    audience_parser: Optional[AudienceParser] = bot_obj.get("audience_parser")
    if audience_parser:
        audience_parser.set_user_sender(sender)
    auto_sender: Optional[AutoSender] = bot_obj.get("auto_sender")
    if auto_sender:
        await auto_sender.replace_user_sender(sender)
    return sender

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Missng BOT_TOKEN")

storage = create_storage_from_env()

tg_user_api_id_raw = os.getenv("TG_USER_API_ID")
tg_user_api_hash = os.getenv("TG_USER_API_HASH")
tg_user_session = os.getenv("TG_USER_SESSION")
tg_user_proxy_raw = os.getenv("TG_USER_PROXY")
tg_user_api_id: Optional[int]
if tg_user_api_id_raw:
    try:
        tg_user_api_id = int(tg_user_api_id_raw)
    except ValueError:
        logger.warning("TG_USER_API_ID должен быть числом. Пользовательская рассылка отключена.")
        tg_user_api_id = None
else:
    tg_user_api_id = None
env_shared_proxy: Optional[Dict[str, Any]] = None
if tg_user_proxy_raw:
    try:
        env_shared_proxy = parse_proxy_string(tg_user_proxy_raw)
        logger.info("Используем прокси для TG_USER_SESSION: %s", format_proxy_display(env_shared_proxy))
    except ValueError as exc:
        logger.warning("Не удалось разобрать TG_USER_PROXY. Параметр проигнорирован: %s", exc)

bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(SleepModeMiddleware())

bot["storage"] = storage
bot["auto_sender"] = None  # filled on startup
bot["user_sender"] = None
bot["personal_api_id"] = tg_user_api_id
bot["personal_api_hash"] = tg_user_api_hash
bot["personal_api_available"] = bool(tg_user_api_id and tg_user_api_hash)
bot["personal_session"] = tg_user_session
bot["shared_proxy"] = env_shared_proxy
bot["shared_proxy_source"] = "env" if env_shared_proxy else None
if bot["personal_api_available"] and tg_user_api_id and tg_user_api_hash:
    bot["account_manager"] = AccountManager(tg_user_api_id, tg_user_api_hash)
else:
    bot["account_manager"] = None
bot["audience_parser"] = AudienceParser(
    storage,
    base_dir=BASE_DIR,
    user_sender=None,
    account_manager=bot["account_manager"],
)
if bot["account_manager"]:
    bot["invite_engine"] = InviteEngine(storage, bot["account_manager"])
else:
    bot["invite_engine"] = None

PAYMENT_AMOUNT = int(os.getenv("PAYMENT_AMOUNT", "100000"))
PAYMENT_CURRENCY = os.getenv("PAYMENT_CURRENCY", "UZS")
PAYMENT_DESCRIPTION = os.getenv("PAYMENT_DESCRIPTION", "Оплата услуг логистического бота")
PAYMENT_VALID_DAYS = int(os.getenv("PAYMENT_VALID_DAYS", "30"))
PAYMENT_CARD_TARGET = os.getenv("PAYMENT_CARD_TARGET", "9860 1701 1433 3116")
PAYMENT_CARD_PROMPT = "Введите номер карты (12–19 цифр).\nДля отмены используйте /cancel."
PAYMENT_CARD_NAME_PROMPT = "Укажите имя, как на карте.\nДля отмены используйте /cancel."
PAYMENT_CARD_INVALID_MESSAGE = (
    "Номер карты должен содержать только 12–19 цифр. Пожалуйста, отправьте номер ещё раз.\n\n"
    f"{PAYMENT_CARD_PROMPT}"
)
PAYMENT_CARD_NAME_INVALID_MESSAGE = "Имя должно содержать минимум 3 символа. Попробуйте снова."
PAYMENT_THANK_YOU_MESSAGE = (
    "Спасибо! Данные отправлены администратору. \n"
    f"После подтверждения оплата будет действовать {PAYMENT_VALID_DAYS} дней."
)

WELCOME_TEXT_ADMIN = (
    "👋 Добро пожаловать обратно!\n\n"
    "⚒ Авторассылка — настройка сообщений и расписания\n"
    "👤 Мой аккаунт — оплаты и номера для авторассылок\n"
    "💰 Пополнить баланс — контроль оплат пользователей\n"
    "📊 Статистика — просмотр результатов рассылки\n"
    "📋 Выбрать группы — управление чатами\n"
    "⚙️ Настройки — текущие параметры\n"
    "📜 Оплаты — список активных и ожидающих платежей"
)

WELCOME_TEXT_USER = (
    "👋 Добро пожаловать!\n\n"
    "👤 Мой аккаунт — оплаты и номера для авторассылок.\n"
    f"💰 Пополнить баланс — отправьте данные оплаты на карту {PAYMENT_CARD_TARGET}.\n"
    "📜 История оплат — проверьте статус заявок и срок подписки.\n\n"
    "Если вы оператор, используйте команду /admin и введите код доступа."
)

GROUPS_BASE_TEXT = (
    "📋 <b>Выбор групп для рассылки</b>\n"
    "Нажмите на кнопки, чтобы добавить или убрать чат."
)

STATIC_ADMIN_IDS: Set[int] = {
    int(admin_id.strip())
    for admin_id in os.getenv("ADMIN_IDS", "").split(",")
    if admin_id.strip().isdigit()
}

ADMIN_INVITE_CODE = os.getenv("ADMIN_CODE", "TW13")


@dataclass
class PendingAccountLogin:
    client: TelegramClient
    phone: str
    phone_code_hash: str
    awaiting_password: bool = False


pending_account_lock = asyncio.Lock()
pending_account_logins: Dict[int, PendingAccountLogin] = {}


async def refresh_account_chats(bot_obj: Bot, owner_id: int, account_id: int) -> bool:
    manager: Optional[AccountManager] = bot_obj.get("account_manager")
    if not manager:
        return False
    account = await storage.get_user_account(account_id, owner_id=owner_id)
    if not account:
        return False
    session = account.get("session")
    if not session:
        return False
    try:
        sender = await manager.get_sender(account)
        dialogs = await sender.list_accessible_chats()
    except Exception:
        logger.exception("Не удалось обновить список чатов аккаунта %s", account_id)
        return False
    await storage.replace_account_chats(account_id, dialogs)
    return True


async def should_require_targets(bot_obj: Bot, user_id: int) -> bool:
    if bot_obj.get("user_sender"):
        return False
    auto = await storage.get_auto(user_id)
    return not auto.get("sender_account_id")


async def get_active_sender_account_id(user_id: int) -> Optional[int]:
    auto = await storage.get_auto(user_id)
    account_id = auto.get("sender_account_id")
    if account_id is None:
        return None
    account = await storage.get_user_account(int(account_id), owner_id=user_id)
    return int(account_id) if account else None


async def replace_pending_account(user_id: int, pending: Optional[PendingAccountLogin]) -> None:
    async with pending_account_lock:
        existing = pending_account_logins.pop(user_id, None)
        if pending:
            pending_account_logins[user_id] = pending
    if existing:
        try:
            await existing.client.disconnect()
        except Exception:
            logger.exception("Не удалось закрыть предыдущую сессию подтверждения номера пользователя %s.", user_id)


async def get_pending_account(user_id: int) -> Optional[PendingAccountLogin]:
    async with pending_account_lock:
        return pending_account_logins.get(user_id)


async def get_user_role(user_id: int) -> str:
    if user_id in STATIC_ADMIN_IDS:
        return "admin"
    role = await storage.get_user_role(user_id)
    return role or "user"


async def collect_admin_ids() -> Set[int]:
    admins = set(STATIC_ADMIN_IDS)
    dynamic = await storage.list_admin_user_ids()
    admins.update(dynamic)
    return admins


async def is_admin_user(user_id: int) -> bool:
    if user_id in STATIC_ADMIN_IDS:
        return True
    role = await storage.get_user_role(user_id)
    return role == "admin"


def format_currency(amount: int, currency: str) -> str:
    formatted = f"{amount:,}".replace(",", " ")
    return f"{formatted} {currency}"


def format_datetime(value: Optional[str]) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromisoformat(value).strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return value


def mask_phone(phone: Optional[str]) -> str:
    if not phone:
        return "—"
    raw = "".join(ch for ch in phone.strip() if ch.isdigit() or ch == "+")
    if len(raw) <= 6:
        return raw
    prefix = raw[:4]
    suffix = raw[-2:]
    return f"{prefix}…{suffix}"


def format_account_display(account: Dict[str, Any]) -> str:
    title = (account.get("title") or "").strip()
    phone = mask_phone(account.get("phone"))
    if title:
        if phone and phone != "—":
            return f"{title} ({phone})"
        return title
    return phone or "Аккаунт"

def normalize_phone(raw: str) -> Optional[str]:
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return None
    text = raw.strip()
    if text.startswith("+"):
        return f"+{digits}"
    if digits.startswith("00"):
        return f"+{digits[2:]}"
    return f"+{digits}"


def payment_admin_keyboard(request_id: str) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Оплатил", callback_data=f"payment:approve:{request_id}"),
        InlineKeyboardButton("❌ Не оплатил", callback_data=f"payment:decline:{request_id}"),
    )
    return keyboard


def build_payment_admin_text(payment: Dict[str, Any]) -> str:
    user_display = payment.get("full_name") or "Неизвестный пользователь"
    username = payment.get("username")
    if username:
        user_display = f"{user_display} (@{username})"
    card_number = payment.get("card_number") or "—"
    card_name = payment.get("card_name") or "—"
    status = payment.get("status", "pending")
    status_map = {
        "pending": "В ожидании",
        "approved": "Оплачен ✅",
        "declined": "Не оплачен ❌",
    }
    status_text = status_map.get(status, status)
    created_at = payment.get("created_at")
    resolved_at = payment.get("resolved_at")
    resolved_by = payment.get("resolved_by") or {}
    lines = [
        "💳 <b>Заявка на оплату</b>",
        f"ID заявки: <code>{payment.get('request_id')}</code>",
        f"Пользователь: {quote_html(user_display)}",
        f"ID пользователя: <code>{payment.get('user_id')}</code>",
        f"Сумма: {format_currency(PAYMENT_AMOUNT, PAYMENT_CURRENCY)}",
        f"Номер карты: <code>{card_number}</code>",
        f"Имя на карте: {quote_html(card_name)}",
        f"Статус: {status_text}",
    ]
    if created_at:
        lines.append(f"Создано: {quote_html(created_at)}")
    if resolved_at:
        lines.append(f"Обновлено: {quote_html(resolved_at)}")
        if status == "approved":
            try:
                resolved_dt = datetime.fromisoformat(resolved_at)
                expires_dt = resolved_dt + timedelta(days=PAYMENT_VALID_DAYS)
                lines.append(f"Оплачено до: {expires_dt.strftime('%d.%m.%Y')}")
            except ValueError:
                pass
    if resolved_by:
        admin_info = resolved_by.get("admin_username")
        if admin_info:
            lines.append(f"Обработал: @{admin_info}")
        else:
            lines.append(f"Обработал ID: <code>{resolved_by.get('admin_id')}</code>")
    return "\n".join(lines)


async def send_payment_status_to_user(user_id: int, text: str) -> None:
    try:
        await bot.send_message(user_id, text)
    except exceptions.TelegramAPIError as exc:
        logger.warning("Не удалось отправить уведомление пользователю %s через бота: %s", user_id, exc)


async def notify_admins_about_payment(requester_id: int, request_id: str) -> None:
    payment = await storage.get_payment(request_id)
    if not payment:
        return
    admin_text = build_payment_admin_text(payment)
    admin_ids = await collect_admin_ids()
    requester_is_admin = await is_admin_user(requester_id)
    for admin_id in admin_ids:
        if admin_id == requester_id and not requester_is_admin:
            continue
        if not await is_admin_user(admin_id):
            continue
        try:
            await bot.send_message(
                admin_id,
                admin_text,
                reply_markup=payment_admin_keyboard(request_id),
            )
        except exceptions.TelegramAPIError as exc:
            logger.error("Не удалось уведомить админа %s: %s", admin_id, exc)


def build_user_payment_status_message(status: str, resolved_at: Optional[str]) -> str:
    if status == "approved":
        expires_text = ""
        if resolved_at:
            try:
                resolved_dt = datetime.fromisoformat(resolved_at)
                expires_dt = resolved_dt + timedelta(days=PAYMENT_VALID_DAYS)
                expires_text = f" Оплата активна до {expires_dt.strftime('%d.%m.%Y')} включительно."
            except ValueError:
                expires_text = ""
        return "✅ Администратор подтвердил оплату. Спасибо!" + expires_text
    if status == "declined":
        return "❌ Администратор отклонил оплату. Свяжитесь с поддержкой."
    return "Статус оплаты обновлён."


async def build_user_payment_history_text(user_id: int) -> str:
    payments = await storage.get_user_payments(user_id)
    lines = ["📜 <b>История оплат</b>"]
    if not payments:
        lines.append("У вас ещё нет заявок на оплату.")
        return "\n".join(lines)

    status_map = {
        "approved": "✅ Оплачено",
        "pending": "⏳ Ожидает подтверждения",
        "declined": "❌ Отклонено",
    }
    for payment in payments[:20]:
        status = payment.get("status")
        symbol = {"approved": "✅", "pending": "⏳", "declined": "❌"}.get(status, "•")
        created = format_datetime(payment.get("created_at"))
        lines.append(f"{symbol} {created} — {status_map.get(status, status)}")
        if status == "approved":
            resolved_at = payment.get("resolved_at")
            if resolved_at:
                try:
                    expires_dt = datetime.fromisoformat(resolved_at) + timedelta(days=PAYMENT_VALID_DAYS)
                    lines.append(f"     Активна до: {expires_dt.strftime('%d.%m.%Y')}")
                except ValueError:
                    pass
        card_number = payment.get("card_number")
        if card_number:
            lines.append(f"     Карта: {card_number}")
    return "\n".join(lines)


async def build_my_account_text(user_id: int) -> str:
    payments = await storage.get_user_payments(user_id)
    accounts = await storage.list_user_accounts(user_id)
    auto = await storage.get_auto(user_id)
    active_account_id = auto.get("sender_account_id")

    approved_payment = next((payment for payment in payments if payment.get("status") == "approved"), None)
    if approved_payment and approved_payment.get("resolved_at"):
        try:
            expires_dt = datetime.fromisoformat(approved_payment["resolved_at"]) + timedelta(days=PAYMENT_VALID_DAYS)
            payment_status = f"активна до {expires_dt.strftime('%d.%m.%Y')} ✅"
        except ValueError:
            payment_status = "подтверждена ✅"
    elif approved_payment:
        payment_status = "подтверждена ✅"
    elif any(payment.get("status") == "pending" for payment in payments):
        payment_status = "ожидает подтверждения ⏳"
    else:
        payment_status = "не найдена"

    lines = [
        "👤 <b>Мой аккаунт</b>",
        f"Оплата: {payment_status}",
        f"Всего заявок на оплату: {len(payments)}",
        "",
        "📱 <b>Номера с авторассылками</b>",
    ]
    if accounts:
        for account in accounts:
            marker = "✅" if active_account_id is not None and int(account["id"]) == int(active_account_id) else "•"
            lines.append(f"{marker} {format_account_display(account)}")
    else:
        lines.append("Номеров пока нет. Добавьте номер в разделе авторассылки.")
    return "\n".join(lines)


async def build_admin_payments_text(limit: int = 50) -> str:
    payments = await storage.get_all_payments()
    if not payments:
        return "📜 Пока нет заявок на оплату."

    lines = ["📜 <b>Список оплат</b>"]
    for payment in payments[:limit]:
        status = payment.get("status")
        symbol = {"approved": "✅", "pending": "⏳", "declined": "❌"}.get(status, "•")
        created = format_datetime(payment.get("created_at"))
        resolved_at = payment.get("resolved_at")
        expires_text = ""
        if status == "approved" and resolved_at:
            try:
                expires_dt = datetime.fromisoformat(resolved_at) + timedelta(days=PAYMENT_VALID_DAYS)
                expires_text = f", до {expires_dt.strftime('%d.%m.%Y')}"
            except ValueError:
                pass
        full_name = payment.get("full_name") or "—"
        username = payment.get("username")
        user_display = full_name
        if username:
            user_display += f" (@{username})"
        card_number = payment.get("card_number") or "—"
        status_name = {
            "approved": "оплачено",
            "pending": "ожидает подтверждения",
            "declined": "отклонено",
        }.get(status, status)
        lines.append(
            f"{symbol} {user_display}\n"
            f"     Карта: {card_number}\n"
            f"     Статус: {status_name} ({created}{expires_text})"
        )
    return "\n".join(lines)


def admin_stats_keyboard(period: str) -> InlineKeyboardMarkup:
    period = period if period in {"day", "week", "month", "all"} else "day"
    labels = {
        "day": "День",
        "week": "Неделя",
        "month": "Месяц",
        "all": "Всё",
    }
    rows = []
    for first, second in (("day", "week"), ("month", "all")):
        rows.append(
            [
                InlineKeyboardButton(
                    ("● " if period == first else "") + labels[first],
                    callback_data=f"admin_stats:{first}",
                ),
                InlineKeyboardButton(
                    ("● " if period == second else "") + labels[second],
                    callback_data=f"admin_stats:{second}",
                ),
            ]
        )
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="auto:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_stats_period(period: str) -> tuple[str, Optional[datetime]]:
    now = datetime.utcnow()
    if period == "week":
        return "за 7 дней", now - timedelta(days=7)
    if period == "month":
        return "за 30 дней", now - timedelta(days=30)
    if period == "all":
        return "за всё время", None
    return "за сегодня", now.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


async def build_admin_stats_text(period: str = "day") -> str:
    period_title, since = admin_stats_period(period)
    payments = await storage.get_all_payments()
    period_payments = []
    approved_in_period = []
    pending_in_period = []
    declined_in_period = []
    active_user_ids = set()
    now = datetime.utcnow()
    active_threshold = now - timedelta(days=PAYMENT_VALID_DAYS)

    for payment in payments:
        created_at = parse_iso_datetime(payment.get("created_at"))
        resolved_at = parse_iso_datetime(payment.get("resolved_at"))
        status = payment.get("status")
        if since is None or (created_at and created_at >= since):
            period_payments.append(payment)
            if status == "pending":
                pending_in_period.append(payment)
            elif status == "declined":
                declined_in_period.append(payment)
        if status == "approved" and resolved_at:
            if since is None or resolved_at >= since:
                approved_in_period.append(payment)
            if resolved_at >= active_threshold:
                active_user_ids.add(int(payment.get("user_id")))

    campaign_starts = await storage.count_auto_campaign_starts(since=since)
    deliveries = await storage.count_auto_deliveries(since=since)
    active_campaigns = await storage.count_active_auto_campaigns()
    revenue = len(approved_in_period) * PAYMENT_AMOUNT

    latest_payment = await storage.latest_payment_timestamp()
    if latest_payment:
        payment_due = latest_payment + timedelta(days=PAYMENT_VALID_DAYS)
        global_payment_line = f"Общая оплата активна до {payment_due.strftime('%d.%m.%Y')}"
    else:
        global_payment_line = "Общая оплата не найдена"

    lines = [
        f"📊 <b>Админ-аналитика {period_title}</b>",
        "",
        "💳 <b>Платежи и подписки</b>",
        f"Заявок на оплату: {len(period_payments)}",
        f"Оформлено подписок: {len(approved_in_period)}",
        f"Ожидают подтверждения: {len(pending_in_period)}",
        f"Отклонено: {len(declined_in_period)}",
        f"Активных подписок сейчас: {len(active_user_ids)}",
        f"Выручка периода: {format_currency(revenue, PAYMENT_CURRENCY)}",
        global_payment_line,
        "",
        "📨 <b>Рассылки</b>",
        f"Запусков рассылки: {campaign_starts}",
        f"Отправлено сообщений: {deliveries}",
        f"Активно сейчас: {active_campaigns}",
    ]
    return "\n".join(lines)


async def build_main_menu(user_id: int) -> tuple[str, InlineKeyboardMarkup, bool]:
    is_admin = await is_admin_user(user_id)
    text = WELCOME_TEXT_ADMIN if is_admin else WELCOME_TEXT_USER
    return text, main_menu_keyboard(is_admin, allow_group_pick=True), is_admin


async def send_main_menu(message: types.Message, *, edit: bool = False, user_id: Optional[int] = None) -> None:
    uid = user_id or (message.from_user.id if message.from_user else message.chat.id)
    text, keyboard, _ = await build_main_menu(uid)
    if edit:
        try:
            await message.edit_text(text, reply_markup=keyboard)
        except exceptions.MessageNotModified:
            pass
    else:
        await message.answer(text, reply_markup=keyboard)


async def safe_edit_text(message: types.Message, text: str, **kwargs: Any) -> None:
    try:
        await message.edit_text(text, **kwargs)
    except exceptions.MessageNotModified:
        pass


async def show_auto_menu(message: types.Message, auto_data: dict, *, user_id: int) -> None:
    status = "Активна ✅" if auto_data.get("is_enabled") else "Не запущена"
    message_preview_raw = auto_data.get("message") or "— не задано"
    if len(message_preview_raw) > 180:
        message_preview_raw = message_preview_raw[:177] + "..."
    message_preview = quote_html(message_preview_raw)
    if len(message_preview) > 180:
        message_preview = message_preview[:177] + "..."
    interval = auto_data.get("interval_minutes") or 0
    targets = auto_data.get("target_chat_ids") or []
    accounts = await storage.list_user_accounts(user_id)
    selected_account_id = auto_data.get("sender_account_id")
    selected_account = None
    if selected_account_id is not None:
        for account in accounts:
            if int(account["id"]) == int(selected_account_id):
                selected_account = account
                break
    personal_api_available = bool(message.bot.get("personal_api_available"))
    allow_group_pick = True
    if selected_account:
        sender_line = f"Номер для рассылки: {format_account_display(selected_account)}"
        known = await storage.list_known_chats(account_id=selected_account_id, owner_id=user_id)
        total_known = len(known)
        if targets:
            total = total_known or len(targets)
            group_line = f"Группы номера: {len(targets)} выбрано из {total}"
        else:
            group_line = (
                f"Группы номера: все {total_known} чатов"
                if total_known
                else "Группы номера: нет сохранённых чатов"
            )
    elif message.bot.get("user_sender"):
        auto_sender: Optional[AutoSender] = message.bot.get("auto_sender")
        available = 0
        if auto_sender:
            personal_chats = await auto_sender.get_personal_chats(refresh=True)
            available = len(personal_chats)
        sender_line = "Номер для рассылки: общий личный аккаунт"
        if targets:
            total = available or len(targets)
            group_line = f"Группы пользователя: {len(targets)} выбрано из {total}"
        else:
            group_line = (
                f"Группы пользователя: все {available} чатов"
                if available
                else "Группы пользователя: нет доступных чатов"
            )
    else:
        sender_line = (
            "Номер для рассылки: бот"
            if not personal_api_available
            else "Номер для рассылки: бот (можно выбрать личный номер)"
        )
        group_line = f"Выбрано групп: {len(targets)}"
    system_payment_valid = await storage.has_recent_payment(within_days=PAYMENT_VALID_DAYS)
    latest_payment = await storage.latest_payment_timestamp()
    if system_payment_valid and latest_payment:
        expires_dt = latest_payment + timedelta(days=PAYMENT_VALID_DAYS)
        system_payment_line = f"Общая оплата: действительна до {expires_dt.strftime('%d.%m.%Y')} ✅"
    else:
        system_payment_line = f"Общая оплата: требуется пополнение (каждые {PAYMENT_VALID_DAYS} дней)"
    payment_lines = []
    is_admin = await is_admin_user(user_id)
    personal_valid = await storage.has_recent_payment_for_user(user_id, within_days=PAYMENT_VALID_DAYS)
    if personal_valid:
        personal_ts = await storage.latest_payment_timestamp_for_user(user_id)
        if personal_ts:
            personal_expires = personal_ts + timedelta(days=PAYMENT_VALID_DAYS)
            payment_lines.append(f"Ваша оплата: активна до {personal_expires.strftime('%d.%m.%Y')} ✅")
        else:
            payment_lines.append("Ваша оплата: подтверждена ✅")
    else:
        payment_lines.append(
            "Ваша оплата: не найдена или просрочена. Пополните баланс и дождитесь подтверждения. "
            "Если платеж уже был, попросите администратора нажать «🔁 Перепроверить оплату»."
        )
    if is_admin:
        payment_lines.append(system_payment_line)
    payment_line = "\n".join(payment_lines)
    text = (
        f"🛠 {hbold('Авторассылка')}\n\n"
        f"Статус: {status}\n"
        f"Интервал: {interval} мин\n"
        f"{sender_line}\n"
        f"{group_line}\n\n"
        f"{payment_line}\n\n"
        f"Сообщение:\n{message_preview}"
    )
    try:
        await message.edit_text(
            text,
            reply_markup=auto_menu_keyboard(
                is_enabled=auto_data.get("is_enabled"),
                allow_group_pick=allow_group_pick,
                allow_account_pick=personal_api_available,
            ),
        )
    except exceptions.MessageNotModified:
        pass


async def show_account_menu(message: types.Message, *, user_id: int) -> None:
    accounts = await storage.list_user_accounts(user_id)
    auto_data = await storage.get_auto(user_id)
    active_account_id = auto_data.get("sender_account_id")
    allow_bot_sender = True
    bot_label = "Отправлять от бота"
    if message.bot.get("user_sender"):
        bot_label = "Отправлять от общего аккаунта"
    lines = [
        "📱 <b>Номера для рассылки</b>",
        "Выберите аккаунт, с которого будет идти авторассылка.",
    ]
    if accounts:
        for account in accounts:
            marker = "✅" if active_account_id is not None and int(account["id"]) == int(active_account_id) else "•"
            proxy_suffix = " 🌐" if get_account_proxy(account) else ""
            lines.append(f"{marker} {format_account_display(account)}{proxy_suffix}")
    else:
        lines.append("Пока нет подключённых номеров. Нажмите «➕ Добавить номер», чтобы пройти подтверждение.")
    text = "\n".join(lines)
    keyboard = accounts_keyboard(
        accounts,
        active_account_id=active_account_id,
        allow_bot_sender=allow_bot_sender,
        bot_label=bot_label,
    )
    sender_is_bot = bool(message.from_user and message.from_user.is_bot)
    if sender_is_bot:
        await safe_edit_text(message, text, reply_markup=keyboard)
    else:
        await message.answer(text, reply_markup=keyboard)


def personal_api_ready(bot_obj: Bot) -> bool:
    return bool(bot_obj.get("personal_api_available"))


@dp.callback_query_handler(lambda c: c.data == "auto:pick_account")
async def cb_auto_pick_account(call: types.CallbackQuery) -> None:
    if not personal_api_ready(call.bot):
        await call.answer("Добавление личных номеров отключено. Укажите TG_USER_API_ID и TG_USER_API_HASH.", show_alert=True)
        return
    await call.answer()
    await show_account_menu(call.message, user_id=call.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "accounts:back")
async def cb_accounts_back(call: types.CallbackQuery) -> None:
    await call.answer()
    auto_data = await storage.get_auto(call.from_user.id)
    await show_auto_menu(call.message, auto_data, user_id=call.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "accounts:add", state="*")
async def cb_accounts_add(call: types.CallbackQuery, state: FSMContext) -> None:
    if not personal_api_ready(call.bot):
        await call.answer("Добавление личных номеров недоступно.", show_alert=True)
        return
    await call.answer()
    await replace_pending_account(call.from_user.id, None)
    await AccountStates.waiting_for_phone.set()
    await call.message.answer(
        "Отправьте номер телефона, который подключён к Telegram (в международном формате, например +998901234567).\n"
        "Мы пришлём на него код подтверждения. Используйте /cancel для отмены."
    )


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("accounts:set:"))
async def cb_accounts_set(call: types.CallbackQuery) -> None:
    if not personal_api_ready(call.bot):
        await call.answer("Невозможно выбрать номер — личные аккаунты отключены.", show_alert=True)
        return
    parts = call.data.split(":", 2)
    if len(parts) < 3:
        await call.answer("Некорректная команда.", show_alert=True)
        return
    target = parts[2]
    user_id = call.from_user.id
    if target == "bot":
        await storage.set_user_sender_account(user_id, None)
        await call.answer("Рассылка будет идти от бота.", show_alert=True)
    else:
        try:
            account_id = int(target)
        except ValueError:
            await call.answer("Некорректный идентификатор аккаунта.", show_alert=True)
            return
        account = await storage.get_user_account(account_id, owner_id=user_id)
        if not account:
            await call.answer("Аккаунт не найден или недоступен.", show_alert=True)
            return
        await storage.set_user_sender_account(user_id, account_id)
        await call.answer("Номер выбран для рассылки.", show_alert=True)
    require_targets = await should_require_targets(call.bot, user_id)
    await storage.ensure_constraints(user_id=user_id, require_targets=require_targets)
    auto_sender: AutoSender = call.bot["auto_sender"]
    await auto_sender.refresh_user(user_id)
    await show_account_menu(call.message, user_id=user_id)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("accounts:proxy:"), state="*")
async def cb_accounts_proxy(call: types.CallbackQuery, state: FSMContext) -> None:
    if not personal_api_ready(call.bot):
        await call.answer("Личные аккаунты отключены.", show_alert=True)
        return
    parts = call.data.split(":", 2)
    if len(parts) < 3:
        await call.answer("Некорректная команда.", show_alert=True)
        return
    try:
        account_id = int(parts[2])
    except ValueError:
        await call.answer("Некорректный идентификатор аккаунта.", show_alert=True)
        return
    account = await storage.get_user_account(account_id, owner_id=call.from_user.id)
    if not account:
        await call.answer("Аккаунт недоступен.", show_alert=True)
        return
    proxy = get_account_proxy(account)
    current_status = format_proxy_display(proxy) if proxy else "не настроен"
    await AccountStates.waiting_for_proxy.set()
    await state.update_data(proxy_account_id=account_id)
    await call.answer()
    await call.message.answer(
        "Отправьте параметры прокси одним сообщением.\n"
        "Формат: socks5://login:pass@host:port или host:port[:login[:password]].\n"
        "Поддерживаются схемы socks5/socks4/http. Чтобы отключить прокси, напишите off.\n\n"
        f"Текущее состояние: {current_status}."
    )


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("accounts:refresh:"))
async def cb_accounts_refresh(call: types.CallbackQuery) -> None:
    if not personal_api_ready(call.bot):
        await call.answer("Личные номера отключены.", show_alert=True)
        return
    parts = call.data.split(":", 2)
    if len(parts) < 3:
        await call.answer("Некорректная команда.", show_alert=True)
        return
    try:
        account_id = int(parts[2])
    except ValueError:
        await call.answer("Некорректный идентификатор.", show_alert=True)
        return
    account = await storage.get_user_account(account_id, owner_id=call.from_user.id)
    if not account:
        await call.answer("Аккаунт не найден.", show_alert=True)
        return
    await call.answer("Обновляем чаты…")
    success = await refresh_account_chats(call.bot, call.from_user.id, account_id)
    if success:
        await call.message.answer("Список чатов обновлён. Теперь можно выбрать группы.")
    else:
        await call.message.answer("Не удалось обновить чаты. Проверьте, что аккаунт авторизован и состоит в нужных группах.")
    await show_account_menu(call.message, user_id=call.from_user.id)


async def _get_personal_api_credentials(bot_obj: Bot) -> tuple[Optional[int], Optional[str]]:
    return bot_obj.get("personal_api_id"), bot_obj.get("personal_api_hash")


@dp.message_handler(state=AccountStates.waiting_for_phone, content_types=types.ContentTypes.TEXT)
async def handle_account_phone(message: types.Message, state: FSMContext) -> None:
    if await handle_possible_cancel(message, state):
        return
    if not personal_api_ready(message.bot):
        await message.reply("Добавление персональных номеров временно недоступно.")
        await state.finish()
        return
    normalized = normalize_phone(message.text or "")
    if not normalized or len("".join(ch for ch in normalized if ch.isdigit())) < 8:
        await message.reply("Отправьте номер телефона в формате +998901234567.")
        return
    api_id, api_hash = await _get_personal_api_credentials(message.bot)
    if not api_id or not api_hash:
        await message.reply("Сервер не настроен для подключения номеров. Обратитесь к администратору.")
        await state.finish()
        return
    shared_proxy = message.bot.get("shared_proxy")
    client = TelegramClient(
        StringSession(),
        api_id,
        api_hash,
        proxy=build_telethon_proxy(shared_proxy),
        connection_retries=2,
        request_retries=2,
        timeout=10,
    )
    try:
        await client.connect()
        sent = await client.send_code_request(normalized)
    except PhoneNumberInvalidError:
        await message.reply("Telegram отклоняет этот номер. Убедитесь, что аккаунт активен и попробуйте снова.")
        await client.disconnect()
        return
    except FloodWaitError as exc:
        await message.reply(f"Telegram попросил подождать {exc.seconds} секунд перед следующей попыткой.")
        await client.disconnect()
        return
    except Exception as exc:
        logger.exception("Не удалось отправить код подтверждения на %s", normalized)
        if shared_proxy:
            await message.reply(build_shared_proxy_error_text(message.bot))
        else:
            await message.reply(f"Не удалось отправить код: {exc}. Попробуйте ещё раз чуть позже.")
        await client.disconnect()
        return
    pending = PendingAccountLogin(
        client=client,
        phone=normalized,
        phone_code_hash=sent.phone_code_hash,
    )
    await replace_pending_account(message.from_user.id, pending)
    await AccountStates.waiting_for_code.set()
    await message.answer(
        "Код выслан в Telegram. Пришлите его одним сообщением.\n"
        "Если передумали, используйте /cancel."
    )


@dp.message_handler(state=AccountStates.waiting_for_code, content_types=types.ContentTypes.TEXT)
async def handle_account_code(message: types.Message, state: FSMContext) -> None:
    if await handle_possible_cancel(message, state):
        return
    pending = await get_pending_account(message.from_user.id)
    if not pending:
        await message.reply("Нет активной сессии подтверждения. Нажмите «➕ Добавить номер» ещё раз.")
        await state.finish()
        return
    code = "".join(ch for ch in (message.text or "") if ch.isdigit())
    if not code:
        await message.reply("Введите код цифрами.")
        return
    try:
        await pending.client.sign_in(phone=pending.phone, code=code, phone_code_hash=pending.phone_code_hash)
    except SessionPasswordNeededError:
        pending.awaiting_password = True
        await message.answer("На аккаунте включён пароль. Введите его (буквы учитываются).")
        await AccountStates.waiting_for_password.set()
        return
    except PhoneCodeInvalidError:
        await message.reply("Код неверный. Попробуйте ещё раз.")
        return
    except PhoneCodeExpiredError:
        await message.reply("Срок действия кода истёк. Начните заново с «➕ Добавить номер».")
        await replace_pending_account(message.from_user.id, None)
        await state.finish()
        return
    except Exception:
        logger.exception("Ошибка подтверждения номера для пользователя %s", message.from_user.id)
        await message.reply("Не удалось подтвердить код. Попробуйте позже.")
        await replace_pending_account(message.from_user.id, None)
        await state.finish()
        return
    await finalize_account_login(message, state, pending)


@dp.message_handler(state=AccountStates.waiting_for_password, content_types=types.ContentTypes.TEXT)
async def handle_account_password(message: types.Message, state: FSMContext) -> None:
    if await handle_possible_cancel(message, state):
        return
    pending = await get_pending_account(message.from_user.id)
    if not pending or not pending.awaiting_password:
        await message.reply("Нет активной сессии подтверждения. Начните заново.")
        await state.finish()
        return
    password = (message.text or "").strip()
    if not password:
        await message.reply("Пароль не может быть пустым.")
        return
    try:
        await pending.client.sign_in(password=password)
    except PasswordHashInvalidError:
        await message.reply("Пароль неверный. Попробуйте снова.")
        return
    except Exception:
        logger.exception("Ошибка подтверждения пароля аккаунта пользователя %s", message.from_user.id)
        await message.reply("Не удалось подтвердить пароль. Попробуйте снова.")
        return
    await finalize_account_login(message, state, pending)


@dp.message_handler(state=AccountStates.waiting_for_proxy, content_types=types.ContentTypes.TEXT)
async def handle_account_proxy_input(message: types.Message, state: FSMContext) -> None:
    if await handle_possible_cancel(message, state):
        return
    data = await state.get_data()
    account_id = data.get("proxy_account_id")
    if not account_id:
        await message.reply("Аккаунт не выбран. Вернитесь к списку номеров и повторите попытку.")
        await state.finish()
        return
    text = (message.text or "").strip()
    if not text:
        await message.reply("Укажите адрес прокси или напишите off для отключения.")
        return
    lowered = text.lower()
    if lowered in PROXY_DISABLE_WORDS:
        proxy = None
    else:
        try:
            proxy = parse_proxy_string(text)
        except ValueError as exc:
            await message.reply(str(exc))
            return
    updated = await storage.update_user_account_proxy(message.from_user.id, int(account_id), proxy=proxy)
    if not updated:
        await message.reply("Аккаунт не найден или недоступен.")
        await state.finish()
        return
    if proxy:
        await message.answer(f"Прокси сохранён: {format_proxy_display(proxy)}")
    else:
        await message.answer("Прокси отключён. Трафик будет идти без обхода.")
    await state.finish()
    await show_account_menu(message, user_id=message.from_user.id)


@dp.message_handler(state=SharedProxyStates.waiting_for_proxy, content_types=types.ContentTypes.TEXT)
async def handle_shared_proxy_input(message: types.Message, state: FSMContext) -> None:
    if await handle_possible_cancel(message, state):
        return
    if not await is_admin_user(message.from_user.id):
        await message.reply("Настройка доступна только администраторам.")
        await state.finish()
        return
    if message.bot.get("shared_proxy_source") == "env":
        await message.reply("Текущий прокси задан через TG_USER_PROXY. Измените .env на сервере.")
        await state.finish()
        return
    text = (message.text or "").strip()
    if not text:
        await message.reply("Отправьте адрес прокси или off для отключения.")
        return
    lowered = text.lower()
    proxy: Optional[Dict[str, Any]]
    if lowered in PROXY_DISABLE_WORDS:
        proxy = None
    else:
        try:
            proxy = parse_proxy_string(text)
        except ValueError as exc:
            await message.reply(str(exc))
            return
    await storage.set_shared_proxy(proxy)
    message.bot["shared_proxy"] = proxy
    message.bot["shared_proxy_source"] = "db" if proxy else None
    await state.finish()
    sender = await replace_bot_user_sender(message.bot)
    if proxy:
        await message.answer(f"Прокси сохранён: {format_proxy_display(proxy)}")
    else:
        await message.answer("Прокси отключён. Теперь используется прямое подключение.")
    session_value = message.bot.get("personal_session")
    if not session_value:
        await message.answer(
            "Общий пользовательский аккаунт пока не активен: укажите TG_USER_SESSION в .env, чтобы его использовать."
        )
    elif sender is None:
        await message.answer("Не удалось запустить общий пользовательский аккаунт. Проверьте TG_USER_SESSION.")
    else:
        identity = await sender.describe_self()
        await message.answer(f"Общий аккаунт подключён: {identity}.")
    await send_main_menu(message, user_id=message.from_user.id)


async def finalize_account_login(message: types.Message, state: FSMContext, pending: PendingAccountLogin) -> None:
    user_id = message.from_user.id
    try:
        me = await pending.client.get_me()
    except Exception:
        me = None
    username = getattr(me, "username", None) if me else None
    full_name = " ".join(filter(None, [getattr(me, "first_name", None), getattr(me, "last_name", None)])) if me else None
    title = full_name or username or f"Аккаунт {mask_phone(pending.phone)}"
    session_string = pending.client.session.save()
    try:
        account = await storage.create_user_account(
            user_id,
            phone=pending.phone,
            session=session_string,
            title=title,
            username=username,
        )
    except Exception:
        logger.exception("Не удалось сохранить личный аккаунт пользователя %s", user_id)
        await message.reply("Не удалось сохранить аккаунт. Попробуйте ещё раз позже.")
        await replace_pending_account(user_id, None)
        await state.finish()
        return
    account_id = int(account["id"])
    await storage.set_user_sender_account(user_id, account_id)
    await storage.clear_target_chats(user_id, account_id=account_id)
    require_targets = await should_require_targets(message.bot, user_id)
    await storage.ensure_constraints(user_id=user_id, require_targets=require_targets)
    await refresh_account_chats(message.bot, user_id, account_id)
    auto_sender: AutoSender = message.bot["auto_sender"]
    await auto_sender.refresh_user(user_id)
    await replace_pending_account(user_id, None)
    await state.finish()
    await message.answer(
        f"Номер {format_account_display(account)} подключён.\n"
        "Используйте кнопку «📱 Номер» в меню авторассылки, чтобы выбрать группы и начать отправку."
    )


async def load_available_chats(
    user_id: int,
    bot_obj: Bot,
    *,
    auto_data: Optional[Dict[str, Any]] = None,
) -> tuple[Dict[str, Dict[str, Any]], Optional[int]]:
    auto = auto_data or await storage.get_auto(user_id)
    account_id = auto.get("sender_account_id")
    if account_id is not None:
        known = await storage.list_known_chats(account_id=account_id, owner_id=user_id)
        if not known:
            await refresh_account_chats(bot_obj, user_id, account_id)
            known = await storage.list_known_chats(account_id=account_id, owner_id=user_id)
        return known, account_id
    if bot_obj.get("user_sender"):
        auto_sender: Optional[AutoSender] = bot_obj.get("auto_sender")
        if auto_sender:
            await auto_sender.get_personal_chats(refresh=True)
    known = await storage.list_known_chats()
    return known, None


@dp.message_handler(commands=["start", "menu"], state="*")
async def cmd_start(message: types.Message, state: FSMContext) -> None:
    if await answer_sleep_message_if_needed(message):
        return
    await state.finish()
    await send_main_menu(message)


@dp.message_handler(commands=["help"], state="*")
async def cmd_help(message: types.Message) -> None:
    if await answer_sleep_message_if_needed(message):
        return
    await message.answer(f"Поддержка: {SUPPORT_AGENT_USERNAME}\nНаш ИИ-агент поможет с вопросами по боту.")


@dp.message_handler(commands=["cancel"], state="*")
async def cmd_cancel(message: types.Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state is None:
        return
    user_id = message.from_user.id if message.from_user else message.chat.id
    is_account_state = current_state.startswith(AccountStates.__name__)
    if is_account_state:
        await replace_pending_account(user_id, None)
    await state.finish()
    if is_account_state:
        await message.answer("Действие отменено. Возвращаемся к выбору номера.")
        await show_account_menu(message, user_id=user_id)
        return
    await message.answer("Действие отменено. Возвращаемся в меню.")
    await send_main_menu(message)


async def handle_possible_cancel(message: types.Message, state: FSMContext) -> bool:
    text = (message.text or "").strip().lower()
    if text == "/cancel":
        await cmd_cancel(message, state)
        return True
    return False


@dp.message_handler(commands=["history", "payments"], state="*")
async def cmd_user_payments(message: types.Message, state: FSMContext) -> None:
    await state.finish()
    text = await build_user_payment_history_text(message.from_user.id)
    await message.answer(text)
    await send_main_menu(message)


@dp.message_handler(commands=["payments_all"], state="*")
async def cmd_admin_payments(message: types.Message, state: FSMContext) -> None:
    await state.finish()
    if not await is_admin_user(message.from_user.id):
        await message.answer("Команда доступна только администраторам.")
        return
    text = await build_admin_payments_text()
    await message.answer(text)
    await send_main_menu(message)


@dp.message_handler(commands=["dumps"], state="*")
async def cmd_list_dumps(message: types.Message, state: FSMContext) -> None:
    await state.finish()
    if not await is_admin_user(message.from_user.id):
        await message.answer("Команда доступна только администраторам.")
        return
    dumps = await storage.list_audience_dumps(message.from_user.id, limit=10)
    if not dumps:
        await message.answer("Пока нет сохранённых выгрузок аудитории.")
        await send_main_menu(message)
        return
    lines = ["🗂 <b>Последние выгрузки</b>"]
    for dump in dumps:
        created = format_datetime(dump.get("created_at"))
        lines.append(
            f"#{dump['id'][:6]} — {dump['total_users']} логинов, источник {dump['source']} ({created})"
        )
    await message.answer("\n".join(lines))
    await send_main_menu(message)


@dp.message_handler(commands=["jobs"], state="*")
async def cmd_list_jobs(message: types.Message, state: FSMContext) -> None:
    await state.finish()
    if not await is_admin_user(message.from_user.id):
        await message.answer("Команда доступна только администраторам.")
        return
    jobs = await storage.list_invite_jobs(message.from_user.id, limit=10)
    if not jobs:
        await message.answer("Нет активных или завершённых задач инвайта.")
        await send_main_menu(message)
        return
    lines = ["📦 <b>Задачи инвайта</b>"]
    for job in jobs:
        status = job.get("status")
        status_emoji = {
            "pending": "⏳",
            "running": "⚙️",
            "completed": "✅",
            "failed": "❌",
        }.get(status, "•")
        created = format_datetime(job.get("created_at"))
        lines.append(
            f"{status_emoji} #{job['id'][:6]} — {job['invited_count']}/{job['total_users']} приглашено, "
            f"чат {job['target_chat']} ({created})"
        )
    await message.answer("\n".join(lines))
    await send_main_menu(message)


@dp.message_handler(commands=["stop_all_mailings", "стоп_рассылки"], state="*")
async def cmd_stop_all_mailings(message: types.Message, state: FSMContext) -> None:
    await state.finish()
    if not await is_admin_user(message.from_user.id):
        await message.answer("Команда доступна только администраторам.")
        return
    disabled_count = await storage.disable_all_auto()
    auto_sender: Optional[AutoSender] = message.bot.get("auto_sender")
    if auto_sender:
        await auto_sender.stop_all()
    await message.answer(
        "Все авторассылки остановлены."
        if disabled_count
        else "Активных авторассылок не было."
    )
    await send_main_menu(message)


@dp.message_handler(commands=["админ"], state="*")
async def cmd_admin_login_ru(message: types.Message, state: FSMContext) -> None:
    await cmd_admin_login(message, state)


@dp.message_handler(commands=["admin"], state="*")
async def cmd_admin_login(message: types.Message, state: FSMContext) -> None:
    await state.finish()
    if await is_admin_user(message.from_user.id):
        await message.answer("Вы уже авторизованы как администратор.")
        await send_main_menu(message)
        return
    await AdminLoginStates.waiting_for_code.set()
    await message.answer("Введите код администратора:")


@dp.message_handler(state=AdminLoginStates.waiting_for_code, content_types=types.ContentTypes.TEXT)
async def process_admin_code(message: types.Message, state: FSMContext) -> None:
    if await handle_possible_cancel(message, state):
        return
    code = (message.text or "").strip()
    if code != ADMIN_INVITE_CODE:
        await message.reply("Неверный код. Попробуйте снова или используйте /cancel.")
        return
    await storage.set_user_role(message.from_user.id, "admin")
    await state.finish()
    await message.answer("Статус администратора активирован.")
    await send_main_menu(message)


@dp.message_handler(state=ParserStates.waiting_for_channel, content_types=types.ContentTypes.TEXT)
async def parser_wait_channel(message: types.Message, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        await state.finish()
        await message.answer("Недостаточно прав для парсинга.")
        return
    if await handle_possible_cancel(message, state):
        return
    source = (message.text or "").strip()
    if not source:
        await message.answer("Укажите ссылку или @username канала.")
        return
    await state.update_data(parser_source=source)
    await state.set_state(ParserStates.waiting_for_limit.state)
    await message.answer("Сколько последних постов обрабатывать? Укажите число от 1 до 500.")


@dp.message_handler(state=ParserStates.waiting_for_limit, content_types=types.ContentTypes.TEXT)
async def parser_wait_limit(message: types.Message, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        await state.finish()
        await message.answer("Недостаточно прав для парсинга.")
        return
    if await handle_possible_cancel(message, state):
        return
    text = (message.text or "").strip()
    try:
        limit = max(1, min(500, int(text)))
    except ValueError:
        await message.answer("Введите число от 1 до 500. Например: 50")
        return
    data = await state.get_data()
    source = data.get("parser_source")
    if not source:
        await state.finish()
        await message.answer("Не удалось определить канал. Запустите парсер снова.")
        await send_main_menu(message)
        return
    parser: Optional[AudienceParser] = message.bot.get("audience_parser")
    if not parser:
        await state.finish()
        await message.answer("Парсер недоступен: подключите личный API или аккаунт.")
        await send_main_menu(message)
        return
    await message.answer("🔍 Начинаю собирать комментарии, это может занять пару минут...")
    try:
        dump = await parser.parse_comments(
            message.from_user.id,
            source=source,
            limit=limit,
            account_id=await get_active_sender_account_id(message.from_user.id),
        )
    except Exception as exc:  # pragma: no cover - network operations
        logger.exception("Не удалось выполнить парсинг %s для %s", source, message.from_user.id)
        await message.answer(f"Не удалось выполнить парсинг: {exc}")
    else:
        path = Path(dump["file_path"])
        caption = (
            f"Готово! Найдено {dump['total_users']} логинов.\n"
            f"Источник: {dump['source']}\n"
            "Файл сохранён в /data/dumps."
        )
        if path.exists():
            try:
                await message.bot.send_document(message.chat.id, InputFile(path), caption=caption)
            except Exception:
                logger.exception("Не удалось отправить файл %s", path)
                await message.answer(f"{caption}\nФайл: {path}")
        else:
            await message.answer(caption)
    await state.finish()
    await send_main_menu(message)


@dp.message_handler(state=GroupParserStates.waiting_for_group, content_types=types.ContentTypes.TEXT)
async def group_parser_wait_choice(message: types.Message, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        await state.finish()
        await message.answer("Недостаточно прав для парсинга.")
        return
    if await handle_possible_cancel(message, state):
        return
    parser: Optional[AudienceParser] = message.bot.get("audience_parser")
    if not parser:
        await state.finish()
        await message.answer("Парсер недоступен: подключите личный API или аккаунт.")
        await send_main_menu(message)
        return
    data = await state.get_data()
    groups = data.get("group_parser_choices") or []
    choice = (message.text or "").strip()
    selected: Optional[Dict[str, Any]] = None
    if choice.isdigit():
        idx = int(choice)
        if 1 <= idx <= len(groups):
            selected = groups[idx - 1]
    else:
        lookup = choice.lstrip("@").lower()
        for group in groups:
            username = (group.get("username") or "").lower()
            if username and username == lookup:
                selected = group
                break
            if str(group["id"]) == choice:
                selected = group
                break
    target = None
    source_label = choice
    if selected:
        target = selected.get("username") or int(selected["id"])
        source_label = selected.get("title") or str(selected["id"])
    else:
        if choice.startswith("@"):
            target = choice
        else:
            try:
                target = int(choice)
            except ValueError:
                target = choice
    await message.answer("👥 Начинаю собирать участников выбранной группы...")
    try:
        dump = await parser.parse_group_members(
            message.from_user.id,
            group=target,
            account_id=await get_active_sender_account_id(message.from_user.id),
        )
    except Exception as exc:  # pragma: no cover - network work
        logger.exception("Не удалось выполнить групповой парсинг %s", target)
        await message.answer(f"Не удалось собрать участников: {exc}")
    else:
        path = Path(dump["file_path"])
        caption = (
            f"Готово! Собрано {dump['total_users']} участников.\n"
            f"Группа: {source_label}\n"
            "Файл сохранён в /data/dumps."
        )
        if path.exists():
            try:
                await message.bot.send_document(message.chat.id, InputFile(path), caption=caption)
            except Exception:
                logger.exception("Не удалось отправить файл %s", path)
                await message.answer(f"{caption}\nФайл: {path}")
        else:
            await message.answer(caption)
    await state.finish()
    await send_main_menu(message)


@dp.message_handler(state=InviteStates.waiting_for_file, content_types=types.ContentTypes.ANY)
async def invite_wait_file(message: types.Message, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        await state.finish()
        await message.answer("Недостаточно прав для запуска инвайта.")
        return
    if await handle_possible_cancel(message, state):
        return
    document = message.document
    if not document:
        await message.answer("Пришлите .txt файл с логинами (по одному @username в строке).")
        return
    uploads_dir = BASE_DIR / "data" / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    original_name = document.file_name or "usernames.txt"
    safe_name = "".join(ch for ch in original_name if ch.isalnum() or ch in ("_", "-", ".")) or "usernames.txt"
    path = uploads_dir / f"{timestamp}_{safe_name}"
    await document.download(destination_file=str(path))
    await state.update_data(invite_file=str(path))
    await state.set_state(InviteStates.waiting_for_target.state)
    await message.answer("Файл сохранён. Укажите @username или ссылку на чат/канал, куда будем приглашать.")


@dp.message_handler(state=InviteStates.waiting_for_target, content_types=types.ContentTypes.TEXT)
async def invite_wait_target(message: types.Message, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        await state.finish()
        await message.answer("Недостаточно прав для запуска инвайта.")
        return
    if await handle_possible_cancel(message, state):
        return
    target = (message.text or "").strip()
    if not target:
        await message.answer("Укажите ссылку или @username чата.")
        return
    await state.update_data(invite_target=target)
    await state.set_state(InviteStates.waiting_for_limits.state)
    await message.answer(
        "Укажите лимиты через пробел: <b>инвайтов_на_аккаунт задержка_сек потоки</b>.\n"
        "Например: <code>20 8 3</code> — по 20 приглашений на аккаунт, задержка 8 секунд, 3 аккаунта параллельно."
    )


@dp.message_handler(state=InviteStates.waiting_for_limits, content_types=types.ContentTypes.TEXT)
async def invite_wait_limits(message: types.Message, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        await state.finish()
        await message.answer("Недостаточно прав для запуска инвайта.")
        return
    if await handle_possible_cancel(message, state):
        return
    parts = (message.text or "").replace(",", " ").split()
    try:
        invites_per_account = max(1, int(parts[0]))
    except (IndexError, ValueError):
        invites_per_account = 10
    try:
        delay_seconds = max(0.5, float(parts[1]))
    except (IndexError, ValueError):
        delay_seconds = 5.0
    try:
        thread_limit = max(1, int(parts[2]))
    except (IndexError, ValueError):
        thread_limit = 1
    data = await state.get_data()
    file_path_raw = data.get("invite_file")
    target = data.get("invite_target")
    if not file_path_raw or not target:
        await state.finish()
        await message.answer("Данные не найдены, начните настройку заново.")
        await send_main_menu(message)
        return
    engine: Optional[InviteEngine] = message.bot.get("invite_engine")
    if not engine:
        await state.finish()
        await message.answer("Инвайтер недоступен: подключите личные аккаунты.")
        await send_main_menu(message)
        return
    jitter = max(0.5, delay_seconds * 0.4)
    await message.answer(
        "🚀 Задача поставлена. Приглашения начнутся после подготовки сессий.\n"
        "Проверить статус можно командой /jobs."
    )
    try:
        job = await engine.start_job(
            message.from_user.id,
            target_chat=target,
            usernames_file=Path(file_path_raw),
            settings={
                "invites_per_account": invites_per_account,
                "delay_seconds": delay_seconds,
                "thread_limit": thread_limit,
                "delay_jitter": jitter,
            },
        )
        await message.answer(
            f"Задача #{job['id'][:6]}: ожидается {job['total_users']} приглашений в {target}."
        )
    except Exception as exc:  # pragma: no cover - MTProto operations
        logger.exception("Не удалось запустить инвайт %s", target)
        await message.answer(f"Не удалось запустить задачу: {exc}")
    await state.finish()
    await send_main_menu(message)


@dp.callback_query_handler(lambda c: c.data == "main:auto")
async def cb_main_auto(call: types.CallbackQuery) -> None:
    await call.answer()
    auto_data = await storage.get_auto(call.from_user.id)
    await show_auto_menu(call.message, auto_data, user_id=call.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "main:parser")
async def cb_main_parser(call: types.CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    parser: Optional[AudienceParser] = call.bot.get("audience_parser")
    if not parser:
        await call.answer("Парсер недоступен. Добавьте личный API или аккаунт.", show_alert=True)
        return
    await state.set_state(ParserStates.waiting_for_channel.state)
    await call.message.answer(
        "Отправьте @username или ссылку на канал, из которого нужно собрать комментарии.\n"
        "Используйте /cancel для выхода."
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "main:group_parser")
async def cb_main_group_parser(call: types.CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    parser: Optional[AudienceParser] = call.bot.get("audience_parser")
    if not parser:
        await call.answer("Парсер недоступен. Добавьте личный API или аккаунт.", show_alert=True)
        return
    try:
        groups = await parser.list_personal_groups(
            call.from_user.id,
            account_id=await get_active_sender_account_id(call.from_user.id),
        )
    except Exception as exc:  # pragma: no cover - network
        logger.exception("Не удалось получить список групп для парсинга.")
        await call.answer(f"Ошибка подключения к аккаунту: {exc}", show_alert=True)
        return
    if not groups:
        await call.answer("Нет доступных групп в личной сессии.", show_alert=True)
        return
    await GroupParserStates.waiting_for_group.set()
    await state.update_data(group_parser_choices=groups)
    lines = [
        "👥 <b>Выберите группу для парсинга</b>",
        "Отправьте номер из списка или @username/ID группы. /cancel — отмена.",
        "",
    ]
    for idx, group in enumerate(groups, start=1):
        mention = f"@{group['username']}" if group.get("username") else group["id"]
        lines.append(f"{idx}. {quote_html(group['title'])} — {mention}")
    await call.message.answer("\n".join(lines))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "main:inviter")
async def cb_main_inviter(call: types.CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    invite_engine: Optional[InviteEngine] = call.bot.get("invite_engine")
    if not invite_engine:
        await call.answer(
            "Инвайтер недоступен: подключите хотя бы один личный аккаунт через личный API.",
            show_alert=True,
        )
        return
    await state.set_state(InviteStates.waiting_for_file.state)
    await call.message.answer(
        "Загрузите .txt файл с логинами (по одному @username в строке). "
        "Команда /cancel отменяет настройку."
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "main:stats")
async def cb_main_stats(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    await safe_edit_text(
        call.message,
        await build_admin_stats_text("day"),
        reply_markup=admin_stats_keyboard("day"),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("admin_stats:"))
async def cb_admin_stats_period(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    period = call.data.split(":", 1)[1]
    if period not in {"day", "week", "month", "all"}:
        await call.answer("Неизвестный период.", show_alert=True)
        return
    await call.answer()
    await safe_edit_text(
        call.message,
        await build_admin_stats_text(period),
        reply_markup=admin_stats_keyboard(period),
    )


@dp.callback_query_handler(lambda c: c.data == "main:groups")
async def cb_main_groups(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    auto = await storage.get_auto(call.from_user.id)
    known, account_id = await load_available_chats(call.from_user.id, call.bot, auto_data=auto)
    selected = auto.get("target_chat_ids") or []
    if not known:
        text, keyboard, _ = await build_main_menu(call.from_user.id)
        if account_id is not None or personal_api_ready(call.bot):
            empty_text = (
                "📋 Пока нет групп для рассылки.\n"
                "Добавьте личный аккаунт в рабочие чаты и повторите попытку."
            )
        else:
            empty_text = (
                "📋 Пока нет групп для рассылки.\n"
                "Добавьте бота в нужные чаты и убедитесь, что он может отправлять сообщения, затем повторите попытку."
            )
        await safe_edit_text(
            call.message,
            empty_text,
            reply_markup=keyboard,
        )
        return
    await safe_edit_text(
        call.message,
        GROUPS_BASE_TEXT,
        reply_markup=groups_keyboard(
            known,
            selected,
            origin="main",
            page=0,
            per_page=GROUPS_PAGE_SIZE,
        ),
    )


@dp.callback_query_handler(lambda c: c.data == "main:settings")
async def cb_main_settings(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    auto = await storage.get_auto(call.from_user.id)
    interval = auto.get("interval_minutes")
    message_text_raw = auto.get("message") or "— не задано"
    message_text = quote_html(message_text_raw)
    status = "Активна" if auto.get("is_enabled") else "Отключена"
    targets = auto.get("target_chat_ids") or []
    if call.bot.get("user_sender"):
        auto_sender: Optional[AutoSender] = call.bot.get("auto_sender")
        available = 0
        if auto_sender:
            personal_chats = await auto_sender.get_personal_chats(refresh=True)
            available = len(personal_chats)
        if targets:
            total = available or len(targets)
            group_line = f"Группы пользователя: {len(targets)} выбрано из {total}"
        else:
            group_line = (
                f"Группы пользователя: все {available} чатов"
                if available
                else "Группы пользователя: нет доступных чатов"
            )
    else:
        group_line = f"Группы: {len(targets)} выбрано"
    payment_valid = await storage.has_recent_payment(within_days=PAYMENT_VALID_DAYS)
    latest_payment = await storage.latest_payment_timestamp()
    if payment_valid and latest_payment:
        expires_dt = latest_payment + timedelta(days=PAYMENT_VALID_DAYS)
        payment_line = f"Оплата: действительна до {expires_dt.strftime('%d.%m.%Y')} ✅"
    else:
        payment_line = f"Оплата: требуется пополнение (каждые {PAYMENT_VALID_DAYS} дней)"
    text = (
        "⚙️ <b>Настройки рассылки</b>\n"
        f"Статус: {status}\n"
        f"Интервал: {interval} мин\n"
        f"{group_line}\n"
        f"{payment_line}\n\n"
        f"Сообщение:\n{message_text}"
    )
    _, keyboard, _ = await build_main_menu(call.from_user.id)
    await call.message.edit_text(text, reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data == "main:shared_proxy", state="*")
async def cb_main_shared_proxy(call: types.CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    if call.bot.get("shared_proxy_source") == "env":
        current = call.bot.get("shared_proxy")
        status = format_proxy_display(current) if current else "не задан"
        await call.message.answer(
            "🌐 Общий прокси для MTProto задан через переменную окружения TG_USER_PROXY.\n"
            f"Текущее значение: {status}.\n"
            "Чтобы изменить его, обновите .env на сервере и перезапустите бота."
        )
        return
    current = call.bot.get("shared_proxy")
    status = format_proxy_display(current) if current else "не настроен"
    await SharedProxyStates.waiting_for_proxy.set()
    await call.message.answer(
        "Отправьте параметры прокси одним сообщением.\n"
        "Формат: socks5://login:pass@host:port или host:port[:login[:password]].\n"
        "Команда off отключит прокси. Используйте /cancel для отмены.\n\n"
        f"Текущее состояние: {status}."
    )


@dp.callback_query_handler(lambda c: c.data == "main:account")
async def cb_main_account(call: types.CallbackQuery) -> None:
    await call.answer()
    text = await build_my_account_text(call.from_user.id)
    await call.message.edit_text(
        text,
        reply_markup=my_account_keyboard(allow_account_pick=personal_api_ready(call.bot)),
    )


@dp.callback_query_handler(lambda c: c.data == "main:pay")
async def cb_main_pay(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    admin_ids = await collect_admin_ids()
    eligible_admin_ids = {
        admin_id
        for admin_id in admin_ids
        if admin_id != call.from_user.id or await is_admin_user(call.from_user.id)
    }
    if not eligible_admin_ids:
        await call.message.answer(
            "Платёж недоступен: не настроены администраторы для подтверждения."
        )
        return
    await state.finish()
    await PaymentStates.waiting_for_card_number.set()
    await call.message.answer(
        f"Для пополнения баланса: {PAYMENT_DESCRIPTION}.\n"
        f"Сумма к оплате: {format_currency(PAYMENT_AMOUNT, PAYMENT_CURRENCY)}.\n\n"
        f"После подтверждения оплата действует {PAYMENT_VALID_DAYS} дней.\n\n"
        f"Переведите сумму на карту <code>{PAYMENT_CARD_TARGET}</code> и введите номер своей карты ниже.\n\n"
        f"{PAYMENT_CARD_PROMPT}",
        disable_web_page_preview=True,
    )


@dp.callback_query_handler(lambda c: c.data == "main:user_payments")
async def cb_main_user_payments(call: types.CallbackQuery) -> None:
    await call.answer()
    text = await build_user_payment_history_text(call.from_user.id)
    _, keyboard, _ = await build_main_menu(call.from_user.id)
    await call.message.edit_text(text, reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data == "main:admin_payments")
async def cb_main_admin_payments(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    text = await build_admin_payments_text()
    _, keyboard, _ = await build_main_menu(call.from_user.id)
    await call.message.edit_text(text, reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data == "main:manual_payment")
async def cb_main_manual_payment(call: types.CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    await AdminManualPaymentStates.waiting_for_user.set()
    await call.message.answer(
        "Введите Telegram ID или @username пользователя, чтобы перепроверить оплату.\n"
        "Используйте /cancel для отмены."
    )


@dp.callback_query_handler(lambda c: c.data == "auto:back")
async def cb_auto_back(call: types.CallbackQuery) -> None:
    await call.answer()
    await send_main_menu(call.message, edit=True, user_id=call.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "auto:set_message")
async def cb_auto_set_message(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    await AutoCampaignStates.waiting_for_message.set()
    await call.message.answer(
        "Отправьте новый текст сообщения для авторассылки.\n"
        "Используйте /cancel для отмены."
    )


@dp.message_handler(state=AutoCampaignStates.waiting_for_message, content_types=types.ContentTypes.TEXT)
async def process_auto_message(message: types.Message, state: FSMContext) -> None:
    if await handle_possible_cancel(message, state):
        return
    text = (message.text or "").strip()
    if not text:
        await message.reply("Сообщение не может быть пустым. Попробуйте снова.")
        return
    await storage.set_auto_message(message.from_user.id, text)
    require_targets = await should_require_targets(message.bot, message.from_user.id)
    await storage.ensure_constraints(user_id=message.from_user.id, require_targets=require_targets)
    auto_sender: AutoSender = message.bot["auto_sender"]
    await auto_sender.refresh_user(message.from_user.id)
    await state.finish()
    await message.answer("Сообщение сохранено.")
    auto_data = await storage.get_auto(message.from_user.id)
    await message.answer(
        "Параметры авторассылки обновлены.",
        reply_markup=auto_menu_keyboard(
            is_enabled=auto_data.get("is_enabled"),
            allow_group_pick=True,
            allow_account_pick=bool(message.bot.get("personal_api_available")),
        ),
    )


@dp.callback_query_handler(lambda c: c.data == "auto:set_interval")
async def cb_auto_set_interval(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    await AutoCampaignStates.waiting_for_interval.set()
    await call.message.answer(
        "Укажите интервал рассылки в минутах (целое число > 0).\n"
        "Используйте /cancel для отмены."
    )


@dp.message_handler(state=AutoCampaignStates.waiting_for_interval)
async def process_auto_interval(message: types.Message, state: FSMContext) -> None:
    if await handle_possible_cancel(message, state):
        return
    content = (message.text or "").strip()
    if not content.isdigit():
        await message.reply("Нужно целое число минут. Попробуйте ещё раз.")
        return
    minutes = int(content)
    if minutes <= 0:
        await message.reply("Интервал должен быть больше нуля.")
        return
    await storage.set_auto_interval(message.from_user.id, minutes)
    require_targets = await should_require_targets(message.bot, message.from_user.id)
    await storage.ensure_constraints(user_id=message.from_user.id, require_targets=require_targets)
    auto_sender: AutoSender = message.bot["auto_sender"]
    await auto_sender.refresh_user(message.from_user.id)
    await state.finish()
    await message.answer(f"Интервал установлен: {minutes} мин.")
    auto_data = await storage.get_auto(message.from_user.id)
    await message.answer(
        "Параметры авторассылки обновлены.",
        reply_markup=auto_menu_keyboard(
            is_enabled=auto_data.get("is_enabled"),
            allow_group_pick=True,
            allow_account_pick=bool(message.bot.get("personal_api_available")),
        ),
    )


@dp.message_handler(state=AdminManualPaymentStates.waiting_for_user, content_types=types.ContentTypes.TEXT)
async def process_manual_payment_user(message: types.Message, state: FSMContext) -> None:
    if await handle_possible_cancel(message, state):
        return
    if not await is_admin_user(message.from_user.id):
        await message.reply("Доступно только администраторам.")
        await state.finish()
        return
    raw = (message.text or "").strip()
    user_id: Optional[int] = None
    if raw.startswith("@") and len(raw) > 1:
        found = await storage.find_user_id_by_username(raw[1:])
        if found:
            user_id = found
        else:
            await message.reply(
                "Не удалось найти пользователя по username. Укажите числовой Telegram ID или повторите попытку."
            )
            return
    elif raw.isdigit():
        user_id = int(raw)
    if user_id is None:
        await message.reply("Нужно указать Telegram ID (цифры) или @username. Попробуйте ещё раз.")
        return
    payment = await storage.get_latest_payment_for_user(user_id)
    info_lines = [f"Перепроверка пользователя <code>{user_id}</code>."]
    if payment:
        info_lines.append(
            f"Последний статус: {payment.get('status')} (создано {format_datetime(payment.get('created_at'))})"
        )
    else:
        info_lines.append("Ранее оплаты не найдены.")
    info_lines.append("Выберите результат перепроверки:")
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Подтвердить", callback_data=f"manual_payment:approve:{user_id}"),
        InlineKeyboardButton("❌ Не подтверждать", callback_data=f"manual_payment:decline:{user_id}"),
    )
    await state.finish()
    await message.answer("\n".join(info_lines), reply_markup=keyboard)


@dp.message_handler(state=PaymentStates.waiting_for_card_number, content_types=types.ContentTypes.TEXT)
async def process_payment_card_number(message: types.Message, state: FSMContext) -> None:
    if await handle_possible_cancel(message, state):
        return
    digits = "".join(filter(str.isdigit, message.text or ""))
    if len(digits) < 12 or len(digits) > 19:
        await message.reply(PAYMENT_CARD_INVALID_MESSAGE)
        return
    formatted = " ".join(digits[i : i + 4] for i in range(0, len(digits), 4))
    await state.update_data(card_number=formatted)
    await PaymentStates.waiting_for_card_name.set()
    await message.answer(PAYMENT_CARD_NAME_PROMPT)


@dp.message_handler(state=PaymentStates.waiting_for_card_name, content_types=types.ContentTypes.TEXT)
async def process_payment_card_name(message: types.Message, state: FSMContext) -> None:
    if await handle_possible_cancel(message, state):
        return
    card_name = (message.text or "").strip()
    if len(card_name) < 3:
        await message.reply(PAYMENT_CARD_NAME_INVALID_MESSAGE)
        return
    data = await state.get_data()
    card_number = data.get("card_number")
    if not card_number:
        await state.finish()
        await message.answer("Что-то пошло не так. Попробуйте снова начать оплату.")
        return
    user = message.from_user
    request_id = await storage.create_payment_request(
        user_id=user.id,
        username=user.username,
        full_name=user.full_name or user.username or str(user.id),
        card_number=card_number,
        card_name=card_name,
    )
    await notify_admins_about_payment(user.id, request_id)
    await message.answer(PAYMENT_THANK_YOU_MESSAGE)
    await state.finish()


@dp.callback_query_handler(lambda c: c.data == "auto:pick_groups")
async def cb_auto_pick_groups(call: types.CallbackQuery) -> None:
    await call.answer()
    auto = await storage.get_auto(call.from_user.id)
    known, account_id = await load_available_chats(call.from_user.id, call.bot, auto_data=auto)
    selected = auto.get("target_chat_ids") or []
    if not known:
        _, keyboard, _ = await build_main_menu(call.from_user.id)
        if account_id is not None or personal_api_ready(call.bot):
            empty_text = (
                "📋 Пока нет групп для рассылки.\nДобавьте личный аккаунт в рабочие чаты и повторите попытку."
            )
        else:
            empty_text = (
                "📋 Пока нет групп для рассылки.\nДобавьте бота в нужные чаты и убедитесь, что он может отправлять сообщения, затем повторите попытку."
            )
        await safe_edit_text(
            call.message,
            empty_text,
            reply_markup=keyboard,
        )
        return
    await safe_edit_text(
        call.message,
        GROUPS_BASE_TEXT,
        reply_markup=groups_keyboard(
            known,
            selected,
            origin="auto",
            page=0,
            per_page=GROUPS_PAGE_SIZE,
        ),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("group:"))
async def cb_group_toggle(call: types.CallbackQuery) -> None:
    await call.answer()
    parts = call.data.split(":")
    if len(parts) < 3:
        await call.answer("Неизвестная команда", show_alert=True)
        return
    _, origin, action, *extra = parts
    page = 0
    user_id = call.from_user.id
    if action == "noop":
        return
    if action == "done":
        if origin == "main":
            await send_main_menu(call.message, edit=True, user_id=user_id)
        else:
            auto_data = await storage.get_auto(user_id)
            await show_auto_menu(call.message, auto_data, user_id=user_id)
        return
    auto = await storage.get_auto(user_id)

    async def fetch_known(auto_snapshot: Optional[Dict[str, Any]] = None) -> tuple[
        Dict[str, Dict[str, Any]], Dict[str, Any], Optional[int]
    ]:
        snapshot = auto_snapshot or await storage.get_auto(user_id)
        known_chats, account = await load_available_chats(user_id, call.bot, auto_data=snapshot)
        return known_chats, snapshot, account

    if action == "page":
        if extra:
            try:
                page = max(0, int(extra[0]))
            except ValueError:
                page = 0
        known, auto, account_id = await fetch_known(auto)
        if not known:
            await call.answer("Нет доступных групп.", show_alert=True)
            return
        await safe_edit_text(
            call.message,
            GROUPS_BASE_TEXT,
            reply_markup=groups_keyboard(
                known,
                auto.get("target_chat_ids"),
                origin=origin,
                page=page,
                per_page=GROUPS_PAGE_SIZE,
            ),
        )
        return
    if action in {"select_all", "clear_all"}:
        if extra:
            try:
                page = max(0, int(extra[0]))
            except ValueError:
                page = 0
        known, auto, account_id = await fetch_known(auto)
        if not known:
            await call.answer("Нет доступных групп.", show_alert=True)
            return
        if action == "clear_all":
            await storage.clear_target_chats(user_id, account_id=account_id)
            status_line = "Все группы сняты из рассылки."
        else:
            all_ids = sorted(int(info["chat_id"]) for info in known.values())
            if not all_ids:
                await call.answer("Нет групп для выбора.", show_alert=True)
                return
            await storage.set_target_chats(user_id, all_ids, account_id=account_id)
            status_line = "Все группы выбраны для рассылки."
        require_targets = await should_require_targets(call.bot, user_id)
        await storage.ensure_constraints(
            user_id=user_id,
            require_targets=require_targets,
        )
        auto_sender: AutoSender = call.bot["auto_sender"]
        await auto_sender.refresh_user(user_id)
        known, auto, account_id = await fetch_known(None)
        reply_text = (
            f"{GROUPS_BASE_TEXT}\n\n"
            f"{status_line}\nПри необходимости уточните список или нажмите 'Готово'."
        )
        await safe_edit_text(
            call.message,
            reply_text,
            reply_markup=groups_keyboard(
                known,
                auto.get("target_chat_ids"),
                origin=origin,
                page=page,
                per_page=GROUPS_PAGE_SIZE,
            ),
        )
        return
    if action == "chat":
        if not extra:
            await call.answer("Некорректные данные.", show_alert=True)
            return
        try:
            chat_id = int(extra[0])
        except ValueError:
            await call.answer("Некорректный идентификатор чата", show_alert=True)
            return
        if len(extra) > 1:
            try:
                page = max(0, int(extra[1]))
            except ValueError:
                page = 0
    else:
        try:
            chat_id = int(action)
        except ValueError:
            await call.answer("Некорректные данные.", show_alert=True)
            return
        known, auto, account_id = await fetch_known(auto)
        sorted_items = sorted(known.items(), key=lambda item: item[1].get("title", ""))
        index_lookup = {int(chat_id_str): idx for idx, (chat_id_str, _) in enumerate(sorted_items)}
        if chat_id in index_lookup:
            page = index_lookup[chat_id] // GROUPS_PAGE_SIZE
    known, auto, account_id = await fetch_known(auto)
    title_raw = (known.get(str(chat_id)) or {}).get("title") or str(chat_id)
    title = quote_html(title_raw)
    selected = await storage.toggle_target_chat(user_id, chat_id, title_raw, account_id=account_id)
    require_targets = await should_require_targets(call.bot, user_id)
    await storage.ensure_constraints(
        user_id=user_id,
        require_targets=require_targets,
    )
    auto_sender: AutoSender = call.bot["auto_sender"]
    await auto_sender.refresh_user(user_id)
    known, auto, account_id = await fetch_known(None)
    reply_text = (
        f"{GROUPS_BASE_TEXT}\n\n"
        f"Чат {'добавлен в' if selected else 'убран из'} рассылки: {title}\n"
        "При необходимости выберите другие чаты или нажмите 'Готово'."
    )
    await safe_edit_text(
        call.message,
        reply_text,
        reply_markup=groups_keyboard(
            known,
            auto.get("target_chat_ids"),
            origin=origin,
            page=page,
            per_page=GROUPS_PAGE_SIZE,
        ),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("manual_payment:"))
async def cb_manual_payment_decision(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Недостаточно прав.", show_alert=True)
        return
    try:
        _, action, user_id_raw = call.data.split(":", maxsplit=2)
        user_id = int(user_id_raw)
    except (ValueError, TypeError):
        await call.answer("Некорректные данные.", show_alert=True)
        return
    if action not in {"approve", "decline"}:
        await call.answer("Неизвестное действие.", show_alert=True)
        return
    last_payment = await storage.get_latest_payment_for_user(user_id)
    username = (last_payment or {}).get("username")
    full_name = (last_payment or {}).get("full_name") or (username and f"@{username}") or f"Пользователь {user_id}"
    card_number = (last_payment or {}).get("card_number") or "manual-check"
    card_name = (last_payment or {}).get("card_name") or "Перепроверка"
    request_id = await storage.create_payment_request(
        user_id=user_id,
        username=username,
        full_name=full_name,
        card_number=card_number,
        card_name=card_name,
    )
    updated = await storage.set_payment_status(
        request_id,
        status="approved" if action == "approve" else "declined",
        admin_id=call.from_user.id,
        admin_username=call.from_user.username,
    )
    if not updated:
        await call.answer("Не удалось обновить заявку.", show_alert=True)
        return
    status_message = build_user_payment_status_message(updated.get("status"), updated.get("resolved_at"))
    user_id = updated.get("user_id")
    await send_payment_status_to_user(user_id, status_message)
    admin_text = build_payment_admin_text(updated)
    await call.message.edit_text("Перепроверка завершена:\n\n" + admin_text)
    auto_sender: Optional[AutoSender] = call.bot.get("auto_sender")
    if auto_sender and user_id:
        await auto_sender.refresh_user(user_id)
    await call.answer("Решение сохранено.")


@dp.callback_query_handler(lambda c: c.data.startswith("payment:"))
async def cb_payment_decision(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Недостаточно прав.", show_alert=True)
        return
    try:
        _, action, request_id = call.data.split(":", maxsplit=2)
    except ValueError:
        await call.answer("Неверный формат данных.", show_alert=True)
        return
    payment = await storage.get_payment(request_id)
    if not payment:
        await call.answer("Заявка не найдена.", show_alert=True)
        return
    if payment.get("status") != "pending":
        await call.answer("Заявка уже обработана.", show_alert=True)
        return
    if action not in {"approve", "decline"}:
        await call.answer("Неизвестное действие.", show_alert=True)
        return
    status = "approved" if action == "approve" else "declined"
    updated = await storage.set_payment_status(
        request_id,
        status=status,
        admin_id=call.from_user.id,
        admin_username=call.from_user.username,
    )
    if not updated:
        await call.answer("Не удалось обновить заявку.", show_alert=True)
        return
    status_message = build_user_payment_status_message(status, updated.get("resolved_at"))
    user_id = updated.get("user_id")
    await send_payment_status_to_user(user_id, status_message)
    admin_text = build_payment_admin_text(updated)
    await call.message.edit_text(admin_text)
    await call.answer("Решение сохранено.")


@dp.callback_query_handler(lambda c: c.data == "main:payments_pdf")
async def cb_main_payments_pdf(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    payments = await storage.get_all_payments()
    if not payments:
        await call.message.answer("Пока нет заявок на оплату.")
        return
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    pdf_path = BASE_DIR / "data" / f"payments_{timestamp}.pdf"
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, build_payments_pdf, payments, pdf_path)
    try:
        await call.message.answer_document(
            InputFile(str(pdf_path)),
            caption="Отчёт по оплатам (PDF).",
        )
    finally:
        try:
            pdf_path.unlink()
        except OSError:
            pass


@dp.callback_query_handler(lambda c: c.data == "auto:start")
async def cb_auto_start(call: types.CallbackQuery) -> None:
    await call.answer()
    auto = await storage.get_auto(call.from_user.id)
    if not auto.get("message"):
        await call.message.answer("Сначала задайте текст сообщения.")
        return
    selected_targets = auto.get("target_chat_ids") or []
    selected_account_id = auto.get("sender_account_id")
    if selected_account_id is not None:
        known = await storage.list_known_chats(account_id=selected_account_id, owner_id=call.from_user.id)
        if not known:
            refreshed = await refresh_account_chats(call.bot, call.from_user.id, int(selected_account_id))
            if refreshed:
                known = await storage.list_known_chats(account_id=selected_account_id, owner_id=call.from_user.id)
        if not known:
            await call.message.answer(
                "У выбранного номера нет сохранённых групп. Нажмите «📱 Номер» → «🔄 Обновить чаты» "
                "или добавьте аккаунт в рабочие группы."
            )
            return
        available_ids = {int(chat_id) for chat_id in known.keys()}
        if selected_targets:
            valid_targets = [chat_id for chat_id in selected_targets if chat_id in available_ids]
            if not valid_targets:
                await call.message.answer(
                    "Выбранные группы недоступны для этого номера. Обновите список чатов и выберите группы заново."
                )
                return
        else:
            selected_targets = list(available_ids)
    elif call.bot.get("user_sender"):
        auto_sender: AutoSender = call.bot["auto_sender"]
        personal_chats = await auto_sender.get_personal_chats(refresh=True)
        if not personal_chats:
            await call.message.answer(
                "Личный аккаунт не состоит ни в одной группы. Добавьте его в рабочие чаты и попробуйте снова."
            )
            return
        available_ids = set(personal_chats.keys())
        if selected_targets:
            valid_targets = [chat_id for chat_id in selected_targets if chat_id in available_ids]
            if not valid_targets:
                await call.message.answer(
                    "Выбранные группы недоступны для личного аккаунта. Обновите список и попробуйте снова."
                )
                return
        else:
            selected_targets = list(available_ids)
        if not selected_targets:
            await call.message.answer("Нет доступных групп для рассылки.")
            return
    else:
        if not selected_targets:
            await call.message.answer("Не выбрано ни одной группы для рассылки.")
            return
    if (auto.get("interval_minutes") or 0) <= 0:
        await call.message.answer("Неверный интервал. Укажите значение больше нуля.")
        return
    if not await storage.has_recent_payment_for_user(call.from_user.id, within_days=PAYMENT_VALID_DAYS):
        await call.message.answer(
            "Для запуска авторассылки вам нужна подтверждённая оплата. "
            "Если платёж уже был, попросите администратора нажать «🔁 Перепроверить оплату» и подтвердить его."
        )
        return
    if not await storage.has_recent_payment(within_days=PAYMENT_VALID_DAYS):
        await call.message.answer(
            f"Для запуска авторассылки необходимо актуальное пополнение баланса за последние {PAYMENT_VALID_DAYS} дней."
        )
        return
    await storage.set_auto_enabled(call.from_user.id, True)
    await storage.record_auto_campaign_start(call.from_user.id)
    auto_sender: AutoSender = call.bot["auto_sender"]
    await auto_sender.refresh_user(call.from_user.id)
    await call.message.answer("Авторассылка запущена.")
    updated = await storage.get_auto(call.from_user.id)
    await show_auto_menu(call.message, updated, user_id=call.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "auto:stop")
async def cb_auto_stop(call: types.CallbackQuery) -> None:
    await call.answer()
    await storage.set_auto_enabled(call.from_user.id, False)
    auto_sender: AutoSender = call.bot["auto_sender"]
    await auto_sender.stop_user(call.from_user.id)
    await call.message.answer("Авторассылка остановлена.")
    updated = await storage.get_auto(call.from_user.id)
    await show_auto_menu(call.message, updated, user_id=call.from_user.id)


@dp.message_handler(
    lambda message: message.chat.type == types.ChatType.PRIVATE and not (message.text or "").startswith("/"),
    content_types=types.ContentTypes.ANY,
    state="*",
)
async def handle_private_message_without_command(message: types.Message, state: FSMContext) -> None:
    if await answer_sleep_message_if_needed(message):
        return
    if await state.get_state():
        return
    await send_main_menu(message)


@dp.my_chat_member_handler()
async def handle_my_chat_member(update: types.ChatMemberUpdated) -> None:
    new_status = update.new_chat_member.status
    chat = update.chat
    if chat.type not in (types.ChatType.GROUP, types.ChatType.SUPERGROUP):
        return
    title = chat.title or chat.full_name or str(chat.id)
    if new_status in (
        types.ChatMemberStatus.ADMINISTRATOR,
        types.ChatMemberStatus.CREATOR,
        types.ChatMemberStatus.MEMBER,
    ):
        await storage.upsert_known_chat(chat.id, title)
        logger.info("Добавлен чат %s (%s)", chat.id, title)
    elif new_status in (
        types.ChatMemberStatus.LEFT,
        types.ChatMemberStatus.KICKED,
        types.ChatMemberStatus.RESTRICTED,
    ):
        await storage.remove_known_chat(chat.id)
        logger.info("Удалён чат %s", chat.id)


@dp.message_handler(content_types=types.ContentTypes.TEXT, chat_type=[types.ChatType.GROUP, types.ChatType.SUPERGROUP])
async def handle_group_text(message: types.Message) -> None:
    chat = message.chat
    title_raw = chat.title or chat.full_name or str(chat.id)
    bot_id = message.bot.get("bot_id")
    if bot_id is None:
        me = await message.bot.get_me()
        bot_id = me.id
        message.bot["bot_id"] = bot_id
    member = await message.bot.get_chat_member(chat.id, bot_id)
    if member.status in (
        types.ChatMemberStatus.ADMINISTRATOR,
        types.ChatMemberStatus.CREATOR,
        types.ChatMemberStatus.MEMBER,
    ):
        await storage.upsert_known_chat(chat.id, title_raw)


async def on_startup(dispatcher: Dispatcher) -> None:
    me = await dispatcher.bot.get_me()
    await dispatcher.bot.set_my_commands(
        [
            types.BotCommand("start", "Открыть меню"),
            types.BotCommand("help", "Поддержка"),
            types.BotCommand("cancel", "Отменить текущий шаг"),
            types.BotCommand("stop_all_mailings", "Остановить все авторассылки"),
        ]
    )
    await sync_shared_proxy_from_storage(dispatcher.bot)
    user_sender_instance = await replace_bot_user_sender(dispatcher.bot)
    if user_sender_instance:
        try:
            identity = await user_sender_instance.describe_self()
            logger.info("Пользовательская рассылка активирована от %s", identity)
        except Exception:
            logger.exception("Не удалось определить пользователя для общей сессии.")
    account_manager: Optional[AccountManager] = dispatcher.bot.get("account_manager")
    auto_sender = AutoSender(
        dispatcher.bot,
        storage,
        PAYMENT_VALID_DAYS,
        user_sender=user_sender_instance,
        account_manager=account_manager,
    )
    dispatcher.bot["auto_sender"] = auto_sender
    if user_sender_instance:
        await auto_sender.get_personal_chats(refresh=True)
    dispatcher.bot["bot_id"] = me.id
    await storage.ensure_constraints(
        user_id=None,
        require_targets=dispatcher.bot.get("user_sender") is None,
    )
    await auto_sender.start_if_enabled()
    logger.info("Бот %s (%s) запущен", me.first_name, me.id)


async def on_shutdown(dispatcher: Dispatcher) -> None:
    auto_sender: Optional[AutoSender] = dispatcher.bot.get("auto_sender")
    if auto_sender:
        await auto_sender.stop_all()
    user_sender_instance: Optional[UserSender] = dispatcher.bot.get("user_sender")
    if user_sender_instance:
        await user_sender_instance.stop()
    account_manager: Optional[AccountManager] = dispatcher.bot.get("account_manager")
    if account_manager:
        await account_manager.stop_all()
    await dispatcher.storage.close()
    await dispatcher.storage.wait_closed()


if __name__ == "__main__":
    retry_delay_raw = os.getenv("POLLING_RETRY_DELAY", "5")
    try:
        retry_delay = int(retry_delay_raw)
    except ValueError:
        retry_delay = 5
    retry_delay = max(1, retry_delay)
    while True:
        try:
            executor.start_polling(dp, skip_updates=False, on_startup=on_startup, on_shutdown=on_shutdown)
            break
        except exceptions.TerminatedByOtherGetUpdates:
            logger.warning(
                "Получен сигнал о другом активном getUpdates. Ждём %s c и пробуем снова.",
                retry_delay,
            )
            # Два инстанса могут короткое время пересекаться при деплое, поэтому просто ждём и пробуем ещё раз.
            asyncio.run(asyncio.sleep(retry_delay))
