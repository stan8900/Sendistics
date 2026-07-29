import asyncio
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional, Set

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.utils import exceptions, executor
from aiogram.utils.markdown import hbold, quote_html
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from dotenv import load_dotenv

from app.auto_sender import AutoSender
from app.keyboards import auto_menu_keyboard, groups_keyboard, main_menu_keyboard, inbox_reply_keyboard
from app.pdf_reports import build_payments_pdf
from app.states import AutoCampaignStates, PaymentStates, AdminLoginStates, AdminManualPaymentStates, AdminInboxStates
from app.storage import Storage
from app.user_delivery import UserDelivery


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Missng BOT_TOKEN")

database_url = os.getenv("DATABASE_URL") or None
database_required = os.getenv("DATABASE_URL_REQUIRED", "false").lower() in {"1", "true", "yes"}
storage_path_env = os.getenv("STORAGE_PATH")
if storage_path_env:
    storage_path = Path(storage_path_env)
    if not storage_path.is_absolute():
        storage_path = (BASE_DIR / storage_path).resolve()
else:
    storage_path = (BASE_DIR / "data" / "storage.db").resolve()

if storage_path.suffix == ".json":
    legacy_storage_path = storage_path
    storage_path = storage_path.with_suffix(".db")
else:
    legacy_storage_path = storage_path.with_suffix(".json")


def create_storage() -> Storage:
    if database_url:
        attempts = 5
        for attempt in range(1, attempts + 1):
            try:
                logger.info("Используем PostgreSQL хранилище (попытка %s/%s).", attempt, attempts)
                return Storage(storage_path, legacy_json_path=legacy_storage_path, database_url=database_url)
            except Exception:
                logger.exception("Не удалось подключиться к PostgreSQL (попытка %s).", attempt)
                if attempt == attempts:
                    if database_required:
                        raise
                    logger.warning("Переходим на локальную SQLite-базу по пути %s.", storage_path)
                    break
                wait_for = min(5, attempt)
                logger.info("Повторяем подключение через %s c.", wait_for)
                time.sleep(wait_for)
    return Storage(storage_path, legacy_json_path=legacy_storage_path)


storage = create_storage()
GROUP_CHAT_TYPES = {types.ChatType.GROUP, types.ChatType.SUPERGROUP}


async def safe_edit_text(message: types.Message, text: str, **kwargs) -> None:
    """Edit message but ignore Telegram 'message not modified' errors."""
    try:
        await message.edit_text(text, **kwargs)
    except exceptions.MessageNotModified:
        return
bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
tg_user_api_id = os.getenv("TG_USER_API_ID")
tg_user_api_hash = os.getenv("TG_USER_API_HASH")
tg_user_session = os.getenv("TG_USER_SESSION")
tg_user_dialogs_limit_raw = os.getenv("TG_USER_DIALOGS_LIMIT")
tg_user_dialogs_limit: Optional[int] = None
if tg_user_dialogs_limit_raw:
    try:
        tg_user_dialogs_limit_value = int(tg_user_dialogs_limit_raw)
    except ValueError:
        logger.warning("Некорректное значение TG_USER_DIALOGS_LIMIT: %s", tg_user_dialogs_limit_raw)
    else:
        if tg_user_dialogs_limit_value > 0:
            tg_user_dialogs_limit = tg_user_dialogs_limit_value

user_delivery: Optional[UserDelivery] = None
try:
    api_id_value = int(tg_user_api_id) if tg_user_api_id else None
except ValueError:
    api_id_value = None
if api_id_value and tg_user_api_hash and tg_user_session:
    user_delivery = UserDelivery(
        api_id=api_id_value,
        api_hash=tg_user_api_hash,
        session_string=tg_user_session,
        dialogs_limit=tg_user_dialogs_limit,
    )
USE_USER_DELIVERY = user_delivery is not None
dp = Dispatcher(bot, storage=MemoryStorage())

bot["storage"] = storage
bot["auto_sender"] = None  # filled on startup
bot["user_delivery"] = user_delivery


async def refresh_user_delivery_chats() -> None:
    if not USE_USER_DELIVERY:
        return
    user_delivery_instance: Optional[UserDelivery] = bot.get("user_delivery")
    if not user_delivery_instance:
        return
    try:
        await user_delivery_instance.sync_known_chats(storage)
    except Exception:
        logger.exception("Не удалось обновить список чатов пользовательского клиента.")

PAYMENT_AMOUNT = 100_000
PAYMENT_CURRENCY = "UZS"
PAYMENT_DESCRIPTION = "Оплата услуг логистического бота"
PAYMENT_VALID_DAYS = 30
PAYMENT_CARD_TARGET = "9860 1701 1433 3116"
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
    "💰 Пополнить баланс — контроль оплат пользователей\n"
    "📊 Статистика — просмотр результатов рассылки\n"
    "📋 Выбрать группы — управление чатами\n"
    "⚙️ Настройки — текущие параметры\n"
    "📜 Оплаты — список активных и ожидающих платежей"
)

WELCOME_TEXT_USER = (
    "👋 Добро пожаловать!\n\n"
    f"💰 Пополнить баланс — отправьте данные оплаты на карту {PAYMENT_CARD_TARGET}.\n"
    "📜 История оплат — проверьте статус заявок и срок подписки.\n\n"
    "Если вы оператор, используйте команду /admin и введите код доступа."
)

STATIC_ADMIN_IDS: Set[int] = {
    int(admin_id.strip())
    for admin_id in os.getenv("ADMIN_IDS", "").split(",")
    if admin_id.strip().isdigit()
}

ADMIN_INVITE_CODE = os.getenv("ADMIN_CODE", "TW13")


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


async def notify_admins_about_incoming_message(message: types.Message) -> bool:
    if not message.from_user:
        return False
    user = message.from_user
    if await is_admin_user(user.id):
        return False
    admin_ids = await collect_admin_ids()
    if not admin_ids:
        return False
    full_name = quote_html(user.full_name or "Неизвестный пользователь")
    username = f"@{user.username}" if user.username else "—"
    preview = message.text or message.caption or ""
    preview = preview.strip()
    if preview:
        if len(preview) > 600:
            preview = preview[:597] + "..."
        preview = quote_html(preview)
    header_lines = [
        "📥 <b>Новое обращение</b>",
        f"Имя: {full_name}",
        f"Username: {username}",
        f"ID: <code>{user.id}</code>",
    ]
    if preview:
        header_lines.append(f"Текст: {preview}")
    header_lines.append("Нажмите «Ответить», чтобы ответить пользователю.")
    header = "\n".join(header_lines)
    keyboard = inbox_reply_keyboard(user.id)
    delivered = False
    for admin_id in admin_ids:
        if admin_id == user.id:
            continue
        if not await is_admin_user(admin_id):
            continue
        try:
            await bot.send_message(admin_id, header, reply_markup=keyboard)
            await bot.forward_message(admin_id, message.chat.id, message.message_id)
            delivered = True
        except exceptions.TelegramAPIError as exc:
            logger.warning(
                "Не удалось переслать сообщение пользователя %s админу %s: %s",
                user.id,
                admin_id,
                exc,
            )
    return delivered


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


async def build_main_menu(user_id: int) -> tuple[str, InlineKeyboardMarkup, bool]:
    is_admin = await is_admin_user(user_id)
    allow_group_pick = True
    text = WELCOME_TEXT_ADMIN if is_admin else WELCOME_TEXT_USER
    return text, main_menu_keyboard(is_admin, allow_group_pick=allow_group_pick), is_admin


async def send_main_menu(message: types.Message, *, edit: bool = False, user_id: Optional[int] = None) -> None:
    uid = user_id or (message.from_user.id if message.from_user else message.chat.id)
    text, keyboard, _ = await build_main_menu(uid)
    if edit:
        await safe_edit_text(message, text, reply_markup=keyboard)
    else:
        await message.answer(text, reply_markup=keyboard)


async def show_auto_menu(message: types.Message, auto_data: dict, *, user_id: Optional[int] = None) -> None:
    if USE_USER_DELIVERY:
        await refresh_user_delivery_chats()
    status = "Активна ✅" if auto_data.get("is_enabled") else "Не запущена"
    message_preview_raw = auto_data.get("message") or "— не задано"
    if len(message_preview_raw) > 180:
        message_preview_raw = message_preview_raw[:177] + "..."
    message_preview = quote_html(message_preview_raw)
    if len(message_preview) > 180:
        message_preview = message_preview[:177] + "..."
    interval = auto_data.get("interval_minutes") or 0
    targets = auto_data.get("target_chat_ids") or []
    known_chats = await storage.list_known_chats()
    available_total = sum(1 for info in known_chats.values() if info.get("delivery_available"))
    agent_name = "пользователя рассылки" if USE_USER_DELIVERY else "бота"
    if targets:
        missing = [
            chat_id
            for chat_id in targets
            if str(chat_id) not in known_chats or not known_chats[str(chat_id)].get("delivery_available")
        ]
        if missing:
            group_line = (
                f"⚠️ Выбрано групп: {len(targets)}. "
                f"Добавьте {agent_name} во все выбранные чаты."
            )
        else:
            group_line = f"Выбрано групп: {len(targets)} из доступных {available_total}"
    else:
        if available_total:
            group_line = f"Группы не выбраны (доступно {available_total})"
        else:
            group_line = f"Нет доступных групп: добавьте {agent_name} в рабочие чаты."
    system_payment_valid = await storage.has_recent_payment(within_days=PAYMENT_VALID_DAYS)
    latest_payment = await storage.latest_payment_timestamp()
    if system_payment_valid and latest_payment:
        expires_dt = latest_payment + timedelta(days=PAYMENT_VALID_DAYS)
        system_payment_line = f"Общая оплата: действительна до {expires_dt.strftime('%d.%m.%Y')} ✅"
    else:
        system_payment_line = f"Общая оплата: требуется пополнение (каждые {PAYMENT_VALID_DAYS} дней)"
    payment_lines = []
    is_admin = None
    if user_id is not None:
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
    if is_admin or user_id is None:
        payment_lines.append(system_payment_line)
    payment_line = "\n".join(payment_lines) if payment_lines else system_payment_line
    allow_group_pick = True
    text = (
        f"🛠 {hbold('Авторассылка')}\n\n"
        f"Статус: {status}\n"
        f"Интервал: {interval} мин\n"
        f"{group_line}\n\n"
        f"{payment_line}\n\n"
        f"Сообщение:\n{message_preview}"
    )
    await safe_edit_text(
        message,
        text,
        reply_markup=auto_menu_keyboard(
            is_enabled=auto_data.get("is_enabled"),
            allow_group_pick=allow_group_pick,
        ),
    )


@dp.message_handler(commands=["start", "menu"], state="*")
async def cmd_start(message: types.Message, state: FSMContext) -> None:
    await state.finish()
    await send_main_menu(message)


@dp.message_handler(commands=["cancel"], state="*")
async def cmd_cancel(message: types.Message, state: FSMContext) -> None:
    if await state.get_state() is None:
        return
    await state.finish()
    await message.answer("Действие отменено. Возвращаемся в меню.")
    await send_main_menu(message)


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
    code = (message.text or "").strip()
    if code != ADMIN_INVITE_CODE:
        await message.reply("Неверный код. Попробуйте снова или используйте /cancel.")
        return
    await storage.set_user_role(message.from_user.id, "admin")
    await state.finish()
    await message.answer("Статус администратора активирован.")
    await send_main_menu(message)


@dp.callback_query_handler(lambda c: c.data == "main:auto")
async def cb_main_auto(call: types.CallbackQuery) -> None:
    await call.answer()
    auto_data = await storage.get_auto(call.from_user.id)
    await show_auto_menu(call.message, auto_data, user_id=call.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "main:stats")
async def cb_main_stats(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    auto = await storage.get_auto()
    stats = auto.get("stats") or {}
    campaigns_total = auto.get("campaigns_total", 0)
    campaigns_active = auto.get("campaigns_active", 0)
    sent_total = stats.get("sent_total", 0)
    last_sent_at = stats.get("last_sent_at")
    last_error = stats.get("last_error")
    latest_payment = await storage.latest_payment_timestamp()
    payment_valid = await storage.has_recent_payment(within_days=PAYMENT_VALID_DAYS)
    human_time = "—"
    if last_sent_at:
        try:
            dt = datetime.fromisoformat(last_sent_at)
            human_time = dt.strftime("%d.%m.%Y %H:%M:%S")
        except ValueError:
            human_time = last_sent_at
    if latest_payment:
        payment_due = latest_payment + timedelta(days=PAYMENT_VALID_DAYS)
        payment_line = (
            f"Оплата действительна до {payment_due.strftime('%d.%m.%Y')}"
            if payment_valid
            else f"Оплата просрочена {payment_due.strftime('%d.%m.%Y')}"
        )
    else:
        payment_line = "Оплата не найдена"
    lines = [
        "📊 <b>Статистика авторассылки</b>",
        f"Активных кампаний: {campaigns_active} из {campaigns_total}",
        f"Всего отправлено: {sent_total}",
        f"Последняя отправка: {human_time}",
        payment_line,
    ]
    if last_error:
        lines.append("Ошибки последнего запуска:")
        lines.append(last_error)
    else:
        lines.append("Ошибок не зафиксировано.")
    _, keyboard, _ = await build_main_menu(call.from_user.id)
    await safe_edit_text(call.message, "\n".join(lines), reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data == "main:groups")
async def cb_main_groups(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    await refresh_user_delivery_chats()
    known = await storage.list_known_chats()
    auto = await storage.get_auto(call.from_user.id)
    selected = auto.get("target_chat_ids") or []
    if not known:
        delivery_subject = "пользователя рассылки" if USE_USER_DELIVERY else "бота"
        text, keyboard, _ = await build_main_menu(call.from_user.id)
        await safe_edit_text(
            call.message,
            "📋 Пока нет групп для рассылки.\n"
            f"Добавьте {delivery_subject} в рабочие чаты и убедитесь, что он может отправлять сообщения, затем повторите попытку.",
            reply_markup=keyboard,
        )
        return
    header = (
        "📋 <b>Выбор групп для рассылки</b>\n"
        "Нажмите на кнопку, чтобы добавить или убрать чат.\n"
        "Используйте «Выбрать все», чтобы отметить все чаты сразу.\n"
        f"🤖 — {'пользователь рассылки' if USE_USER_DELIVERY else 'бот'} в чате, 🚫 — отсутствует (нужно добавить его в группу)."
    )
    await safe_edit_text(
        call.message,
        header,
        reply_markup=groups_keyboard(known, selected, origin="main", page=0),
    )


@dp.callback_query_handler(lambda c: c.data == "main:settings")
async def cb_main_settings(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    await refresh_user_delivery_chats()
    auto = await storage.get_auto(call.from_user.id)
    interval = auto.get("interval_minutes")
    message_text_raw = auto.get("message") or "— не задано"
    message_text = quote_html(message_text_raw)
    status = "Активна" if auto.get("is_enabled") else "Отключена"
    targets = auto.get("target_chat_ids") or []
    known_chats = await storage.list_known_chats()
    available_total = sum(1 for info in known_chats.values() if info.get("delivery_available"))
    agent_name = "пользователя рассылки" if USE_USER_DELIVERY else "бота"
    if targets:
        missing = [
            chat_id
            for chat_id in targets
            if str(chat_id) not in known_chats or not known_chats[str(chat_id)].get("delivery_available")
        ]
        if missing:
            group_line = f"Группы: выбраны недоступные чаты — добавьте {agent_name}."
        else:
            group_line = f"Группы: {len(targets)} выбрано (доступно {available_total})"
    else:
        if available_total:
            group_line = f"Группы: не выбраны (доступно {available_total})"
        else:
            group_line = f"Группы: нет доступных чатов — добавьте {agent_name}."
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
    await safe_edit_text(call.message, text, reply_markup=keyboard)


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
    await safe_edit_text(call.message, text, reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data == "main:admin_payments")
async def cb_main_admin_payments(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    text = await build_admin_payments_text()
    _, keyboard, _ = await build_main_menu(call.from_user.id)
    await safe_edit_text(call.message, text, reply_markup=keyboard)


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
    text = message.text.strip()
    if not text:
        await message.reply("Сообщение не может быть пустым. Попробуйте снова.")
        return
    await storage.set_auto_message(message.from_user.id, text)
    await storage.ensure_constraints(message.from_user.id)
    auto_sender: AutoSender = message.bot["auto_sender"]
    await auto_sender.refresh(owner_id=message.from_user.id)
    await state.finish()
    await message.answer("Сообщение сохранено.")
    auto_data = await storage.get_auto(message.from_user.id)
    await message.answer(
        "Параметры авторассылки обновлены.",
        reply_markup=auto_menu_keyboard(
            is_enabled=auto_data.get("is_enabled"),
            allow_group_pick=True,
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
    content = message.text.strip()
    if not content.isdigit():
        await message.reply("Нужно целое число минут. Попробуйте ещё раз.")
        return
    minutes = int(content)
    if minutes <= 0:
        await message.reply("Интервал должен быть больше нуля.")
        return
    await storage.set_auto_interval(message.from_user.id, minutes)
    await storage.ensure_constraints(message.from_user.id)
    auto_sender: AutoSender = message.bot["auto_sender"]
    await auto_sender.refresh(owner_id=message.from_user.id)
    await state.finish()
    await message.answer(f"Интервал установлен: {minutes} мин.")
    auto_data = await storage.get_auto(message.from_user.id)
    await message.answer(
        "Параметры авторассылки обновлены.",
        reply_markup=auto_menu_keyboard(
            is_enabled=auto_data.get("is_enabled"),
            allow_group_pick=True,
        ),
    )


@dp.message_handler(state=AdminManualPaymentStates.waiting_for_user, content_types=types.ContentTypes.TEXT)
async def process_manual_payment_user(message: types.Message, state: FSMContext) -> None:
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
    await refresh_user_delivery_chats()
    known = await storage.list_known_chats()
    auto = await storage.get_auto(call.from_user.id)
    selected = auto.get("target_chat_ids") or []
    if not known:
        delivery_subject = "пользователя рассылки" if USE_USER_DELIVERY else "бота"
        _, keyboard, _ = await build_main_menu(call.from_user.id)
        await safe_edit_text(
            call.message,
            "📋 Пока нет групп для рассылки.\n"
            f"Добавьте {delivery_subject} в рабочие чаты и убедитесь, что он может отправлять сообщения, затем повторите попытку.",
            reply_markup=keyboard,
        )
        return
    delivery_subject = "пользователь рассылки" if USE_USER_DELIVERY else "бот"
    text = (
        "📋 <b>Выбор групп для рассылки</b>\n"
        "Нажмите на кнопки, чтобы добавить или убрать чат.\n"
        "Если ничего не выбрано, рассылка не запускается.\n"
        "Используйте «Выбрать все», чтобы отметить все чаты сразу.\n"
        f"🤖 — {delivery_subject} в чате, 🚫 — отсутствует."
    )
    await safe_edit_text(
        call.message,
        text,
        reply_markup=groups_keyboard(known, selected, origin="auto", page=0),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("group:"))
async def cb_group_toggle(call: types.CallbackQuery) -> None:
    await call.answer()
    try:
        _, origin, action_raw = call.data.split(":", maxsplit=2)
    except ValueError:
        await call.answer("Неизвестная команда", show_alert=True)
        return
    action_parts = action_raw.split("|")
    action = action_parts[0]
    page = 0
    mode: Optional[str] = None
    if len(action_parts) > 1:
        try:
            page = int(action_parts[1])
        except ValueError:
            page = 0
    if len(action_parts) > 2:
        mode = action_parts[2]
    if action == "done":
        if origin == "main":
            await send_main_menu(call.message, edit=True, user_id=call.from_user.id)
        else:
            auto_data = await storage.get_auto(call.from_user.id)
            await show_auto_menu(call.message, auto_data, user_id=call.from_user.id)
        return
    if action == "noop":
        await call.answer()
        return
    if action == "page":
        known = await storage.list_known_chats()
        auto = await storage.get_auto(call.from_user.id)
        await safe_edit_text(
            call.message,
            call.message.text or "",
            reply_markup=groups_keyboard(known, auto.get("target_chat_ids"), origin=origin, page=page),
        )
        return
    known = await storage.list_known_chats()
    update_message: str
    if action == "all":
        available_ids = [
            int(chat_id)
            for chat_id, info in known.items()
            if info.get("delivery_available")
        ]
        if not available_ids:
            await call.answer("Нет доступных чатов.", show_alert=True)
            return
        auto_data = await storage.get_auto(call.from_user.id)
        known_set = set(available_ids)
        current_targets = set(auto_data.get("target_chat_ids") or [])
        all_selected = bool(known_set) and known_set.issubset(current_targets)
        if mode == "clear":
            clear_all = True
        elif mode == "fill":
            clear_all = False
            if all_selected:
                clear_all = True
        else:
            clear_all = all_selected
        if clear_all:
            await storage.set_target_chats(call.from_user.id, [])
            update_message = "Все чаты убраны из списка рассылки."
        else:
            await storage.set_target_chats(call.from_user.id, available_ids)
            update_message = "Все доступные чаты добавлены в рассылку."
    else:
        if len(action_parts) < 3:
            await call.answer("Некорректные данные.", show_alert=True)
            return
        try:
            chat_id = int(action_parts[2])
        except ValueError:
            await call.answer("Некорректный идентификатор чата", show_alert=True)
            return
        chat_info = known.get(str(chat_id))
        if not chat_info:
            await call.answer("Чат не найден. Обновите список.", show_alert=True)
            return
        if not chat_info.get("delivery_available"):
            await refresh_user_delivery_chats()
            known = await storage.list_known_chats()
            chat_info = known.get(str(chat_id))
            if not chat_info or not chat_info.get("delivery_available"):
                missing_subject = "Пользователь рассылки" if USE_USER_DELIVERY else "Бот"
                await call.answer(
                    f"{missing_subject} не добавлен в этот чат. Добавьте его и попробуйте снова.",
                    show_alert=True,
                )
                return
        title_raw = chat_info.get("title") or str(chat_id)
        title = quote_html(title_raw)
        selected = await storage.toggle_target_chat(call.from_user.id, chat_id, title_raw)
        update_message = f"Чат {'добавлен в' if selected else 'убран из'} рассылки: {title}"
    await storage.ensure_constraints(call.from_user.id)
    auto_sender_instance: AutoSender = call.bot["auto_sender"]
    await auto_sender_instance.refresh(owner_id=call.from_user.id)
    known = await storage.list_known_chats()
    auto = await storage.get_auto(call.from_user.id)
    reply_text = (
        "📋 <b>Выбор групп для рассылки</b>\n\n"
        f"{update_message}\n"
        "При необходимости выберите другие чаты или нажмите 'Готово'."
    )
    await safe_edit_text(
        call.message,
        reply_text,
        reply_markup=groups_keyboard(known, auto.get("target_chat_ids"), origin=origin, page=page),
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
    await safe_edit_text(call.message, "Перепроверка завершена:\n\n" + admin_text)
    auto_sender: Optional[AutoSender] = call.bot.get("auto_sender")
    if auto_sender and user_id:
        await auto_sender.refresh(owner_id=user_id)
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
    await safe_edit_text(call.message, admin_text)
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
    await refresh_user_delivery_chats()
    auto = await storage.get_auto(call.from_user.id)
    if not auto.get("message"):
        await call.message.answer("Сначала задайте текст сообщения.")
        return
    selected_targets = auto.get("target_chat_ids") or []
    if not selected_targets:
        await call.message.answer("Не выбрано ни одной группы для рассылки.")
        return
    delivery_ready = await storage.list_delivery_ready_chat_ids()
    missing_targets = [chat_id for chat_id in selected_targets if chat_id not in delivery_ready]
    if missing_targets:
        known = await storage.list_known_chats()
        titles = [
            (known.get(str(chat_id)) or {}).get("title") or str(chat_id)
            for chat_id in missing_targets
        ]
        agent_name = "пользователь рассылки" if USE_USER_DELIVERY else "бот"
        await call.message.answer(
            f"{agent_name.capitalize()} не добавлен в следующие группы:\n"
            + "\n".join(f"• {title}" for title in titles)
            + "\n\nДобавьте его и попробуйте снова."
        )
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
    auto_sender: AutoSender = call.bot["auto_sender"]
    await auto_sender.ensure_running(call.from_user.id)
    await call.message.answer("Авторассылка запущена.")
    updated = await storage.get_auto(call.from_user.id)
    await show_auto_menu(call.message, updated, user_id=call.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "auto:stop")
async def cb_auto_stop(call: types.CallbackQuery) -> None:
    await call.answer()
    await storage.set_auto_enabled(call.from_user.id, False)
    auto_sender: AutoSender = call.bot["auto_sender"]
    await auto_sender.stop(owner_id=call.from_user.id)
    await call.message.answer("Авторассылка остановлена.")
    updated = await storage.get_auto(call.from_user.id)
    await show_auto_menu(call.message, updated, user_id=call.from_user.id)


@dp.message_handler(
    lambda message: message.chat.type == types.ChatType.PRIVATE and not (message.text or "").startswith("/"),
    content_types=types.ContentTypes.ANY,
    state="*",
)
async def handle_private_message_without_command(message: types.Message, state: FSMContext) -> None:
    if await state.get_state():
        return
    if not await is_admin_user(message.from_user.id):
        notified = await notify_admins_about_incoming_message(message)
        if notified:
            await message.answer("💬 Сообщение отправлено администраторам. Ответ придёт здесь.")
        else:
            await message.answer(
                "Сообщение получено. Как только администратор будет доступен, он ответит в этом чате."
            )
    else:
        await message.answer("Сообщение получено.")
    await send_main_menu(message)


@dp.callback_query_handler(lambda c: c.data.startswith("inbox:reply:"), state="*")
async def cb_inbox_reply(call: types.CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Недостаточно прав.", show_alert=True)
        return
    try:
        _, _, user_id_raw = call.data.split(":", maxsplit=2)
        target_user_id = int(user_id_raw)
    except (ValueError, TypeError):
        await call.answer("Некорректные данные.", show_alert=True)
        return
    await call.answer()
    await state.finish()
    await AdminInboxStates.waiting_for_reply.set()
    await state.update_data(reply_target=target_user_id)
    await call.message.answer(
        f"Введите ответ для пользователя <code>{target_user_id}</code>.\n"
        "Используйте /cancel для отмены."
    )


@dp.message_handler(state=AdminInboxStates.waiting_for_reply, content_types=types.ContentTypes.ANY)
async def handle_admin_reply(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    target_user_id = data.get("reply_target")
    if not target_user_id:
        await message.answer("Не удалось определить пользователя для ответа. Попробуйте снова.")
        await state.finish()
        return
    try:
        if message.content_type == types.ContentType.TEXT:
            text = (message.text or "").strip()
            if not text:
                await message.answer("Введите текст ответа или используйте /cancel.")
                return
            await bot.send_message(
                target_user_id,
                "💬 Сообщение от администратора:\n" + text,
            )
        else:
            await bot.send_message(target_user_id, "💬 Администратор отправил вам сообщение:")
            await bot.copy_message(target_user_id, message.chat.id, message.message_id)
        await message.answer("Ответ отправлен пользователю.")
    except exceptions.TelegramAPIError as exc:
        logger.error("Не удалось отправить сообщение пользователю %s: %s", target_user_id, exc)
        await message.answer("Не удалось отправить сообщение пользователю. Попробуйте позже.")
    finally:
        await state.finish()


async def ensure_known_group_chat(chat: types.Chat) -> None:
    if chat.type not in GROUP_CHAT_TYPES:
        return
    title = chat.title or chat.full_name or str(chat.id)
    delivery_ready = None if USE_USER_DELIVERY else True
    await storage.upsert_known_chat(
        chat.id,
        title,
        delivery_available=True if delivery_ready else None,
    )


async def apply_chat_membership_update(
    chat: types.Chat,
    status: str,
) -> None:
    if chat.type not in GROUP_CHAT_TYPES:
        return
    if status in (
        types.ChatMemberStatus.ADMINISTRATOR,
        types.ChatMemberStatus.CREATOR,
        types.ChatMemberStatus.MEMBER,
    ):
        await ensure_known_group_chat(chat)
        logger.info("Добавлен чат %s (%s)", chat.id, chat.title or chat.full_name or str(chat.id))
    elif status in (
        types.ChatMemberStatus.LEFT,
        types.ChatMemberStatus.KICKED,
        types.ChatMemberStatus.RESTRICTED,
    ):
        if USE_USER_DELIVERY:
            await storage.set_delivery_available(chat.id, False)
            logger.info("Основной бот покинул чат %s. Он останется в списке, если пользователь клиент присутствует.", chat.id)
        else:
            await storage.remove_known_chat(chat.id)
            logger.info("Удалён чат %s", chat.id)


@dp.my_chat_member_handler()
async def handle_my_chat_member(update: types.ChatMemberUpdated) -> None:
    await apply_chat_membership_update(update.chat, update.new_chat_member.status)


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
        await ensure_known_group_chat(chat)


async def on_startup(dispatcher: Dispatcher) -> None:
    global USE_USER_DELIVERY

    me = await dispatcher.bot.get_me()
    mtproto_delivery: Optional[UserDelivery] = dispatcher.bot.get("user_delivery")

    async def send_via_bot(chat_id: int, text: str) -> None:
        await dispatcher.bot.send_message(chat_id, text)

    send_callable: Callable[[int, str], Awaitable[None]] = send_via_bot
    if mtproto_delivery:
        try:
            await mtproto_delivery.start()
            await mtproto_delivery.sync_known_chats(storage)
            send_callable = mtproto_delivery.send_text
        except Exception:
            logger.exception("Пользовательская Telegram-сессия недоступна. Рассылка будет выполняться ботом.")
            await mtproto_delivery.stop()
            dispatcher.bot["user_delivery"] = None
            mtproto_delivery = None
            USE_USER_DELIVERY = False
    auto_sender = AutoSender(
        send_callable,
        storage,
        PAYMENT_VALID_DAYS,
    )
    dispatcher.bot["auto_sender"] = auto_sender
    dispatcher.bot["bot_id"] = me.id
    await storage.ensure_constraints()
    if not mtproto_delivery:
        await storage.mark_all_chats_delivery_available()
    await auto_sender.start_if_enabled()
    logger.info("Бот %s (%s) запущен", me.first_name, me.id)


async def on_shutdown(dispatcher: Dispatcher) -> None:
    auto_sender: Optional[AutoSender] = dispatcher.bot.get("auto_sender")
    if auto_sender:
        await auto_sender.stop()
    mtproto_delivery: Optional[UserDelivery] = dispatcher.bot.get("user_delivery")
    if mtproto_delivery:
        await mtproto_delivery.stop()
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
