import asyncio
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Set

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.utils import exceptions, executor
from aiogram.utils.markdown import hbold, quote_html
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from dotenv import load_dotenv

from app.auto_sender import AutoSender
from app.keyboards import auto_menu_keyboard, groups_keyboard, main_menu_keyboard
from app.pdf_reports import build_payments_pdf
from app.states import AutoCampaignStates, PaymentStates, AdminLoginStates, AdminManualPaymentStates
from app.storage import Storage
from app.user_sender import UserSender
from app.user_dialogs import UserDialogResponder


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Missng BOT_TOKEN")

storage_path_env = os.getenv("STORAGE_PATH")
legacy_storage_path: Optional[Path] = None
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

storage = Storage(storage_path, legacy_json_path=legacy_storage_path)

tg_user_api_id_raw = os.getenv("TG_USER_API_ID")
tg_user_api_hash = os.getenv("TG_USER_API_HASH")
tg_user_session = os.getenv("TG_USER_SESSION")
user_sender: Optional[UserSender]
if tg_user_api_id_raw and tg_user_api_hash and tg_user_session:
    try:
        tg_user_api_id = int(tg_user_api_id_raw)
    except ValueError:
        logger.warning("TG_USER_API_ID должен быть числом. Пользовательская рассылка отключена.")
        user_sender = None
    else:
        user_sender = UserSender(tg_user_api_id, tg_user_api_hash, tg_user_session)
else:
    user_sender = None

bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())

bot["storage"] = storage
bot["auto_sender"] = None  # filled on startup
bot["user_sender"] = user_sender
bot["user_dialog_responder"] = None

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
PAYMENT_DIALOG_CANCEL_MESSAGE = "Действие отменено. Чтобы начать заново, просто напишите любое сообщение."
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


def build_user_session_welcome_text() -> str:
    return (
        f"{WELCOME_TEXT_USER}\n"
        f"Для пополнения баланса: {PAYMENT_DESCRIPTION}.\n"
        f"Сумма к оплате: {format_currency(PAYMENT_AMOUNT, PAYMENT_CURRENCY)}.\n\n"
        f"После подтверждения оплата действует {PAYMENT_VALID_DAYS} дней.\n\n"
        f"Переведите сумму на карту {PAYMENT_CARD_TARGET} и введите номер своей карты ниже.\n\n"
        f"{PAYMENT_CARD_PROMPT}"
    )


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
    text = WELCOME_TEXT_ADMIN if is_admin else WELCOME_TEXT_USER
    return text, main_menu_keyboard(is_admin), is_admin


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


async def show_auto_menu(message: types.Message, auto_data: dict, *, user_id: Optional[int] = None) -> None:
    status = "Активна ✅" if auto_data.get("is_enabled") else "Не запущена"
    message_preview_raw = auto_data.get("message") or "— не задано"
    if len(message_preview_raw) > 180:
        message_preview_raw = message_preview_raw[:177] + "..."
    message_preview = quote_html(message_preview_raw)
    if len(message_preview) > 180:
        message_preview = message_preview[:177] + "..."
    interval = auto_data.get("interval_minutes") or 0
    targets = auto_data.get("target_chat_ids") or []
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
    text = (
        f"🛠 {hbold('Авторассылка')}\n\n"
        f"Статус: {status}\n"
        f"Интервал: {interval} мин\n"
        f"Выбрано групп: {len(targets)}\n\n"
        f"{payment_line}\n\n"
        f"Сообщение:\n{message_preview}"
    )
    try:
        await message.edit_text(text, reply_markup=auto_menu_keyboard(is_enabled=auto_data.get("is_enabled")))
    except exceptions.MessageNotModified:
        pass


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
    auto_data = await storage.get_auto()
    await show_auto_menu(call.message, auto_data, user_id=call.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "main:stats")
async def cb_main_stats(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    auto = await storage.get_auto()
    stats = auto.get("stats") or {}
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
    await call.message.edit_text("\n".join(lines), reply_markup=keyboard)


@dp.callback_query_handler(lambda c: c.data == "main:groups")
async def cb_main_groups(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    known = await storage.list_known_chats()
    auto = await storage.get_auto()
    selected = auto.get("target_chat_ids") or []
    if not known:
        text, keyboard, _ = await build_main_menu(call.from_user.id)
        await call.message.edit_text(
            "📋 Пока нет групп для рассылки.\n"
            "Добавьте бота в нужный чат и убедитесь, что он может отправлять сообщения, затем повторите попытку.",
            reply_markup=keyboard,
        )
        return
    header = (
        "📋 <b>Выбор групп для рассылки</b>\n"
        "Нажмите на кнопку, чтобы добавить или убрать чат."
    )
    await call.message.edit_text(
        header,
        reply_markup=groups_keyboard(known, selected, origin="main"),
    )


@dp.callback_query_handler(lambda c: c.data == "main:settings")
async def cb_main_settings(call: types.CallbackQuery) -> None:
    if not await is_admin_user(call.from_user.id):
        await call.answer("Доступно только администраторам.", show_alert=True)
        return
    await call.answer()
    auto = await storage.get_auto()
    interval = auto.get("interval_minutes")
    message_text_raw = auto.get("message") or "— не задано"
    message_text = quote_html(message_text_raw)
    targets = auto.get("target_chat_ids") or []
    status = "Активна" if auto.get("is_enabled") else "Отключена"
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
        f"Группы: {len(targets)} выбрано\n"
        f"{payment_line}\n\n"
        f"Сообщение:\n{message_text}"
    )
    _, keyboard, _ = await build_main_menu(call.from_user.id)
    await call.message.edit_text(text, reply_markup=keyboard)


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
        "Переведите сумму на карту <code>9860 1701 1433 3116</code> и введите номер своей карты ниже.\n\n"
        "Введите номер карты (12–19 цифр).\n"
        "Для отмены используйте /cancel.",
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
    text = message.text.strip()
    if not text:
        await message.reply("Сообщение не может быть пустым. Попробуйте снова.")
        return
    await storage.set_auto_message(text)
    await storage.ensure_constraints()
    auto_sender: AutoSender = message.bot["auto_sender"]
    await auto_sender.refresh()
    await state.finish()
    await message.answer("Сообщение сохранено.")
    auto_data = await storage.get_auto()
    await message.answer(
        "Параметры авторассылки обновлены.",
        reply_markup=auto_menu_keyboard(is_enabled=auto_data.get("is_enabled")),
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
    await storage.set_auto_interval(minutes)
    await storage.ensure_constraints()
    auto_sender: AutoSender = message.bot["auto_sender"]
    await auto_sender.refresh()
    await state.finish()
    await message.answer(f"Интервал установлен: {minutes} мин.")
    auto_data = await storage.get_auto()
    await message.answer(
        "Параметры авторассылки обновлены.",
        reply_markup=auto_menu_keyboard(is_enabled=auto_data.get("is_enabled")),
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
        await message.reply("Введите корректный номер карты (12–19 цифр).")
        return
    formatted = " ".join(digits[i : i + 4] for i in range(0, len(digits), 4))
    await state.update_data(card_number=formatted)
    await PaymentStates.waiting_for_card_name.set()
    await message.answer(
        "Укажите имя, как на карте.\n"
        "Для отмены используйте /cancel."
    )


@dp.message_handler(state=PaymentStates.waiting_for_card_name, content_types=types.ContentTypes.TEXT)
async def process_payment_card_name(message: types.Message, state: FSMContext) -> None:
    card_name = (message.text or "").strip()
    if len(card_name) < 3:
        await message.reply("Имя должно содержать минимум 3 символа.")
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
    known = await storage.list_known_chats()
    auto = await storage.get_auto()
    selected = auto.get("target_chat_ids") or []
    if not known:
        _, keyboard, _ = await build_main_menu(call.from_user.id)
        await call.message.edit_text(
            "📋 Пока нет групп для рассылки.\n"
            "Добавьте бота в нужный чат и убедитесь, что он может отправлять сообщения, затем повторите попытку.",
            reply_markup=keyboard,
        )
        return
    text = (
        "📋 <b>Выбор групп для рассылки</b>\n"
        "Нажмите на кнопки, чтобы добавить или убрать чат."
    )
    await call.message.edit_text(
        text,
        reply_markup=groups_keyboard(known, selected, origin="auto"),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("group:"))
async def cb_group_toggle(call: types.CallbackQuery) -> None:
    await call.answer()
    try:
        _, origin, action = call.data.split(":", maxsplit=2)
    except ValueError:
        await call.answer("Неизвестная команда", show_alert=True)
        return
    if action == "done":
        if origin == "main":
            await send_main_menu(call.message, edit=True, user_id=call.from_user.id)
        else:
            auto_data = await storage.get_auto()
            await show_auto_menu(call.message, auto_data, user_id=call.from_user.id)
        return
    try:
        chat_id = int(action)
    except ValueError:
        await call.answer("Некорректный идентификатор чата", show_alert=True)
        return
    known = await storage.list_known_chats()
    title_raw = (known.get(str(chat_id)) or {}).get("title") or str(chat_id)
    title = quote_html(title_raw)
    selected = await storage.toggle_target_chat(chat_id, title_raw)
    await storage.ensure_constraints()
    auto_sender: AutoSender = call.bot["auto_sender"]
    await auto_sender.refresh()
    known = await storage.list_known_chats()
    auto = await storage.get_auto()
    reply_text = (
        "📋 <b>Выбор групп для рассылки</b>\n\n"
        f"Чат {'добавлен в' if selected else 'убран из'} рассылки: {title}\n"
        "При необходимости выберите другие чаты или нажмите 'Готово'."
    )
    await call.message.edit_text(
        reply_text,
        reply_markup=groups_keyboard(known, auto.get("target_chat_ids"), origin=origin),
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
    try:
        await bot.send_message(user_id, status_message)
    except exceptions.TelegramAPIError as exc:
        logger.error("Не удалось отправить уведомление пользователю %s: %s", user_id, exc)
    admin_text = build_payment_admin_text(updated)
    await call.message.edit_text("Перепроверка завершена:\n\n" + admin_text)
    auto_sender: Optional[AutoSender] = call.bot.get("auto_sender")
    if auto_sender:
        await auto_sender.refresh()
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
    try:
        await bot.send_message(user_id, status_message)
    except exceptions.TelegramAPIError as exc:
        logger.error("Не удалось отправить уведомление пользователю %s: %s", user_id, exc)
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
    auto = await storage.get_auto()
    if not auto.get("message"):
        await call.message.answer("Сначала задайте текст сообщения.")
        return
    if not auto.get("target_chat_ids"):
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
    await storage.set_auto_enabled(True)
    auto_sender: AutoSender = call.bot["auto_sender"]
    await auto_sender.ensure_running()
    await call.message.answer("Авторассылка запущена.")
    updated = await storage.get_auto()
    await show_auto_menu(call.message, updated, user_id=call.from_user.id)


@dp.callback_query_handler(lambda c: c.data == "auto:stop")
async def cb_auto_stop(call: types.CallbackQuery) -> None:
    await call.answer()
    await storage.set_auto_enabled(False)
    auto_sender: AutoSender = call.bot["auto_sender"]
    await auto_sender.stop()
    await call.message.answer("Авторассылка остановлена.")
    updated = await storage.get_auto()
    await show_auto_menu(call.message, updated, user_id=call.from_user.id)


@dp.message_handler(
    lambda message: message.chat.type == types.ChatType.PRIVATE and not (message.text or "").startswith("/"),
    content_types=types.ContentTypes.ANY,
    state="*",
)
async def handle_private_message_without_command(message: types.Message, state: FSMContext) -> None:
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
    user_sender_instance: Optional[UserSender] = dispatcher.bot.get("user_sender")
    user_dialog_instance: Optional[UserDialogResponder] = None
    if user_sender_instance:
        try:
            await user_sender_instance.start()
            identity = await user_sender_instance.describe_self()
            logger.info("Пользовательская рассылка активирована от %s", identity)
            user_dialog_instance = UserDialogResponder(
                user_sender_instance,
                storage,
                welcome_message=build_user_session_welcome_text(),
                card_prompt_message=PAYMENT_CARD_PROMPT,
                card_name_prompt=PAYMENT_CARD_NAME_PROMPT,
                thank_you_message=PAYMENT_THANK_YOU_MESSAGE,
                invalid_card_message=PAYMENT_CARD_INVALID_MESSAGE,
                invalid_name_message=PAYMENT_CARD_NAME_INVALID_MESSAGE,
                cancel_message=PAYMENT_DIALOG_CANCEL_MESSAGE,
                payment_created_callback=notify_admins_about_payment,
            )
            await user_dialog_instance.start()
            logger.info("Автоответы личного аккаунта включены.")
        except Exception:
            logger.exception(
                "Не удалось подключить пользовательскую сессию. Будем отправлять сообщения от имени бота."
            )
            user_sender_instance = None
            dispatcher.bot["user_sender"] = None
            user_dialog_instance = None
    dispatcher.bot["user_dialog_responder"] = user_dialog_instance
    auto_sender = AutoSender(
        dispatcher.bot,
        storage,
        PAYMENT_VALID_DAYS,
        user_sender=user_sender_instance,
    )
    dispatcher.bot["auto_sender"] = auto_sender
    dispatcher.bot["bot_id"] = me.id
    await storage.ensure_constraints()
    await auto_sender.start_if_enabled()
    logger.info("Бот %s (%s) запущен", me.first_name, me.id)


async def on_shutdown(dispatcher: Dispatcher) -> None:
    auto_sender: Optional[AutoSender] = dispatcher.bot.get("auto_sender")
    if auto_sender:
        await auto_sender.stop()
    user_dialog_instance: Optional[UserDialogResponder] = dispatcher.bot.get("user_dialog_responder")
    if user_dialog_instance:
        await user_dialog_instance.stop()
    user_sender_instance: Optional[UserSender] = dispatcher.bot.get("user_sender")
    if user_sender_instance:
        await user_sender_instance.stop()
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
