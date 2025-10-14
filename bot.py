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
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from dotenv import load_dotenv

from app.auto_sender import AutoSender
from app.keyboards import auto_menu_keyboard, groups_keyboard, main_menu_keyboard, payme_keyboard
from app.states import AutoCampaignStates, PaymentStates
from app.storage import Storage


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Missng BOT_TOKEN")

storage_path_env = os.getenv("STORAGE_PATH")
if storage_path_env:
    storage_path = Path(storage_path_env)
    if not storage_path.is_absolute():
        storage_path = (BASE_DIR / storage_path).resolve()
else:
    storage_path = BASE_DIR / "data" / "storage.json"

storage = Storage(storage_path)
bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())

bot["storage"] = storage
bot["auto_sender"] = None  # filled on startup

WELCOME_TEXT = (
    "👋 Добро пожаловать обратно!\n\n"
    "⚒ Авторассылка — настройка сообщений и расписания\n"
    "💰 Пополнить баланс — передача данных оператору\n"
    "📊 Статистика — просмотр результатов рассылки\n"
    "📋 Выбрать группы — управление чатами\n"
    "⚙️ Настройки — текущие параметры"
)

PAYMENT_AMOUNT = 100_000
PAYMENT_CURRENCY = "UZS"
PAYMENT_DESCRIPTION = "Оплата услуг логистического бота"
PAYMENT_VALID_DAYS = 30

STATIC_ADMIN_IDS: Set[int] = {
    int(admin_id.strip())
    for admin_id in os.getenv("ADMIN_IDS", "").split(",")
    if admin_id.strip().isdigit()
}


async def get_user_role(user_id: int) -> str:
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


async def show_main_menu(message: types.Message) -> None:
    try:
        await message.edit_text(WELCOME_TEXT, reply_markup=main_menu_keyboard())
    except exceptions.MessageNotModified:
        pass


async def show_auto_menu(message: types.Message, auto_data: dict) -> None:
    status = "Активна ✅" if auto_data.get("is_enabled") else "Не запущена"
    message_preview_raw = auto_data.get("message") or "— не задано"
    if len(message_preview_raw) > 180:
        message_preview_raw = message_preview_raw[:177] + "..."
    message_preview = quote_html(message_preview_raw)
    if len(message_preview) > 180:
        message_preview = message_preview[:177] + "..."
    interval = auto_data.get("interval_minutes") or 0
    targets = auto_data.get("target_chat_ids") or []
    payment_valid = await storage.has_recent_payment(within_days=PAYMENT_VALID_DAYS)
    latest_payment = await storage.latest_payment_timestamp()
    if payment_valid and latest_payment:
        expires_dt = latest_payment + timedelta(days=PAYMENT_VALID_DAYS)
        payment_line = f"Оплата: действительна до {expires_dt.strftime('%d.%m.%Y')} ✅"
    else:
        payment_line = f"Оплата: требуется пополнение (каждые {PAYMENT_VALID_DAYS} дней)"
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
    await message.answer(WELCOME_TEXT, reply_markup=main_menu_keyboard())


@dp.message_handler(commands=["cancel"], state="*")
async def cmd_cancel(message: types.Message, state: FSMContext) -> None:
    if await state.get_state() is None:
        return
    await state.finish()
    await message.answer("Действие отменено. Возвращаемся в меню.")
    await message.answer(WELCOME_TEXT, reply_markup=main_menu_keyboard())


@dp.message_handler(commands=["админ", "admin"], state="*")
async def cmd_set_admin(message: types.Message, state: FSMContext) -> None:
    await state.finish()
    user = message.from_user
    if await is_admin_user(user.id):
        await message.answer("Вы уже назначены администратором и будете получать уведомления.")
        return
    await storage.set_user_role(user.id, "admin")
    await message.answer(
        "Статус администратора активирован. Вы будете получать заявки на оплату и сможете подтверждать их."
    )


@dp.callback_query_handler(lambda c: c.data == "main:auto")
async def cb_main_auto(call: types.CallbackQuery) -> None:
    await call.answer()
    auto_data = await storage.get_auto()
    await show_auto_menu(call.message, auto_data)


@dp.callback_query_handler(lambda c: c.data == "main:stats")
async def cb_main_stats(call: types.CallbackQuery) -> None:
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
    await call.message.edit_text("\n".join(lines), reply_markup=main_menu_keyboard())


@dp.callback_query_handler(lambda c: c.data == "main:groups")
async def cb_main_groups(call: types.CallbackQuery) -> None:
    await call.answer()
    known = await storage.list_known_chats()
    auto = await storage.get_auto()
    selected = auto.get("target_chat_ids") or []
    if not known:
        await call.message.edit_text(
            "📋 Пока нет групп для рассылки.\n"
            "Добавьте бота в нужный чат и назначьте администратором, затем повторите попытку.",
            reply_markup=main_menu_keyboard(),
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
    await call.message.edit_text(text, reply_markup=main_menu_keyboard())


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


@dp.callback_query_handler(lambda c: c.data == "auto:back")
async def cb_auto_back(call: types.CallbackQuery) -> None:
    await call.answer()
    await show_main_menu(call.message)


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
    user_role = await get_user_role(user.id)
    request_id = await storage.create_payment_request(
        user_id=user.id,
        username=user.username,
        full_name=user.full_name or user.username or str(user.id),
        card_number=card_number,
        card_name=card_name,
    )
    payment = await storage.get_payment(request_id)
    if payment:
        admin_text = build_payment_admin_text(payment)
        admin_ids = await collect_admin_ids()
        for admin_id in admin_ids:
            if admin_id == user.id and user_role != "admin":
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
    await message.answer(
        "Спасибо! Данные отправлены администратору. \n"
        f"После подтверждения оплата будет действовать {PAYMENT_VALID_DAYS} дней."
    )
    await state.finish()


@dp.callback_query_handler(lambda c: c.data == "auto:pick_groups")
async def cb_auto_pick_groups(call: types.CallbackQuery) -> None:
    await call.answer()
    known = await storage.list_known_chats()
    auto = await storage.get_auto()
    selected = auto.get("target_chat_ids") or []
    if not known:
        await call.message.answer(
            "Список групп пуст. Добавьте бота в нужный чат и назначьте администратором,"
            " затем повторите попытку."
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
            await call.message.edit_text(WELCOME_TEXT, reply_markup=main_menu_keyboard())
        else:
            auto_data = await storage.get_auto()
            await show_auto_menu(call.message, auto_data)
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
    if status == "approved":
        expires_text = ""
        resolved_at = updated.get("resolved_at")
        if resolved_at:
            try:
                resolved_dt = datetime.fromisoformat(resolved_at)
                expires_dt = resolved_dt + timedelta(days=PAYMENT_VALID_DAYS)
                expires_text = f" Оплата активна до {expires_dt.strftime('%d.%m.%Y')} включительно."
            except ValueError:
                expires_text = ""
        status_message = "✅ Администратор подтвердил оплату. Спасибо!" + expires_text
    else:
        status_message = "❌ Администратор отклонил оплату. Свяжитесь с поддержкой."
    user_id = updated.get("user_id")
    try:
        await bot.send_message(user_id, status_message)
    except exceptions.TelegramAPIError as exc:
        logger.error("Не удалось отправить уведомление пользователю %s: %s", user_id, exc)
    admin_text = build_payment_admin_text(updated)
    await call.message.edit_text(admin_text)
    await call.answer("Решение сохранено.")


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
    await show_auto_menu(call.message, updated)


@dp.callback_query_handler(lambda c: c.data == "auto:stop")
async def cb_auto_stop(call: types.CallbackQuery) -> None:
    await call.answer()
    await storage.set_auto_enabled(False)
    auto_sender: AutoSender = call.bot["auto_sender"]
    await auto_sender.stop()
    await call.message.answer("Авторассылка остановлена.")
    updated = await storage.get_auto()
    await show_auto_menu(call.message, updated)


@dp.my_chat_member_handler()
async def handle_my_chat_member(update: types.ChatMemberUpdated) -> None:
    new_status = update.new_chat_member.status
    chat = update.chat
    if chat.type not in (types.ChatType.GROUP, types.ChatType.SUPERGROUP):
        return
    title = chat.title or chat.full_name or str(chat.id)
    if new_status == types.ChatMemberStatus.ADMINISTRATOR:
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
    if member.is_chat_admin():
        await storage.upsert_known_chat(chat.id, title_raw)


async def on_startup(dispatcher: Dispatcher) -> None:
    me = await dispatcher.bot.get_me()
    auto_sender = AutoSender(dispatcher.bot, storage, me.id, PAYMENT_VALID_DAYS)
    dispatcher.bot["auto_sender"] = auto_sender
    dispatcher.bot["bot_id"] = me.id
    await storage.ensure_constraints()
    await auto_sender.start_if_enabled()
    logger.info("Бот %s (%s) запущен", me.first_name, me.id)


async def on_shutdown(dispatcher: Dispatcher) -> None:
    auto_sender: Optional[AutoSender] = dispatcher.bot.get("auto_sender")
    if auto_sender:
        await auto_sender.stop()
    await dispatcher.storage.close()
    await dispatcher.storage.wait_closed()


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=False, on_startup=on_startup, on_shutdown=on_shutdown)
