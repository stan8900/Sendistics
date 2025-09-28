import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.utils import exceptions, executor
from aiogram.utils.markdown import bold, hbold, quote_html
from dotenv import load_dotenv

from app.auto_sender import AutoSender
from app.keyboards import auto_menu_keyboard, groups_keyboard, main_menu_keyboard
from app.states import AutoCampaignStates
from app.storage import Storage


load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Missng BOT_TOKEN")

storage = Storage(Path("data/storage.json"))
bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())

bot["storage"] = storage
bot["auto_sender"] = None  # filled on startup

WELCOME_TEXT = (
    "👋 Добро пожаловать обратно!\n\n"
    "🛠 Авторассылка — управление отправкой сообщений\n"
    "🔍 Поиск групп — добавление новых чатов\n"
    "📊 Статистика — аналитика работы\n"
    "⚙️ Настройки — конфигурация бота"
)


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
    text = (
        f"🛠 {hbold('Авторассылка')}\n\n"
        f"Статус: {status}\n"
        f"Интервал: {interval} мин\n"
        f"Выбрано групп: {len(targets)}\n\n"
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


@dp.callback_query_handler(lambda c: c.data == "main:auto")
async def cb_main_auto(call: types.CallbackQuery) -> None:
    await call.answer()
    auto_data = await storage.get_auto()
    await show_auto_menu(call.message, auto_data)


@dp.callback_query_handler(lambda c: c.data == "main:search")
async def cb_main_search(call: types.CallbackQuery) -> None:
    await call.answer()
    known = await storage.list_known_chats()
    lines = [
        "🔍 <b>Поиск и добавление групп</b>",
        "1. Добавьте бота в нужный чат и назначьте его администратором.",
        "2. Как только бот получит права администратора, группа появится в списке.",
        "3. Вернитесь в раздел 'Авторассылка' и выберите чаты для рассылки.",
    ]
    if known:
        lines.append("\nТекущие группы:")
        for info in known.values():
            title = quote_html(info.get("title") or str(info.get("chat_id")))
            lines.append(f"• {title}")
    else:
        lines.append("\nПока нет ни одной зарегистрированной группы.")
    await call.message.edit_text("\n".join(lines), reply_markup=main_menu_keyboard())


@dp.callback_query_handler(lambda c: c.data == "main:stats")
async def cb_main_stats(call: types.CallbackQuery) -> None:
    await call.answer()
    auto = await storage.get_auto()
    stats = auto.get("stats") or {}
    sent_total = stats.get("sent_total", 0)
    last_sent_at = stats.get("last_sent_at")
    last_error = stats.get("last_error")
    human_time = "—"
    if last_sent_at:
        try:
            dt = datetime.fromisoformat(last_sent_at)
            human_time = dt.strftime("%d.%m.%Y %H:%M:%S")
        except ValueError:
            human_time = last_sent_at
    lines = [
        "📊 <b>Статистика авторассылки</b>",
        f"Всего отправлено: {sent_total}",
        f"Последняя отправка: {human_time}",
    ]
    if last_error:
        lines.append("Ошибки последнего запуска:")
        lines.append(last_error)
    else:
        lines.append("Ошибок не зафиксировано.")
    await call.message.edit_text("\n".join(lines), reply_markup=main_menu_keyboard())


@dp.callback_query_handler(lambda c: c.data == "main:settings")
async def cb_main_settings(call: types.CallbackQuery) -> None:
    await call.answer()
    auto = await storage.get_auto()
    interval = auto.get("interval_minutes")
    message_text_raw = auto.get("message") or "— не задано"
    message_text = quote_html(message_text_raw)
    targets = auto.get("target_chat_ids") or []
    status = "Активна" if auto.get("is_enabled") else "Отключена"
    text = (
        "⚙️ <b>Настройки</b>\n"
        f"Статус: {status}\n"
        f"Интервал: {interval} мин\n"
        f"Группы: {len(targets)} выбрано\n\n"
        f"Сообщение:\n{message_text}"
    )
    await call.message.edit_text(text, reply_markup=main_menu_keyboard())


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
        "Выберите группы для авторассылки.\n"
        "Нажмите на кнопки, чтобы добавить или убрать чат."
    )
    await call.message.edit_text(text, reply_markup=groups_keyboard(known, selected))


@dp.callback_query_handler(lambda c: c.data.startswith("group:"))
async def cb_group_toggle(call: types.CallbackQuery) -> None:
    await call.answer()
    action = call.data.split(":", maxsplit=1)[1]
    if action == "done":
        auto_data = await storage.get_auto()
        await show_auto_menu(call.message, auto_data)
        return
    chat_id = int(action)
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
        f"Чат {'добавлен в' if selected else 'убран из'} рассылки: {title}\n"
        "При необходимости выберите другие чаты или нажмите 'Готово'."
    )
    await call.message.edit_text(reply_text, reply_markup=groups_keyboard(known, auto.get("target_chat_ids")))


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
    auto_sender = AutoSender(dispatcher.bot, storage, me.id)
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
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup, on_shutdown=on_shutdown)
