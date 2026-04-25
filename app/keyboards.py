from typing import Dict, Iterable, List

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup


def main_menu_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("👤 Мой аккаунт", callback_data="main:account")],
        [InlineKeyboardButton("🛠 Авторассылка", callback_data="main:auto")],
        [InlineKeyboardButton("🔍 Поиск групп", callback_data="main:search")],
        [InlineKeyboardButton("📊 Статистика", callback_data="main:stats")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="main:settings")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def auto_menu_keyboard(*, is_enabled: bool) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("✏️ Сообщение", callback_data="auto:set_message"),
         InlineKeyboardButton("⏱ Интервал", callback_data="auto:set_interval")],
        [InlineKeyboardButton("👥 Группы", callback_data="auto:pick_groups")],
    ]
    toggle_label = "⏸ Остановить" if is_enabled else "▶️ Запустить"
    toggle_action = "auto:stop" if is_enabled else "auto:start"
    keyboard.append([InlineKeyboardButton(toggle_label, callback_data=toggle_action)])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="auto:back")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def groups_keyboard(known_chats: Dict[str, Dict[str, str]], selected_ids: Iterable[int]) -> InlineKeyboardMarkup:
    selected_set = set(selected_ids)
    rows: List[List[InlineKeyboardButton]] = []
    for chat_key, chat_info in sorted(known_chats.items(), key=lambda item: item[1].get("title", "")):
        chat_id = int(chat_key)
        title = chat_info.get("title") or f"Чат {chat_id}"
        prefix = "✅" if chat_id in selected_set else "➕"
        rows.append([InlineKeyboardButton(f"{prefix} {title[:48]}", callback_data=f"group:{chat_id}")])
    rows.append([InlineKeyboardButton("⬅️ Готово", callback_data="group:done")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def auth_menu_keyboard(*, is_authenticated: bool) -> InlineKeyboardMarkup:
    if is_authenticated:
        keyboard = [
            [InlineKeyboardButton("💳 Платежи", callback_data="account:payments")],
            [InlineKeyboardButton("📱 Номера", callback_data="account:numbers")],
            [InlineKeyboardButton("🚪 Выйти", callback_data="auth:logout")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="auth:back")],
        ]
    else:
        keyboard = [
            [InlineKeyboardButton("📲 Войти по номеру", callback_data="auth:contact")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="auth:back")],
        ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def phone_contact_keyboard() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    keyboard.add(KeyboardButton("Поделиться номером", request_contact=True))
    return keyboard


def account_numbers_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("➕ Добавить номер", callback_data="account:add_number")],
        [InlineKeyboardButton("⬅️ Мой аккаунт", callback_data="main:account")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def account_back_keyboard() -> InlineKeyboardMarkup:
    keyboard = [[InlineKeyboardButton("⬅️ Мой аккаунт", callback_data="main:account")]]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)
