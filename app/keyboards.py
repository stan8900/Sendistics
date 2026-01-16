from typing import Dict, Iterable, List

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def main_menu_keyboard(is_admin: bool) -> InlineKeyboardMarkup:
    if is_admin:
        keyboard = [
            [
                InlineKeyboardButton("⚒ Авторассылка", callback_data="main:auto"),
                InlineKeyboardButton("💰 Пополнить баланс", callback_data="main:pay"),
            ],
            [
                InlineKeyboardButton("📊 Статистика", callback_data="main:stats"),
                InlineKeyboardButton("📋 Выбрать группы", callback_data="main:groups"),
                InlineKeyboardButton("⚙️ Настройки", callback_data="main:settings"),
            ],
            [InlineKeyboardButton("📜 Оплаты", callback_data="main:admin_payments")],
        ]
    else:
        keyboard = [
            [
                InlineKeyboardButton("⚒ Авторассылка", callback_data="main:auto"),
                InlineKeyboardButton("💰 Пополнить баланс", callback_data="main:pay"),
            ],
            [InlineKeyboardButton("📜 История оплат", callback_data="main:user_payments")],
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


def groups_keyboard(
    known_chats: Dict[str, Dict[str, str]],
    selected_ids: Iterable[int],
    *,
    origin: str = "auto",
) -> InlineKeyboardMarkup:
    selected_set = set(selected_ids)
    rows: List[List[InlineKeyboardButton]] = []
    for chat_key, chat_info in sorted(known_chats.items(), key=lambda item: item[1].get("title", "")):
        chat_id = int(chat_key)
        title = chat_info.get("title") or f"Чат {chat_id}"
        prefix = "✅" if chat_id in selected_set else "➕"
        rows.append([
            InlineKeyboardButton(
                f"{prefix} {title[:48]}", callback_data=f"group:{origin}:{chat_id}"
            )
        ])
    rows.append([
        InlineKeyboardButton("⬅️ Готово", callback_data=f"group:{origin}:done")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)
