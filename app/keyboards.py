from typing import Dict, Iterable, List

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def main_menu_keyboard(is_admin: bool, *, allow_group_pick: bool) -> InlineKeyboardMarkup:
    if is_admin:
        controls_row = [
            InlineKeyboardButton("📊 Статистика", callback_data="main:stats"),
            InlineKeyboardButton("⚙️ Настройки", callback_data="main:settings"),
        ]
        if allow_group_pick:
            controls_row.insert(1, InlineKeyboardButton("📋 Выбрать группы", callback_data="main:groups"))
        keyboard = [
            [
                InlineKeyboardButton("⚒ Авторассылка", callback_data="main:auto"),
                InlineKeyboardButton("💰 Пополнить баланс", callback_data="main:pay"),
            ],
            controls_row,
            [
                InlineKeyboardButton("📜 Оплаты", callback_data="main:admin_payments"),
                InlineKeyboardButton("🔁 Перепроверить оплату", callback_data="main:manual_payment"),
            ],
            [InlineKeyboardButton("📄 PDF оплат", callback_data="main:payments_pdf")],
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


def auto_menu_keyboard(*, is_enabled: bool, allow_group_pick: bool) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("✏️ Сообщение", callback_data="auto:set_message"),
            InlineKeyboardButton("⏱ Интервал", callback_data="auto:set_interval"),
        ]
    ]
    if allow_group_pick:
        keyboard.append([InlineKeyboardButton("👥 Группы", callback_data="auto:pick_groups")])
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
    sorted_items = [(int(chat_key), info) for chat_key, info in sorted(
        known_chats.items(), key=lambda item: item[1].get("title", "")
    )]
    chat_ids = [chat_id for chat_id, _ in sorted_items]
    all_selected = bool(chat_ids) and all(chat_id in selected_set for chat_id in chat_ids)
    for chat_id, chat_info in sorted_items:
        title = chat_info.get("title") or f"Чат {chat_id}"
        prefix = "✅" if chat_id in selected_set else "➕"
        rows.append([
            InlineKeyboardButton(
                f"{prefix} {title[:48]}", callback_data=f"group:{origin}:{chat_id}"
            )
        ])
    if chat_ids:
        toggle_label = "➖ Снять выделение" if all_selected else "✅ Выбрать все"
        rows.append([
            InlineKeyboardButton(toggle_label, callback_data=f"group:{origin}:all")
        ])
    rows.append([
        InlineKeyboardButton("⬅️ Готово", callback_data=f"group:{origin}:done")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)
