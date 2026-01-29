from typing import Any, Dict, Iterable, List

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
    known_chats: Dict[str, Dict[str, Any]],
    selected_ids: Iterable[int],
    *,
    origin: str = "auto",
    page: int = 0,
    page_size: int = 20,
) -> InlineKeyboardMarkup:
    selected_set = set(selected_ids)
    rows: List[List[InlineKeyboardButton]] = []
    sorted_items = [
        (int(chat_key), info) for chat_key, info in sorted(known_chats.items(), key=lambda item: item[1].get("title", ""))
    ]
    total = len(sorted_items)
    if total == 0:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton("⬅️ Готово", callback_data=f"group:{origin}:done")],
            ]
        )
    page_size = max(5, page_size)
    total_pages = max(1, (total + page_size - 1) // page_size)
    current_page = max(0, min(page, total_pages - 1))
    start = current_page * page_size
    end = start + page_size
    page_items = sorted_items[start:end]
    chat_ids = [chat_id for chat_id, _ in sorted_items]
    all_selected = bool(chat_ids) and all(chat_id in selected_set for chat_id in chat_ids)
    for chat_id, chat_info in page_items:
        title = chat_info.get("title") or f"Чат {chat_id}"
        prefix = "✅" if chat_id in selected_set else "➕"
        availability_marker = "🤖" if chat_info.get("delivery_available") else "🚫"
        rows.append(
            [
                InlineKeyboardButton(
                    f"{prefix} {availability_marker} {title[:40]}",
                    callback_data=f"group:{origin}:select|{current_page}|{chat_id}",
                )
            ]
        )
    toggle_label = "➖ Снять выделение" if all_selected else "✅ Выбрать все"
    rows.append(
        [InlineKeyboardButton(toggle_label, callback_data=f"group:{origin}:all|{current_page}")]
    )
    nav_row: List[InlineKeyboardButton] = []
    if current_page > 0:
        nav_row.append(
            InlineKeyboardButton("⬅️", callback_data=f"group:{origin}:page|{current_page - 1}")
        )
    nav_row.append(
        InlineKeyboardButton(
            f"{current_page + 1}/{total_pages}",
            callback_data=f"group:{origin}:noop|{current_page}",
        )
    )
    if current_page < total_pages - 1:
        nav_row.append(
            InlineKeyboardButton("➡️", callback_data=f"group:{origin}:page|{current_page + 1}")
        )
    rows.append(nav_row)
    rows.append([InlineKeyboardButton("⬅️ Готово", callback_data=f"group:{origin}:done")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def inbox_reply_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton("✉️ Ответить", callback_data=f"inbox:reply:{user_id}")],
        ]
    )
