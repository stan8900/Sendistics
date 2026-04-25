import math
from typing import Dict, Iterable, List, Optional

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

GROUPS_PAGE_SIZE = 8

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
                InlineKeyboardButton("👤 Мой аккаунт", callback_data="main:account"),
            ],
            [
                InlineKeyboardButton("💰 Пополнить баланс", callback_data="main:pay"),
            ],
            controls_row,
            [
                InlineKeyboardButton("📥 Парсер", callback_data="main:parser"),
                InlineKeyboardButton("👥 Парсер групп", callback_data="main:group_parser"),
                InlineKeyboardButton("📨 Инвайтер", callback_data="main:inviter"),
            ],
            [InlineKeyboardButton("🌐 Общий прокси", callback_data="main:shared_proxy")],
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
                InlineKeyboardButton("👤 Мой аккаунт", callback_data="main:account"),
            ],
            [
                InlineKeyboardButton("💰 Пополнить баланс", callback_data="main:pay"),
            ],
            [InlineKeyboardButton("📜 История оплат", callback_data="main:user_payments")],
        ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def my_account_keyboard(*, allow_account_pick: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("💰 Пополнить баланс", callback_data="main:pay"),
            InlineKeyboardButton("📜 История оплат", callback_data="main:user_payments"),
        ]
    ]
    if allow_account_pick:
        rows.append([InlineKeyboardButton("📱 Номера", callback_data="auto:pick_account")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="auto:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def auto_menu_keyboard(*, is_enabled: bool, allow_group_pick: bool, allow_account_pick: bool) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("✏️ Сообщение", callback_data="auto:set_message"),
            InlineKeyboardButton("⏱ Интервал", callback_data="auto:set_interval"),
        ]
    ]
    if allow_group_pick:
        keyboard.append([InlineKeyboardButton("👥 Группы", callback_data="auto:pick_groups")])
    if allow_account_pick:
        keyboard.append([InlineKeyboardButton("📱 Номер", callback_data="auto:pick_account")])
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
    page: int = 0,
    per_page: int = GROUPS_PAGE_SIZE,
) -> InlineKeyboardMarkup:
    selected_set = set(selected_ids)
    sorted_items = sorted(known_chats.items(), key=lambda item: item[1].get("title", ""))
    total_known = len(sorted_items)
    per_page = max(1, per_page)
    total_pages = max(1, math.ceil(total_known / per_page)) if total_known else 1
    current_page = max(0, min(page, total_pages - 1))
    start = current_page * per_page
    end = start + per_page
    visible_items = sorted_items[start:end]
    rows: List[List[InlineKeyboardButton]] = []
    if total_pages > 1:
        prev_page = max(0, current_page - 1)
        next_page = min(total_pages - 1, current_page + 1)
        rows.append([
            InlineKeyboardButton(
                "⬅️" if current_page > 0 else "—",
                callback_data=f"group:{origin}:{'page' if current_page > 0 else 'noop'}:{prev_page}",
            ),
            InlineKeyboardButton(
                f"{current_page + 1}/{total_pages}",
                callback_data=f"group:{origin}:noop:{current_page}",
            ),
            InlineKeyboardButton(
                "➡️" if current_page < total_pages - 1 else "—",
                callback_data=f"group:{origin}:{'page' if current_page < total_pages - 1 else 'noop'}:{next_page}",
            ),
        ])
    if total_known:
        rows.append([
            InlineKeyboardButton("✅ Выбрать все", callback_data=f"group:{origin}:select_all:{current_page}"),
            InlineKeyboardButton("❎ Снять все", callback_data=f"group:{origin}:clear_all:{current_page}"),
        ])
    for chat_key, chat_info in visible_items:
        chat_id = int(chat_key)
        title = chat_info.get("title") or f"Чат {chat_id}"
        prefix = "✅" if chat_id in selected_set else "➕"
        rows.append([
            InlineKeyboardButton(
                f"{prefix} {title[:48]}",
                callback_data=f"group:{origin}:chat:{chat_id}:{current_page}",
            )
        ])
    rows.append([
        InlineKeyboardButton("⬅️ Готово", callback_data=f"group:{origin}:done:{current_page}")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def accounts_keyboard(
    accounts: List[Dict[str, str]],
    *,
    active_account_id: Optional[int],
    allow_bot_sender: bool,
    bot_label: str = "Отправлять от бота",
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if allow_bot_sender:
        prefix = "✅" if active_account_id is None else "➕"
        rows.append([
            InlineKeyboardButton(f"{prefix} {bot_label[:48]}", callback_data="accounts:set:bot"),
        ])
    for account in accounts:
        account_id = int(account["id"])
        label = account.get("title") or account.get("phone") or f"Аккаунт {account_id}"
        prefix = "✅" if account_id == active_account_id else "➕"
        has_proxy = bool(account.get("proxy_type") and account.get("proxy_host") and account.get("proxy_port"))
        suffix = " 🌐" if has_proxy else ""
        rows.append([
            InlineKeyboardButton(
                f"{prefix} {label[:42]}{suffix}",
                callback_data=f"accounts:set:{account_id}",
            ),
            InlineKeyboardButton(
                "🌐 Прокси",
                callback_data=f"accounts:proxy:{account_id}",
            ),
        ])
    if active_account_id is not None:
        rows.append([
            InlineKeyboardButton("🔄 Обновить чаты", callback_data=f"accounts:refresh:{active_account_id}")
        ])
    rows.append([InlineKeyboardButton("➕ Добавить номер", callback_data="accounts:add")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="accounts:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
