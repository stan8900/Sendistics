from functools import partial

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.types import ReplyKeyboardRemove

from .keyboards import (
    account_back_keyboard,
    account_numbers_keyboard,
    auth_menu_keyboard,
    main_menu_keyboard,
    phone_contact_keyboard,
)
from .sms import MissingSmsSender, SmsSender
from .states import AuthStates
from .storage import Storage


def register_auth_handlers(dp: Dispatcher, storage: Storage, sms_sender: SmsSender | None = None) -> None:
    sender = sms_sender or MissingSmsSender()
    dp.register_callback_query_handler(
        partial(show_account_menu, storage=storage),
        lambda call: call.data == "main:account",
        state="*",
    )
    dp.register_callback_query_handler(
        start_register,
        lambda call: call.data == "auth:register",
        state="*",
    )
    dp.register_callback_query_handler(
        start_login,
        lambda call: call.data == "auth:login",
        state="*",
    )
    dp.register_callback_query_handler(
        start_phone_login,
        lambda call: call.data == "auth:phone",
        state="*",
    )
    dp.register_callback_query_handler(
        start_contact_login,
        lambda call: call.data == "auth:contact",
        state="*",
    )
    dp.register_callback_query_handler(
        partial(logout, storage=storage),
        lambda call: call.data == "auth:logout",
        state="*",
    )
    dp.register_callback_query_handler(
        partial(show_payments, storage=storage),
        lambda call: call.data == "account:payments",
        state="*",
    )
    dp.register_callback_query_handler(
        partial(show_numbers, storage=storage),
        lambda call: call.data == "account:numbers",
        state="*",
    )
    dp.register_callback_query_handler(
        start_add_number,
        lambda call: call.data == "account:add_number",
        state="*",
    )
    dp.register_callback_query_handler(
        back_to_main_menu,
        lambda call: call.data == "auth:back",
        state="*",
    )
    dp.register_message_handler(
        handle_register_username,
        state=AuthStates.waiting_for_register_username,
    )
    dp.register_message_handler(
        partial(handle_register_password, storage=storage),
        state=AuthStates.waiting_for_register_password,
    )
    dp.register_message_handler(
        handle_login_username,
        state=AuthStates.waiting_for_login_username,
    )
    dp.register_message_handler(
        partial(handle_login_password, storage=storage),
        state=AuthStates.waiting_for_login_password,
    )
    dp.register_message_handler(
        partial(handle_phone_number, storage=storage, sms_sender=sender),
        state=AuthStates.waiting_for_phone,
    )
    dp.register_message_handler(
        partial(handle_sms_code, storage=storage),
        state=AuthStates.waiting_for_sms_code,
    )
    dp.register_message_handler(
        partial(handle_contact_login, storage=storage),
        content_types=types.ContentType.CONTACT,
        state=AuthStates.waiting_for_contact,
    )
    dp.register_message_handler(
        handle_contact_login_text,
        state=AuthStates.waiting_for_contact,
    )
    dp.register_message_handler(
        partial(handle_add_number, storage=storage),
        state=AuthStates.waiting_for_account_phone_number,
    )


async def show_account_menu(call: types.CallbackQuery, storage: Storage) -> None:
    dashboard = await storage.get_account_dashboard(call.from_user.id)
    if dashboard:
        account = dashboard["account"]
        text = _format_account_dashboard(
            account,
            payments_count=len(dashboard["payments"]),
            numbers_count=len(dashboard["phone_numbers"]),
        )
    else:
        text = "Войдите по номеру телефона, чтобы открыть раздел Мой аккаунт."
    await call.message.edit_text(
        text,
        reply_markup=auth_menu_keyboard(is_authenticated=dashboard is not None),
    )
    await call.answer()


async def start_register(call: types.CallbackQuery) -> None:
    await AuthStates.waiting_for_register_username.set()
    await call.message.edit_text(
        "Введите имя аккаунта. Можно использовать латиницу, цифры, точку, дефис и подчёркивание."
    )
    await call.answer()


async def handle_register_username(message: types.Message, state: FSMContext) -> None:
    async with state.proxy() as data:
        data["auth_username"] = message.text
    await AuthStates.waiting_for_register_password.set()
    await message.answer("Введите пароль минимум из 8 символов.")


async def handle_register_password(message: types.Message, state: FSMContext, storage: Storage) -> None:
    data = await state.get_data()
    try:
        account = await storage.create_account(
            data.get("auth_username", ""),
            message.text or "",
            telegram_user_id=message.from_user.id,
        )
    except ValueError as exc:
        await message.answer(str(exc), reply_markup=auth_menu_keyboard(is_authenticated=False))
        await state.finish()
        return

    await state.finish()
    await message.answer(
        f"Аккаунт создан: {account['username']}",
        reply_markup=auth_menu_keyboard(is_authenticated=True),
    )


async def start_login(call: types.CallbackQuery) -> None:
    await AuthStates.waiting_for_login_username.set()
    await call.message.edit_text("Введите имя аккаунта.")
    await call.answer()


async def handle_login_username(message: types.Message, state: FSMContext) -> None:
    async with state.proxy() as data:
        data["auth_username"] = message.text
    await AuthStates.waiting_for_login_password.set()
    await message.answer("Введите пароль.")


async def handle_login_password(message: types.Message, state: FSMContext, storage: Storage) -> None:
    data = await state.get_data()
    account = await storage.authenticate_account(
        data.get("auth_username", ""),
        message.text or "",
        telegram_user_id=message.from_user.id,
    )
    await state.finish()
    if not account:
        await message.answer(
            "Неверное имя аккаунта или пароль.",
            reply_markup=auth_menu_keyboard(is_authenticated=False),
        )
        return

    await message.answer(
        f"Вы вошли как {account['username']}",
        reply_markup=auth_menu_keyboard(is_authenticated=True),
    )


async def start_phone_login(call: types.CallbackQuery) -> None:
    await AuthStates.waiting_for_phone.set()
    await call.message.edit_text("Введите номер телефона в международном формате, например +447700900123.")
    await call.answer()


async def handle_phone_number(
    message: types.Message,
    state: FSMContext,
    storage: Storage,
    sms_sender: SmsSender,
) -> None:
    try:
        login = await storage.start_phone_login(message.text or "")
        await sms_sender.send_login_code(login["phone_number"], login["code"])
    except ValueError as exc:
        await message.answer(str(exc), reply_markup=auth_menu_keyboard(is_authenticated=False))
        await state.finish()
        return
    except RuntimeError:
        await message.answer(
            "SMS-отправка не настроена. Подключите SMS-провайдера и передайте его в register_auth_handlers().",
            reply_markup=auth_menu_keyboard(is_authenticated=False),
        )
        await state.finish()
        return

    async with state.proxy() as data:
        data["auth_phone_number"] = login["phone_number"]
    await AuthStates.waiting_for_sms_code.set()
    await message.answer("Код отправлен по SMS. Введите 6 цифр из сообщения.")


async def handle_sms_code(message: types.Message, state: FSMContext, storage: Storage) -> None:
    data = await state.get_data()
    phone_number = data.get("auth_phone_number", "")
    account = await storage.verify_phone_login(
        phone_number,
        message.text or "",
        telegram_user_id=message.from_user.id,
    )
    await state.finish()
    if not account:
        await message.answer(
            "Неверный или просроченный код.",
            reply_markup=auth_menu_keyboard(is_authenticated=False),
        )
        return

    await message.answer(
        _format_account_dashboard(account, payments_count=0, numbers_count=len(account.get("phone_numbers", []))),
        reply_markup=auth_menu_keyboard(is_authenticated=True),
    )


async def start_contact_login(call: types.CallbackQuery) -> None:
    await AuthStates.waiting_for_contact.set()
    await call.message.answer(
        "Нажмите кнопку ниже, чтобы подтвердить номер Telegram без SMS.",
        reply_markup=phone_contact_keyboard(),
    )
    await call.answer()


async def handle_contact_login(message: types.Message, state: FSMContext, storage: Storage) -> None:
    contact = message.contact
    if not contact or contact.user_id != message.from_user.id:
        await message.answer(
            "Нужно отправить именно свой номер через кнопку.",
            reply_markup=phone_contact_keyboard(),
        )
        return

    account = await storage.login_with_verified_phone(
        contact.phone_number,
        telegram_user_id=message.from_user.id,
    )
    await state.finish()
    await message.answer(
        _format_account_dashboard(account, payments_count=0, numbers_count=len(account.get("phone_numbers", []))),
        reply_markup=ReplyKeyboardRemove(),
    )
    await message.answer("Раздел Мой аккаунт", reply_markup=auth_menu_keyboard(is_authenticated=True))


async def handle_contact_login_text(message: types.Message) -> None:
    await message.answer(
        "Нажмите кнопку “Поделиться номером”. Вручную введённый номер не подтверждает владельца.",
        reply_markup=phone_contact_keyboard(),
    )


async def show_payments(call: types.CallbackQuery, storage: Storage) -> None:
    account = await storage.get_active_account(call.from_user.id)
    if not account:
        await call.message.edit_text(
            "Сначала войдите по номеру телефона.",
            reply_markup=auth_menu_keyboard(is_authenticated=False),
        )
        await call.answer()
        return

    payments = await storage.list_account_payments(account["id"])
    await call.message.edit_text(_format_payments(payments), reply_markup=account_back_keyboard())
    await call.answer()


async def show_numbers(call: types.CallbackQuery, storage: Storage) -> None:
    account = await storage.get_active_account(call.from_user.id)
    if not account:
        await call.message.edit_text(
            "Сначала войдите по номеру телефона.",
            reply_markup=auth_menu_keyboard(is_authenticated=False),
        )
        await call.answer()
        return

    numbers = await storage.list_account_phone_numbers(account["id"])
    await call.message.edit_text(_format_numbers(numbers), reply_markup=account_numbers_keyboard())
    await call.answer()


async def start_add_number(call: types.CallbackQuery) -> None:
    await AuthStates.waiting_for_account_phone_number.set()
    await call.message.edit_text("Введите номер для авторассылок в международном формате.")
    await call.answer()


async def handle_add_number(message: types.Message, state: FSMContext, storage: Storage) -> None:
    account = await storage.get_active_account(message.from_user.id)
    if not account:
        await state.finish()
        await message.answer(
            "Сначала войдите по номеру телефона.",
            reply_markup=auth_menu_keyboard(is_authenticated=False),
        )
        return
    try:
        await storage.add_account_phone_number(account["id"], message.text or "", auto_enabled=True)
    except ValueError as exc:
        await state.finish()
        await message.answer(str(exc), reply_markup=account_numbers_keyboard())
        return

    numbers = await storage.list_account_phone_numbers(account["id"])
    await state.finish()
    await message.answer(_format_numbers(numbers), reply_markup=account_numbers_keyboard())


async def logout(call: types.CallbackQuery, storage: Storage) -> None:
    await storage.logout_telegram_user(call.from_user.id)
    await call.message.edit_text(
        "Вы вышли из аккаунта.",
        reply_markup=auth_menu_keyboard(is_authenticated=False),
    )
    await call.answer()


async def back_to_main_menu(call: types.CallbackQuery) -> None:
    await call.message.edit_text("Главное меню", reply_markup=main_menu_keyboard())
    await call.answer()


def _format_account_dashboard(account: dict, *, payments_count: int, numbers_count: int) -> str:
    phone_number = account.get("phone_number") or account.get("username", "")
    return (
        "Мой аккаунт\n"
        f"Номер: {phone_number}\n"
        f"Платежей: {payments_count}\n"
        f"Номеров с авторассылками: {numbers_count}"
    )


def _format_payments(payments: list[dict]) -> str:
    if not payments:
        return "Платежи\nПлатежей пока нет."
    lines = ["Платежи"]
    for payment in payments[-10:]:
        amount = payment.get("amount")
        currency = payment.get("currency", "")
        status = payment.get("status", "")
        description = payment.get("description") or payment.get("id", "")
        lines.append(f"{amount} {currency} - {status} - {description}")
    return "\n".join(lines)


def _format_numbers(numbers: list[dict]) -> str:
    if not numbers:
        return "Номера\nНомеров для авторассылок пока нет."
    lines = ["Номера с авторассылками"]
    for number in numbers:
        enabled = "включена" if number.get("auto_enabled") else "выключена"
        label = number.get("label") or "Без названия"
        lines.append(f"{number['phone_number']} - {label} - авторассылка {enabled}")
    return "\n".join(lines)
