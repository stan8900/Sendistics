from aiogram.dispatcher.filters.state import State, StatesGroup


class AutoCampaignStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_interval = State()


class GroupSearchStates(StatesGroup):
    waiting_for_chat_link = State()


class AuthStates(StatesGroup):
    waiting_for_register_username = State()
    waiting_for_register_password = State()
    waiting_for_login_username = State()
    waiting_for_login_password = State()
    waiting_for_phone = State()
    waiting_for_sms_code = State()
    waiting_for_contact = State()
    waiting_for_account_phone_number = State()
