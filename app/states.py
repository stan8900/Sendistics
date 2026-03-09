from aiogram.dispatcher.filters.state import State, StatesGroup


class AutoCampaignStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_interval = State()


class PaymentStates(StatesGroup):
    waiting_for_card_number = State()
    waiting_for_card_name = State()


class AdminLoginStates(StatesGroup):
    waiting_for_code = State()


class AdminManualPaymentStates(StatesGroup):
    waiting_for_user = State()


class AccountStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_proxy = State()


class SharedProxyStates(StatesGroup):
    waiting_for_proxy = State()


class ParserStates(StatesGroup):
    waiting_for_channel = State()
    waiting_for_limit = State()


class InviteStates(StatesGroup):
    waiting_for_file = State()
    waiting_for_target = State()
    waiting_for_limits = State()


class GroupParserStates(StatesGroup):
    waiting_for_group = State()
