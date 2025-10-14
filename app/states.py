from aiogram.dispatcher.filters.state import State, StatesGroup


class AutoCampaignStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_interval = State()


class PaymentStates(StatesGroup):
    waiting_for_card_number = State()
    waiting_for_card_name = State()
