from aiogram.dispatcher.filters.state import State, StatesGroup


class AutoCampaignStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_interval = State()


class GroupSearchStates(StatesGroup):
    waiting_for_chat_link = State()
