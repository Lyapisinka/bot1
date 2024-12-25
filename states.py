from aiogram.fsm.state import State, StatesGroup


class Form(StatesGroup):
    choosing_location = State()
    waiting_for_action = State()
    choosing_date = State()
    choosing_date2 = State()
    waiting_for_check = State()
