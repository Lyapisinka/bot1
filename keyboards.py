from aiogram.fsm.context import FSMContext
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from config import LOCATIONS


def create_locations_keyboard() -> ReplyKeyboardMarkup:
    buttons = []
    row = []
    for loc in LOCATIONS:
        for loc_name in loc.values():
            row.append(KeyboardButton(text=loc_name))
            if len(row) == 3:
                buttons.append(row)
                row = []

    if row:
        buttons.append(row)

    buttons.append([KeyboardButton(text="Назад")])

    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def action_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Отмена пречеков"), KeyboardButton(text="Общая выручка")],
            [KeyboardButton(text="Скидки"), KeyboardButton(text="Чеки")],
            [KeyboardButton(text="Поиск чека"), KeyboardButton(text="По категориям")],
            [KeyboardButton(text="Назад")]
        ],
        resize_keyboard=True
    )

def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Рестораны")]
        ],
        resize_keyboard=True
    )


async def update_keyboard_history(state: FSMContext, new_keyboard: ReplyKeyboardMarkup):
    """Обновляет историю клавиатур для текущего пользователя."""
    data = await state.get_data()
    keyboard_history = data.get('keyboard_history', [])
    keyboard_history.append(new_keyboard)
    await state.update_data(keyboard_history=keyboard_history)