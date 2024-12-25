from aiogram import types, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.filters import Command
from aiogram_calendar import SimpleCalendar, SimpleCalendarCallback
from config import LOCATIONS
from states import Form
from keyboards import create_locations_keyboard, action_keyboard, main_menu_keyboard, update_keyboard_history
import send_reports

router = Router()


@router.message(Command(commands=["start"]))
async def start(message: types.Message, state: FSMContext):
    await message.answer("Добро пожаловать! Выберите действие:", reply_markup=main_menu_keyboard())
    await update_keyboard_history(state, main_menu_keyboard())


# Обработчик для кнопки "Назад"
@router.message(F.text == "Назад")
async def go_back(message: types.Message, state: FSMContext):
    data = await state.get_data()
    keyboard_history = data.get('keyboard_history', [])
    if len(keyboard_history) > 1:
        # Удаляем текущую клавиатуру и показываем предыдущую
        keyboard_history.pop()
        previous_keyboard = keyboard_history[-1]
        await message.reply("Возвращаемся назад:", reply_markup=previous_keyboard)
        await state.update_data(keyboard_history=keyboard_history)
    else:
        await message.reply("Вы находитесь в главном меню!")


@router.message(F.text == "Рестораны")
async def show_locations_keyboard(message: types.Message, state: FSMContext):
    keyboard = create_locations_keyboard()
    await message.reply("Выберите ресторан:", reply_markup=keyboard)
    await update_keyboard_history(state, keyboard)


@router.message(lambda message: message.text in [loc_name for loc in LOCATIONS for loc_name in loc.values()])
async def handle_location_choice(message: types.Message, state: FSMContext):
    selected_location = message.text
    for loc in LOCATIONS:
        for loc_id, loc_name in loc.items():
            if loc_name == selected_location:
                await state.update_data(location_id=loc_id, location_name=loc_name)
                action_key = action_keyboard()
                await message.reply("Выберите действие:", reply_markup=action_key)
                await update_keyboard_history(state, action_key)


# Обработчик выбора даты в календаре
@router.callback_query(SimpleCalendarCallback.filter(), Form.choosing_date)
async def process_calendar(callback_query: types.CallbackQuery, callback_data: dict, state: FSMContext):
    selected, date = await SimpleCalendar().process_selection(callback_query, callback_data)
    if selected:
        await state.update_data(selected_date=date)
        user_data = await state.get_data()
        date = user_data.get('selected_date')
        if user_data.get('action') == 'Общая выручка':
            await send_reports.show_total_reciepts(callback_query.message, state)
        if user_data.get('action') == 'Отмена пречеков':
            if user_data.get('flag') == True:
                await state.update_data(next_date=date)
                await send_reports.show_cancel_precheck(callback_query.message, state)
            else:
                await state.update_data(prev_date=date)
                await state.update_data(flag=True)
                await callback_query.message.answer("Выберите дату конца отчёта:",
                                                    reply_markup=await SimpleCalendar().start_calendar())
                await state.set_state(Form.choosing_date)
        if user_data.get('action') == 'Скидки':
            await send_reports.show_discounts(callback_query.message, state)
        if user_data.get('action') == 'Чеки':
            await send_reports.show_all_check(callback_query.message, state)
        if user_data.get('action') == 'По категориям':
            if user_data.get('flag') == True:
                await state.update_data(next_date=date)
                await send_reports.show_category_dish(callback_query.message, state)
            else:
                await state.update_data(prev_date=date)
                await state.update_data(flag=True)
                await callback_query.message.answer("Выберите дату конца отчёта:",
                                                    reply_markup=await SimpleCalendar().start_calendar())
                await state.set_state(Form.choosing_date)


@router.message(F.text == "Общая выручка")
async def handle_action_choice(message: types.Message, state: FSMContext):
    await message.answer("Выберите дату:", reply_markup=await SimpleCalendar().start_calendar())
    await state.update_data(action=message.text)
    await state.set_state(Form.choosing_date)


@router.message(F.text == "Отмена пречеков")
async def cansel_void(message: types.Message, state: FSMContext):
    await message.answer("Выберите дату начала отчёта:",
                         reply_markup=await SimpleCalendar().start_calendar())
    await state.update_data(action=message.text)
    await state.update_data(flag=False)
    await state.set_state(Form.choosing_date)


@router.message(F.text == "Скидки")
async def discont(message: types.Message, state: FSMContext):
    await message.answer("Выберите дату отчёта:",
                         reply_markup=await SimpleCalendar().start_calendar())
    await state.update_data(action=message.text)
    await state.set_state(Form.choosing_date)


@router.message(F.text == "Поиск чека")
async def find_check(message: types.Message, state: FSMContext):
    await message.answer("Введите номер чека:")
    await state.set_state(Form.waiting_for_check)


@router.message(F.text.startswith('/'))
async def process_check(message: types.Message, state: FSMContext):
    await send_reports.show_detail_check(message, state)


@router.message(Form.waiting_for_check)
async def waiting_for_check(message: types.Message, state: FSMContext):
    await send_reports.show_detail_check(message, state)


@router.message(F.text == "Чеки")
async def bills(message: types.Message, state: FSMContext):
    await message.answer("Выберите дату отчёта:",
                         reply_markup=await SimpleCalendar().start_calendar())
    await state.update_data(action=message.text)
    await state.set_state(Form.choosing_date)


@router.message(F.text == "По категориям")
async def category_dish(message: types.Message, state: FSMContext):
    await message.answer("Выберите дату начала отчёта:",
                         reply_markup=await SimpleCalendar().start_calendar())
    await state.update_data(action=message.text)
    await state.update_data(flag=False)
    await state.set_state(Form.choosing_date)
