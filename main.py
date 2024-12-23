import pandas as pd
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
from aiogram_calendar import SimpleCalendar, SimpleCalendarCallback
from config import API_TOKEN, LOCATIONS, LOCATION_CONFIG
from connect import fetch_data
import query

async def update_keyboard_history(state: FSMContext, new_keyboard: ReplyKeyboardMarkup):
    """Обновляет историю клавиатур для текущего пользователя."""
    data = await state.get_data()
    keyboard_history = data.get('keyboard_history', [])
    keyboard_history.append(new_keyboard)
    await state.update_data(keyboard_history=keyboard_history)


# Создаём список доступных названий локаций
location_names = [loc_name for loc in LOCATIONS for loc_name in loc.values()]
# Создание объектов бота и диспетчера
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)


class Form(StatesGroup):
    choosing_location = State()
    waiting_for_action = State()
    choosing_date = State()
    choosing_date2 = State()
    waiting_for_check = State()


# Определяем наши собственные метки для календаря

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


@router.message(F.text == "Общая выручка")
async def handle_action_choice(message: types.Message, state: FSMContext):
    await message.answer("Выберите дату:", reply_markup=await SimpleCalendar().start_calendar())
    await state.set_state(Form.choosing_date)


# Обработчик выбора даты в календаре
@router.callback_query(SimpleCalendarCallback.filter(), Form.choosing_date)
async def process_calendar(callback_query: types.CallbackQuery, callback_data: dict, state: FSMContext):
    selected, date = await SimpleCalendar().process_selection(callback_query, callback_data)
    if selected:
        await state.update_data(selected_date=date)
        user_data = await state.get_data()
        date = user_data.get('selected_date')
        if user_data.get('action') == 'Отмена пречеков':
            if user_data.get('flag') == True:
                await state.update_data(next_date=date)
                await perform_action(callback_query.message, state)
            else:
                await state.update_data(prev_date=date)
                await state.update_data(flag=True)
                await callback_query.message.answer("Выберите дату конца отчёта:",
                                     reply_markup=await SimpleCalendar().start_calendar())
                await state.set_state(Form.choosing_date)
        if user_data.get('action') == 'Скидки':
            await show_discounts(callback_query.message, state)
        if user_data.get('action') == 'Чеки':
            await show_bills(callback_query.message, state)
        if user_data.get('action') == 'По категориям':
            if user_data.get('flag') == True:
                await state.update_data(next_date=date)
                await show_category_dish(callback_query.message, state)
            else:
                await state.update_data(prev_date=date)
                await state.update_data(flag=True)
                await callback_query.message.answer("Выберите дату конца отчёта:",
                                     reply_markup=await SimpleCalendar().start_calendar())
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


@router.message(lambda message: message.text.startswith('/'))
async def process_check(message: types.Message, state: FSMContext):
    await process_check_number_input(message, state)

@router.message(lambda message: message.text == "Поиск чека")
async def find_check(message: types.Message, state: FSMContext):
    await message.answer("Введите номер чека:")
    await state.set_state(Form.waiting_for_check)



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


async def show_category_dish(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    location_name = user_data.get('location_name')
    location_id = user_data.get('location_id')
    date = user_data.get('prev_date').strftime("%d.%m.%Y")
    next_data = user_data.get('next_date').strftime("%d.%m.%Y")
    try:
        # Получение параметров для указанной локации или использование параметров по умолчанию
        config = LOCATION_CONFIG.get(location_name, LOCATION_CONFIG['default'])

        # Извлечение параметров из словаря
        ip_address = config['ip_address']
        username = config['username']
        password = config['password']
        classific = config['classific']

        # Выполнение подключения и проверки
        df = query.connectbd(location_id, ip_address, username, password, 'sales_dish', date, end=next_data,
                               classic=classific)
        if df.empty:
            await message.reply(f"Нет данных для локации '{location_name}'.")
        else:
            message_text = []
            cat = ""
            for index, row in df.iterrows():
                if cat != row['CATEGORY']:
                    message_text.append(f"<b>Категория: {row['CATEGORY']}</b>\n\n")
                    cat = row['CATEGORY']
                message_text.append(f"<pre><code>{row['QUANTITY']}   {row['DISH']}")
                message_text.append(f"Суммы: {row['PRLISTSUM']:.2f}  {row['PAYSUM']:.2f}   {round(float(row['PRLISTSUM']) - float(row['PAYSUM']), 2)}</code></pre>")

            complete_message = "\n".join(message_text)
            lines = complete_message.splitlines()
            result = []
            current_check = None
            current_code_block = []


            for item in lines:
                item = item.strip()  # Удаляем пробелы по краям

                if item.startswith("<b>Категория:"):
                    # Добавляем текущий чек если есть кодовый блок
                    if current_check and current_code_block:
                        result.append(current_check)
                        result.append("\n".join(current_code_block))
                        current_code_block = []

                    current_check = item

                elif item.startswith("<pre><code>") or item.endswith("</code></pre>") or len(current_code_block) > 0:
                    # Продолжаем собирать кодовый блок
                    current_code_block.append(item)
                    if item.endswith("</code></pre>"):
                        # Завершился кодовый блок, добавляем данные
                        if current_check:
                            result.append(current_check)
                        result.append("\n".join(current_code_block))
                        current_check = None
                        current_code_block = []

            # Последняя проверка в случае, если есть незавершенные данные
            if current_check and current_code_block:
                result.append(current_check)
                result.append("\n".join(current_code_block))
            df = ""

            for i in range(len(result)):
                df += result[i] + "\n"

                if len(result) == i + 1:
                    break
                if result[i + 1].startswith("<b>Категория:") or len(df) > 2000:
                    # Отправляем сообщение только если встретили элемент начинающийся с "Скидка:"
                    await message.answer(df, parse_mode='HTML')
                    # Очищаем df для новых данных после отправки
                    df = ""
            # Отправляем оставшиеся данные, если они существуют и если не завершились последней отправкой
            if df:
                await message.answer(df, parse_mode='HTML')

    except Exception as e:
        await message.reply(f"Ошибка при получении данных для {location_name}: {e}")

    finally:
        await state.set_state(Form.waiting_for_action)


async def show_bills(message, state: FSMContext):
    user_data = await state.get_data()
    date = user_data.get('selected_date').strftime("%d.%m.%Y")
    location_name = user_data.get('location_name')
    location_id = user_data.get('location_id')
    try:
        # Получение параметров для указанной локации или использование параметров по умолчанию
        config = LOCATION_CONFIG.get(location_name, LOCATION_CONFIG['default'])

        # Извлечение параметров из словаря
        ip_address = config['ip_address']
        username = config['username']
        password = config['password']

        # Выполнение подключения к базе данных
        df = query.connectbd(location_id, ip_address, username, password, 'get_check', date)

        # Проверка, пуст ли полученный DataFrame
        if df.empty:
            await message.reply(f"Нет данных для даты {date}.")
        else:
            message_lines = []
            cur = ""
            for index, row in df.iterrows():
                if row['CURRENCY'] != cur:
                    message_lines.append(f"\n<b>{row['CURRENCY']}</b>\n")
                    cur = row['CURRENCY']
                message_lines.append(f"<i>{row['CLOSEDATETIME']}</i>  /{row['CHECKNUM']}  Сумма:  {row['BINDEDSUM']}")  # Склеиваем строки в одно сообщение

            # Преобразование в единый текст с правильными переносами строк

            complete_message = "\n".join(message_lines)
            lines = complete_message.splitlines()
            result = []

            for item in lines:
                item = item.strip()
                result.append(item)
            df = ""
            for i in range(len(result)):
                df += result[i] + "\n"
                if len(result) == i + 1:
                    break
                if len(df) > 2000:
                    # Отправляем сообщение только если встретили элемент начинающийся с "Скидка:"
                    await message.answer(df, parse_mode='HTML')
                    # Очищаем df для новых данных после отправки
                    df = ""
            # Отправляем оставшиеся данные, если они существуют и если не завершились последней отправкой
            if df:
                await message.answer(df, parse_mode='HTML')


    except Exception as e:
        await message.reply(f"Ошибка при получении данных для {location_name}: {e}")

    finally:
        await state.set_state(Form.waiting_for_action)



@router.message(Form.waiting_for_check)
async def process_check_number_input(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    location_name = user_data.get('location_name')
    location_id = user_data.get('location_id')
    if message.text[0] == '/':
        check_number = message.text[1:]
    else:
        check_number = message.text
    await message.reply(f"Ищем данные по номеру {check_number}")
    try:
        # Получение параметров для указанной локации или использование параметров по умолчанию
        config = LOCATION_CONFIG.get(location_name, LOCATION_CONFIG['default'])

        # Извлечение параметров из словаря
        ip_address = config['ip_address']
        username = config['username']
        password = config['password']

        # Подключение к базе данных и извлечение данных
        df_order = query.connectbd(location_id, ip_address, username, password, 'order', check=check_number)
        df_session = query.connectbd(location_id, ip_address, username, password, 'session', check=check_number)
        df_payment = query.connectbd(location_id, ip_address, username, password, 'payment_data', check=check_number)
        df_discount = query.connectbd(location_id, ip_address, username, password, 'discount', check=check_number)

        # Проверка, пуст ли полученный DataFrame
        if df_order.empty:
            await message.reply(f"Нет данных для локации '{location_name}'.")
        else:
            total_width = 49
            message_lines = [f"Номер чека: {check_number}"]

            # Проход по DataFrame
            for index, row in df_order.iterrows():
                message_lines.append(f"Дата смены: {row['ShiftDate']}")
                message_lines.append(f"Стол: {row['TABLENAME']}")
                message_lines.append(f"Кол-во гостей: {row['GUESTSCOUNT']}")
                message_lines.append(f"Официант: {row['EmployeeName']}")
            for index, row in df_payment.iterrows():
                if not f"Кассир: {row['EmployeeName']}" in message_lines:
                    message_lines.append(f"Кассир: {row['EmployeeName']}")
            for index, row in df_order.iterrows():
                message_lines.append(f"Общая сумма: {row['PRICELISTSUM']}")
            message_lines.append("\nБлюда:")
            for index, row in df_session.iterrows():
                # Преобразуем количество и названия в строку
                if row['ISCOMBOCOMP'] == 0 and row['DisplayName']:
                    quantity_str = f"{row['Quantity']}"
                    if quantity_str == "0.0":
                        menu_item_str = f"\"{row['DisplayName']}\""
                    else:
                        menu_item_str = f"{row['DisplayName']}"
                    # 3 пробела между количеством и названием блюда
                    combined_str = f"{quantity_str}  {menu_item_str} "
                    pr_list_sum_str = f"{row['PRListSum']}"
                    # Вычислим место, оставшееся под сумму
                    remaining_space = (total_width - len(combined_str) - len(pr_list_sum_str))
                    # Выравниваем сумму по правому краю, используя оставшееся место
                    right_aligned_line = combined_str + ' ' * remaining_space + pr_list_sum_str
                    message_lines.append(right_aligned_line)
                if row['ISCOMBOCOMP'] == 1:
                    message_lines.append(f"     {row['Quantity']}  комбо: \"{row['DisplayName']}\"")
                if row['DisplayModifierOpenName']:
                    message_lines.append(f"     {row['ModifierPieces']}  \"{row['DisplayModifierOpenName']}\"")
                if row['VoidName']:
                    message_lines.append(f"     \"{row['VoidName']}\"")
            # Добавляем данные о скидках
            if not df_discount.empty:
                message_lines.append("\nСкидки:")
                for index, row in df_discount.iterrows():
                    discount_name = row['DiscountName']
                    discount_sum = str(abs(row['DiscountAmount']))
                    remaining_space = (total_width - len(discount_name) - len(discount_sum))
                    right_aligned_line = discount_name + ' ' * remaining_space + discount_sum
                    message_lines.append(right_aligned_line)
                    if row['Holder']:
                        message_lines.append(f"Карта: {row['Holder']}")

            message_lines.append("\nСпособы оплаты:")
            for index, row in df_payment.iterrows():
                remaining_space = (total_width - len(row['CurrencyName']) - len(str(row['PaymentNationalSum'])))
                right_aligned_line = row['CurrencyName'] + ' ' * remaining_space + str(row['PaymentNationalSum'])
                message_lines.append(right_aligned_line)

            # Склеиваем строки в одно сообщение
            df_message = "\n".join(message_lines)

            await message.answer(f"<pre>{df_message}</pre>", parse_mode='HTML')
    except Exception as e:
        await message.reply(f"Ошибка при получении данных для {check_number}: {e}")

    finally:
        await state.set_state(Form.waiting_for_action)


async def show_discounts(message, state: FSMContext):
    user_data = await state.get_data()
    selected_date = user_data.get('selected_date')
    location_name = user_data.get('location_name')
    location_id = user_data.get('location_id')
    if selected_date and location_name:
        date_str = selected_date.strftime("%d.%m.%Y")
    else:
        await message.reply("Информация о скидках недоступна. Пожалуйста, выберите локацию и дату.")

    if selected_date:
        date_str = selected_date.strftime("%d.%m.%Y")
    else:
        await message.reply("Не удалось определить дату, выберите ещё раз.")
        await state.set_state(Form.choosing_date)

    try:
        # Получение параметров для указанной локации или использование параметров по умолчанию
        config = LOCATION_CONFIG.get(location_name, LOCATION_CONFIG['default'])

        # Извлечение параметров из словаря
        ip_address = config['ip_address']
        username = config['username']
        password = config['password']

        # Выполнение подключения и извлечение данных
        df = query.connectbd(location_id, ip_address, username, password, 'discount_data', date_str)

        # Проверка, пуст ли полученный DataFrame
        if df.empty:
            await message.reply(f"Нет данных для локации '{location_name}' за указанную дату.")
        else:
            message_text = []
            for index, row in df.iterrows():
                if row['NAME']:
                    message_text.append(f"<b>Скидка: {row['NAME']}</b>\n\n")
                message_text.append(f"Чек: /{row['CHECKNUM']}")
                message_text.append(f"<pre><code>{row['ShiftDate']}   {row['CURRENCY_NAME']}")
                message_text.append(f"{str(row['PR']).ljust(10)} {str(row['DI']).ljust(10)} {row['NI']}")
                if row['CARDCODE']:
                    message_text.append(f"{row['HOLDER']} {row['CARDCODE']}</code></pre>")
                else:
                    message_text.append("</code></pre>")

            # Преобразование в единый текст с правильными переносами строк
            complete_message = "\n".join(message_text)
            lines = complete_message.splitlines()
            result = []
            current_check = None
            current_code_block = []
            current_discount = None
            for item in lines:
                item = item.strip()  # Удаляем пробелы по краям
                if item.startswith("<b>Скидка:"):
                    # Сохраняем текущую скидку и чистим
                    if current_discount:
                        if current_check:
                            result.append(current_check)
                        if current_code_block:
                            result.append("\n".join(current_code_block))

                    # Обновляем текущую скидку
                    current_discount = item
                    result.append(current_discount)
                    current_check = None
                    current_code_block = []

                elif item.startswith("Чек:"):
                    # Добавляем текущий чек если есть кодовый блок
                    if current_check and current_code_block:
                        result.append(current_check)
                        result.append("\n".join(current_code_block))
                        current_code_block = []

                    current_check = item

                elif item.startswith("<pre><code>") or item.endswith("</code></pre>") or len(current_code_block) > 0:
                    # Продолжаем собирать кодовый блок
                    current_code_block.append(item)
                    if item.endswith("</code></pre>"):
                        # Завершился кодовый блок, добавляем данные
                        if current_check:
                            result.append(current_check)
                        result.append("\n".join(current_code_block))
                        current_check = None
                        current_code_block = []

            # Последняя проверка в случае, если есть незавершенные данные
            if current_check and current_code_block:
                result.append(current_check)
                result.append("\n".join(current_code_block))

            # Формируем итоговое сообщение для отправки
            # Представление списка сообщений
            df = ""

            for i in range(len(result)):
                print(result[i])
                df += result[i] + "\n"
                if len(result) == i + 1:
                    break
                if result[i + 1].startswith("<b>Скидка:") or len(df) > 2000:
                    # Отправляем сообщение только если встретили элемент начинающийся с "Скидка:"
                    await message.answer(df, parse_mode='HTML')
                    # Очищаем df для новых данных после отправки
                    df = ""

            # Отправляем оставшиеся данные, если они существуют и если не завершились последней отправкой
            if df:
                await message.answer(df, parse_mode='HTML')
            result.clear()


    except Exception as e:
        await message.reply("Произошла ошибка при обработке данных.")
        print(f"Ошибка: {e}")
    await state.set_state(Form.waiting_for_action)



async def perform_action(message, state: FSMContext):
    user_data = await state.get_data()
    location_name = user_data.get('location_name')
    location_id = user_data.get('location_id')
    date = user_data.get('prev_date')
    next_data = user_data.get('next_date')
    if location_id is not None:
        try:
            if location_name == 'Чудо Юдо':
                ip_address = '10.10.16.52'
                username = 'sa'
                password = 'Tmp3573098'
            else:
                ip_address = 'HA-SQL-RK7-187.chaihona.local'
                username = 'rk7connectdb'
                password = 'mVYM4CMqkCnuIvAKPdWB'
            df = query.connectbd(location_id, ip_address, username, password, 'operation', date, end=next_data)
            if df.empty:
                await message.reply(f"Нет данных для локации '{location_name}'.")
            else:
                df_message = "".join(f"Дата: {row['Дата']:%Y-%m-%d %H:%M:%S}\n"
                                     f"Номер чека: {row['Номер чека']}\n"
                                     f"Менеджер: {row['Менеджер']}\n"
                                     f"Официант: {row['Официант']}\n"
                                     f"Сумма до: {row['Сумма до']:.2f}\n"
                                     f"Сумма после: {row['Сумма после']:.2f}\n"
                                     f"Разница: {row['Разница']:.2f}\n"
                                     "---------------------\n" for _, row in df.iterrows())
                await message.answer(f"Данные для {location_name}:\n<pre>{df_message}</pre>", parse_mode='HTML')
        except Exception as e:
            await message.reply(f"Ошибка при получении данных для {location_name}: {e}")

        finally:
            await state.set_state(Form.waiting_for_action)




# Асинхронная функция обработки данных после выбора даты
async def process_date_input(message: types.Message, user_data, state: FSMContext):
    selected_date = user_data.get('selected_date')
    location_name = user_data.get('location_name')
    location_id = user_data.get('location_id')
    date = selected_date.strftime("%d.%m.%Y")

    try:
        # Получение параметров для указанной локации или использование параметров по умолчанию
        config = LOCATION_CONFIG.get(location_name, LOCATION_CONFIG['default'])

        # Извлечение параметров из словаря
        ip_address = config['ip_address']
        username = config['username']
        password = config['password']

        # Подключение к базе данных и получение данных
        df = query.connectbd(location_id, ip_address, username, password, 'operation', date)

        # Проверка, пуст ли полученный DataFrame
        if df.empty:
            await message.answer(f"Нет данных для даты {date}.")
        else:
            df_message = ""
            for index, row in df.iterrows():
                if pd.notna(row['Тип валюты']):
                    if index > 0:
                        df_message += "\n\n"
                    df_message += (f"{row['Тип валюты']}\n"
                                   "---------------------\n")
                df_message += f"{row['Валюта']}: {row['Сумма']:.2f}\n"

            await message.answer(f"Данные для {location_name} на {date}:\n<pre><code>{df_message}</code></pre>",
                                 parse_mode='HTML')
    except Exception as e:
        await message.answer(f"Ошибка при получении данных для {date}: {e}")
    finally:
        # Возврат состояния в ожидание следующего действия
        await state.set_state(Form.waiting_for_action)


async def main():
    await dp.start_polling(bot)


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())
