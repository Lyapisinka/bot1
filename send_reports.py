from aiogram import types
from aiogram.fsm.context import FSMContext
import query
from config import LOCATION_CONFIG
from states import Form
import pandas as pd


async def fetch_data(location_id, ip_address, username, password, check_number):
    query_types = ['order', 'session', 'payment_data', 'discount']
    return {query_type: query.connectbd(location_id, ip_address, username, password, query_type, check=check_number)
            for query_type in query_types}


def get_connection_details(location_name):
    config = LOCATION_CONFIG.get(location_name, LOCATION_CONFIG['default'])
    return config['ip_address'], config['username'], config['password']


async def extract_user_data(state: FSMContext, include_dates: str = None):
    user_data = await state.get_data()
    location_name = user_data.get('location_name')
    location_id = user_data.get('location_id')

    if include_dates == 'single':
        date = user_data.get('selected_date').strftime('%Y.%m.%d')
        return location_name, location_id, date
    elif include_dates == 'range':
        date = user_data.get('prev_date').strftime('%Y.%m.%d')
        next_data = user_data.get('next_date').strftime('%Y.%m.%d')
        return location_name, location_id, date, next_data

    return location_name, location_id


async def send_messages_in_chunks(message, message_lines, chunk_size=2000):
    current_message = ""

    for line in message_lines:
        if len(current_message) + len(line) + 1 < chunk_size:
            current_message += line + "\n"
        else:
            await message.answer(f"{current_message}", parse_mode='HTML')
            current_message = line + "\n"

    if current_message:
        await message.answer(f"{current_message}", parse_mode='HTML')


# отмена пречеков
async def show_cancel_precheck(message, state: FSMContext):
    """Обрабатывает отмену пречеков."""
    location_name, location_id, date, next_data = await extract_user_data(state, include_dates='range')

    if not location_id:
        await message.reply("Локация не указана или неверна. Пожалуйста, выберите локацию.")
        return

    try:
        ip_address, username, password = get_connection_details(location_name)
        df = query.connectbd(location_id, ip_address, username, password, 'operation', date=date, end=next_data)

        if df.empty:
            await message.reply(f"Нет данных для локации '{location_name}'.")
        else:
            df_message = "\n".join(
                f"Дата: {row['Дата']:%Y-%m-%d %H:%M:%S}\n"
                f"Номер чека: {row['Номер чека']}\n"
                f"Менеджер: {row['Менеджер']}\n"
                f"Официант: {row['Официант']}\n"
                f"Сумма до: {row['Сумма до']:.2f}\n"
                f"Сумма после: {row['Сумма после']:.2f}\n"
                f"Разница: {row['Разница']:.2f}\n"
                "---------------------"
                for _, row in df.iterrows()
            )
            await message.answer(f"Данные для {location_name}:\n<pre>{df_message}</pre>", parse_mode='HTML')
    except Exception as e:
        await message.reply(f"Ошибка при получении данных для {location_name}: {e}")
    finally:
        await state.set_state(Form.waiting_for_action)


async def show_category_dish(message: types.Message, state: FSMContext):
    location_name, location_id, date, next_data = await extract_user_data(state, include_dates='range')

    try:
        ip_address, username, password = get_connection_details(location_name)
        classific = LOCATION_CONFIG.get(location_name, LOCATION_CONFIG['default'])['classific']

        df = query.connectbd(location_id, ip_address, username, password, 'sales_dish', date, end=next_data,
                             classic=classific)

        if df.empty:
            await message.reply(f"Нет данных для локации '{location_name}'.")
            return

        message_lines = []
        current_category = None

        for _, row in df.iterrows():
            if current_category != row['CATEGORY']:
                if current_category is not None:
                    message_lines.append("\n")
                current_category = row['CATEGORY']
                message_lines.append(f"<b>Категория: {current_category}</b>\n")

            current_line = (
                f"<pre><code>{row['QUANTITY']}   {row['DISH']} "
                f"Суммы: {row['PRLISTSUM']:.2f}  {row['PAYSUM']:.2f} "
                f"{row['PRLISTSUM'] - row['PAYSUM']:.2f}</code></pre>"
            )

            message_lines.append(current_line)
        await send_messages_in_chunks(message, message_lines)

    except Exception as e:
        await message.reply(f"Ошибка при получении данных для {location_name}: {e}")

    finally:
        await state.set_state(Form.waiting_for_action)


# чек данные
async def show_detail_check(message: types.Message, state: FSMContext):
    location_name, location_id = await extract_user_data(state)
    check_number = message.text.lstrip('/')
    await message.reply(f"Ищем данные по номеру {check_number}")

    try:
        ip_address, username, password = get_connection_details(location_name)
        data = await fetch_data(location_id, ip_address, username, password, check_number)

        if data['order'].empty:
            await message.reply(f"Нет данных для локации '{location_name}'.")
        else:
            total_width = 49
            message_lines = [f"Номер чека: {check_number}"]

            for index, row in data['order'].iterrows():
                message_lines.append(f"Дата смены: {row['ShiftDate']}")
                message_lines.append(f"Стол: {row['TABLENAME']}")
                message_lines.append(f"Кол-во гостей: {row['GUESTSCOUNT']}")
                message_lines.append(f"Официант: {row['EmployeeName']}")
            for index, row in data['payment_data'].iterrows():
                if not f"Кассир: {row['EmployeeName']}" in message_lines:
                    message_lines.append(f"Кассир: {row['EmployeeName']}")
            for index, row in data['order'].iterrows():
                message_lines.append(f"Общая сумма: {row['PRICELISTSUM']}")
            message_lines.append("\nБлюда:")
            for index, row in data['session'].iterrows():
                if row['ISCOMBOCOMP'] == 0 and row['DisplayName']:
                    quantity_str = f"{row['Quantity']}"
                    if quantity_str == "0.0":
                        menu_item_str = f"\"{row['DisplayName']}\""
                    else:
                        menu_item_str = f"{row['DisplayName']}"
                    combined_str = f"{quantity_str}  {menu_item_str} "
                    pr_list_sum_str = f"{row['PRListSum']}"
                    remaining_space = (total_width - len(combined_str) - len(pr_list_sum_str))
                    right_aligned_line = combined_str + ' ' * remaining_space + pr_list_sum_str
                    message_lines.append(right_aligned_line)
                if row['ISCOMBOCOMP'] == 1:
                    message_lines.append(f"     {row['Quantity']}  комбо: \"{row['DisplayName']}\"")
                if row['DisplayModifierOpenName']:
                    message_lines.append(f"     {row['ModifierPieces']}  \"{row['DisplayModifierOpenName']}\"")
                if row['VoidName']:
                    message_lines.append(f"     \"{row['VoidName']}\"")

            if not data['discount'].empty:
                message_lines.append("\nСкидки:")
                for index, row in data['discount'].iterrows():
                    discount_name = row['DiscountName']
                    discount_sum = str(abs(row['DiscountAmount']))
                    remaining_space = (total_width - len(discount_name) - len(discount_sum))
                    right_aligned_line = discount_name + ' ' * remaining_space + discount_sum
                    message_lines.append(right_aligned_line)
                    if row['Holder']:
                        message_lines.append(f"Карта: {row['Holder']}")

            message_lines.append("\nСпособы оплаты:")
            for index, row in data['payment_data'].iterrows():
                remaining_space = (total_width - len(row['CurrencyName']) - len(str(row['PaymentNationalSum'])))
                right_aligned_line = row['CurrencyName'] + ' ' * remaining_space + str(row['PaymentNationalSum'])
                message_lines.append(right_aligned_line)
            message_lines = ["<pre>"] + message_lines + ["</pre>"]
            await send_messages_in_chunks(message, message_lines)

    except Exception as e:
        await message.reply(f"Ошибка при получении данных для {location_name}: {e}")

    finally:
        await state.set_state(Form.waiting_for_action)


async def show_all_check(message, state: FSMContext):
    location_name, location_id, date = await extract_user_data(state, include_dates='single')

    try:
        ip_address, username, password = get_connection_details(location_name)
        df = query.connectbd(location_id, ip_address, username, password, 'get_check', date)

        if df.empty:
            await message.reply(f"Нет данных для даты {date}.")
            return

        message_lines = []
        current_currency = ""

        for _, row in df.iterrows():
            if row['CURRENCY'] != current_currency:
                message_lines.append(f"\n<b>{row['CURRENCY']}</b>\n")
                current_currency = row['CURRENCY']
            message_lines.append(f"<i>{row['CLOSEDATETIME']}</i>  /{row['CHECKNUM']}  Сумма:  {row['BINDEDSUM']}")
        await send_messages_in_chunks(message, message_lines)

    except Exception as e:
        await message.reply(f"Ошибка при получении данных для {location_name}: {e}")

    finally:
        await state.set_state(Form.waiting_for_action)


async def show_discounts(message, state: FSMContext):
    location_name, location_id, date = await extract_user_data(state, include_dates='single')

    try:
        ip_address, username, password = get_connection_details(location_name)
        df = query.connectbd(location_id, ip_address, username, password, 'discount_data', date)

        if df.empty:
            await message.reply(f"Нет данных для локации '{location_name}' за указанную дату.")
            return

        message_lines = []
        current_discount = None

        for _, row in df.iterrows():
            if row['NAME'] and row['NAME'] != current_discount:
                current_discount = row['NAME']
                message_lines.append(f"<b>Скидка: {current_discount}</b>")

            message_lines.append(f"Чек: /{row['CHECKNUM']}")
            code_lines = [
                f"<pre><code>{row['ShiftDate']}   {row['CURRENCY_NAME']}",
                f"{str(row['PR']).ljust(10)} {str(row['DI']).ljust(10)} {row['NI']}"
            ]

            if row['CARDCODE']:
                code_lines.append(f"{row['HOLDER']} {row['CARDCODE']}")

            code_lines.append("</code></pre>")
            message_lines.extend(code_lines)

        await send_messages_in_chunks(message, message_lines)

    except Exception as e:
        await message.reply(f"Ошибка при получении данных для {location_name}: {e}")

    finally:
        await state.set_state(Form.waiting_for_action)


async def show_total_reciepts(message: types.Message, state: FSMContext):
    location_name, location_id, date = await extract_user_data(state, include_dates='single')
    try:
        ip_address, username, password = get_connection_details(location_name)
        df = query.connectbd(location_id, ip_address, username, password, 'payment', date=date)

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
        await state.set_state(Form.waiting_for_action)
