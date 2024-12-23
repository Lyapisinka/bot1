async def fetch_user_data(state):
    """Извлекает данные пользователя из состояния."""
    user_data = await state.get_data()
    location_name = user_data.get('location_name')
    location_id = user_data.get('location_id')
    date = user_data.get('prev_date').strftime("%d.%m.%Y")
    next_data = user_data.get('next_date').strftime("%d.%m.%Y")
    return location_name, location_id, date, next_data


async def fetch_data(action_type, state, check_number=None):
    """Подключение к базе данных и получение данных на основе типа действия."""
    try:
        # Извлечение данных пользователя из состояния
        location_name, location_id, date, next_data = await fetch_user_data(state)

        # Получение параметров для указанной локации или использование параметров по умолчанию
        config = location_config.get(location_name, location_config['default'])

        # Извлечение параметров из словаря
        ip_address = config['ip_address']
        username = config['username']
        password = config['password']
        classific = config['classific']

        # Подключение на основе действия
        if action_type == 'operation':
            df = connect.connectbd(location_id, ip_address, username, password, 'operation', date, end=next_data,
                                   classic=classific)
        elif action_type == 'discount_data':
            df = connect.connectbd(location_id, ip_address, username, password, 'discount_data', date,
                                   classic=classific)
        elif action_type == 'payment':
            df = connect.connectbd(location_id, ip_address, username, password, 'payment', date, classic=classific)
        elif action_type == 'order':
            df = connect.connectbd(location_id, ip_address, username, password, 'order', check=check_number,
                                   classic=classific)
        elif action_type == 'session':
            df = connect.connectbd(location_id, ip_address, username, password, 'session', check=check_number,
                                   classic=classific)
        elif action_type == 'payment_data':
            df = connect.connectbd(location_id, ip_address, username, password, 'payment_data', check=check_number,
                                   classic=classific)
        elif action_type == 'discount':
            df = connect.connectbd(location_id, ip_address, username, password, 'discount', check=check_number,
                                   classic=classific)
        else:
            raise ValueError(f"Неизвестный тип действия: {action_type}")

        # Проверка, пуст ли полученный DataFrame
        if df.empty:
            await message.answer(f"Нет данных для действия '{action_type}' в локации '{location_name}'.")

    except Exception as e:
        # Обработка возможных исключений
        print(f"Произошла ошибка: {e}")


# # Примеры вызова функции для разных действий
# await fetch_data('operation', state)
# await fetch_data('discount_data', state)
# await fetch_data('payment', state)
# await fetch_data('order', state, check_number=check_number)
# await fetch_data('session', state, check_number=check_number)
# await fetch_data('payment_data', state, check_number=check_number)
# await fetch_data('discount', state, check_number=check_number)