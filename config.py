API_TOKEN = '7949980557:AAGttixd9ida6QYunTs-sA4tFOfcKtHN3e8'

# Список локаций в виде словарей
LOCATIONS = [
    {"Rep59": "Авиапарк"}, {"Rep50": "Алтуфьево"}, {"Rep32": "Арбат"}, {"Rep33": "Бутлерова"},
    {"Rep53": "Бутово"}, {"Rep47": "Вернадского"}, {"Rep39": "Жуковка"}, {"Rep46": "Марьино"},
    {"Rep49": "Митино"}, {"Rep57": "Новокосино"}, {"Rep45": "Петровка"}, {"Rep12": "Полянка"},
    {"Rep51": "Сочи"}, {"Rep44": "Тверская"}, {"Rep37": "Тушино"}, {"Rep76": "МБД"}, {"Rep95": "Русалка"},
    {"RK7CHUDO": "Чудо Юдо"}, {"Rep56": "Эрмитаж"}
]


# Словарь, который содержит параметры для каждой локации
LOCATION_CONFIG = {
    'Чудо Юдо': {
        'ip_address': '10.10.16.52',
        'username': 'sa',
        'password': 'Tmp3573098',
        'classific': 3072
    },
    'default': {
        'ip_address': 'HA-SQL-RK7-187.chaihona.local',
        'username': 'rk7connectdb',
        'password': 'mVYM4CMqkCnuIvAKPdWB',
        'classific': 2560
    }
}