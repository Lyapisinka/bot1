from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta


def get_sales_dish(previous_date, current_date, classic):
    """Возвращает SQL-запрос для 'OperationLog'."""
    previous_date_str = datetime.strptime(previous_date, '%d.%m.%Y').strftime('%Y.%m.%d')
    current_date_str = datetime.strptime(current_date, '%d.%m.%Y').strftime('%Y.%m.%d')
    query = f"""
        SELECT
          CLASSIFICATORGROUPS0000.NAME AS "CATEGORY",
          MENUITEMS00."NAME" AS "DISH",
          SUM(PayBindings."QUANTITY") AS "QUANTITY",
          SUM(PayBindings."PRICESUM") AS "PRLISTSUM",
          SUM(PayBindings."PAYSUM") AS "PAYSUM"
        FROM "PayBindings"
        JOIN "CURRLINES" CurrLines00
          ON (CurrLines00."VISIT" = PayBindings."VISIT") 
          AND (CurrLines00."MIDSERVER" = PayBindings."MIDSERVER") 
          AND (CurrLines00."UNI" = PayBindings."CURRUNI")
        JOIN "PRINTCHECKS" PrintChecks00
          ON (PrintChecks00."VISIT" = CurrLines00."VISIT") 
          AND (PrintChecks00."MIDSERVER" = CurrLines00."MIDSERVER") 
          AND (PrintChecks00."UNI" = CurrLines00."CHECKUNI")
        JOIN "ORDERS" Orders00
          ON (Orders00."VISIT" = PayBindings."VISIT") 
          AND (Orders00."MIDSERVER" = PayBindings."MIDSERVER") 
          AND (Orders00."IDENTINVISIT" = PayBindings."ORDERIDENT")
        LEFT JOIN "SESSIONDISHES" SessionDishes00
          ON (SessionDishes00."VISIT" = PayBindings."VISIT") 
          AND (SessionDishes00."MIDSERVER" = PayBindings."MIDSERVER") 
          AND (SessionDishes00."UNI" = PayBindings."DISHUNI")
        LEFT JOIN "MENUITEMS" MENUITEMS00
          ON (MENUITEMS00."SIFR" = SessionDishes00."SIFR")
        LEFT JOIN DISHGROUPS DISHGROUPS0000
          ON (DISHGROUPS0000.CHILD = MENUITEMS00.SIFR) 
          AND (DISHGROUPS0000.CLASSIFICATION = {classic})
        LEFT JOIN CLASSIFICATORGROUPS CLASSIFICATORGROUPS0000
          ON CLASSIFICATORGROUPS0000.IDENT = DISHGROUPS0000.PARENT
        LEFT JOIN "GLOBALSHIFTS" GLOBALSHIFTS00
          ON (GLOBALSHIFTS00."MIDSERVER" = Orders00."MIDSERVER") 
          AND (GLOBALSHIFTS00."SHIFTNUM" = Orders00."ICOMMONSHIFT")
        LEFT JOIN trk7EnumsValues trk7EnumsValues3600
          ON (trk7EnumsValues3600.EnumData = GLOBALSHIFTS00."STATUS") 
          AND (trk7EnumsValues3600.EnumName = 'TRecordStatus')
        LEFT JOIN "CATEGLIST" CATEGLIST00
          ON (CATEGLIST00."SIFR" = MENUITEMS00."PARENT")
        LEFT JOIN DISHGROUPS DISHGROUPS0001
          ON (DISHGROUPS0001.CHILD = MENUITEMS00.SIFR) 
          AND (DISHGROUPS0001.CLASSIFICATION = 0)
        LEFT JOIN CLASSIFICATORGROUPS CLASSIFICATORGROUPS0001
          ON CLASSIFICATORGROUPS0001.IDENT = DISHGROUPS0001.PARENT
        LEFT JOIN "PAYMENTS" Payments00
          ON (Payments00."VISIT" = CurrLines00."VISIT") 
          AND (Payments00."MIDSERVER" = CurrLines00."MIDSERVER") 
          AND (Payments00."UNI" = CurrLines00."PAYUNIFOROWNERINFO")
        LEFT JOIN "CURRENCIES" CURRENCIES01
          ON (CURRENCIES01."SIFR" = Payments00."SIFR")
        WHERE
          (PrintChecks00."STATE" = 6)
          AND (PrintChecks00."IGNOREINREP" = 0)
          AND (GLOBALSHIFTS00."STATUS" = 3)
          AND GLOBALSHIFTS00.SHIFTDATE BETWEEN CONVERT(datetime, '{previous_date_str}', 102) AND CONVERT(datetime, '{current_date_str}')
        GROUP BY CLASSIFICATORGROUPS0000.NAME, MENUITEMS00."NAME"
        ORDER BY "CATEGORY", DISH
    """
    return query

def get_check(date):
    date_converted = datetime.strptime(date, '%d.%m.%Y').strftime('%Y.%m.%d')
    query = text(f"""
                SELECT
                  CURRENCIES.NAME AS "CURRENCY",
                  PrintChecks."CHECKNUM" AS "CHECKNUM",
                  PrintChecks."BINDEDSUM" AS "BINDEDSUM",
                  FORMAT(PRINTCHECKS.CLOSEDATETIME, 'HH:mm') AS CLOSEDATETIME
                FROM "PrintChecks"
                LEFT JOIN "ORDERS" ON Orders."VISIT" = PrintChecks."VISIT" AND Orders."MIDSERVER" = PrintChecks."MIDSERVER" AND Orders."IDENTINVISIT" = PrintChecks."ORDERIDENT"
                LEFT JOIN "GLOBALSHIFTS" ON GLOBALSHIFTS."MIDSERVER" = Orders."MIDSERVER" AND GLOBALSHIFTS."SHIFTNUM" = Orders."ICOMMONSHIFT"
                LEFT JOIN "TABLES" ON TABLES."SIFR" = Orders."TABLEID"
                LEFT JOIN "EMPLOYEES" ON EMPLOYEES."SIFR" = Orders."MAINWAITER"
                LEFT JOIN "CURRLINES" CurrLines00 ON CurrLines00."VISIT" = PRINTCHECKS."VISIT"
                LEFT JOIN "CURRENCIES" ON CURRENCIES.SIFR = CurrLines00.SIFR
                WHERE
                  PrintChecks."STATE" = 6
                  AND PrintChecks."IGNOREINREP" = 0
                  AND GLOBALSHIFTS."SHIFTDATE" = CONVERT(DATETIME, '{date_converted}', 102)
                  AND GLOBALSHIFTS."STATUS" = 3
                ORDER BY "CURRENCY", CLOSEDATETIME
    """)
    return query

def get_discounted_check_details(date):
    # Устанавливаем подключение к базе данных

    date_converted = datetime.strptime(date, '%d.%m.%Y').strftime('%Y.%m.%d')
    # SQL-запрос с параметризованной датой
    query = f"""
        WITH RankedDiscounts AS (
            SELECT
                DISCOUNTS00.NAME,
                CHECKNUM,
                CURRENCIES.NAME AS CURRENCY_NAME,
                CARDCODE,
                HOLDER,
                PrintChecks00.PRLISTSUM as PR,
                PrintChecks00.DISCOUNTSUM AS DI,
                PrintChecks00.NATIONALSUM AS NI,
                FORMAT(SHIFTDATE, 'dd.MM.yyyy') AS ShiftDate,
                ROW_NUMBER() OVER (PARTITION BY DISCOUNTS00.NAME ORDER BY CHECKNUM) AS RowNum
            FROM "DISHDISCOUNTS"
            LEFT JOIN "DISCOUNTS" DISCOUNTS00
              ON (DISCOUNTS00."SIFR" = "DISHDISCOUNTS"."SIFR")
            JOIN "CURRLINES" CurrLines00
              ON (CurrLines00."VISIT" = "DISHDISCOUNTS"."VISIT") AND (CurrLines00."MIDSERVER" = "DISHDISCOUNTS"."MIDSERVER")
            LEFT JOIN CURRENCIES
              ON (CURRENCIES.SIFR = CurrLines00.SIFR)
            JOIN "PRINTCHECKS" PrintChecks00
              ON (PrintChecks00."VISIT" = CurrLines00."VISIT") AND (PrintChecks00."MIDSERVER" = CurrLines00."MIDSERVER") AND (PrintChecks00."UNI" = CurrLines00."CHECKUNI")
            LEFT JOIN SHIFTS SHIFTS00
              ON (SHIFTS00."MIDSERVER" = PrintChecks00."MIDSERVER") AND (SHIFTS00."SHIFTNUM" = PrintChecks00."ISHIFT")
			LEFT JOIN GLOBALSHIFTS
			  ON (GLOBALSHIFTS.SHIFTNUM = SHIFTS00.ICOMMONSHIFT)
            WHERE
              PrintChecks00."STATE" = 6 AND GLOBALSHIFTS.SHIFTDATE = CONVERT(datetime, '{date_converted}', 102)
        )
        SELECT
            CASE WHEN RowNum = 1 THEN NAME ELSE NULL END AS NAME,
            CHECKNUM,
            CURRENCY_NAME,
            CARDCODE,
            HOLDER,
            PR,
            DI,
            NI,
            ShiftDate
        FROM RankedDiscounts    """
    return query

def get_query_for_operations(previous_date, current_date):
    """Возвращает SQL-запрос для 'OperationLog'."""
    previous_date_str = datetime.strptime(previous_date, '%d.%m.%Y').strftime('%Y.%m.%d')
    current_date_str = datetime.strptime(current_date, '%d.%m.%Y').strftime('%Y.%m.%d')
    query = ("SELECT OP.[DATETIME] AS 'Дата', PRI.CHECKNUM AS 'Номер чека', "
             "EM.NAME AS 'Менеджер', EMP.NAME AS 'Официант', "
             "OP.ORDERSUMBEFORE AS 'Сумма до', ORD.PAIDSUM AS 'Сумма после', "
             "(ORD.PAIDSUM - OP.ORDERSUMBEFORE) AS 'Разница', 1 AS 'Кол-во' "
             "FROM OPERATIONLOG OP "
             "JOIN EMPLOYEES EM ON OP.OPERATOR = EM.SIFR "
             "JOIN ORDERS ORD ON OP.VISIT = ORD.VISIT "
             "JOIN EMPLOYEES EMP ON ORD.ICREATOR = EMP.SIFR "
             "JOIN CASHGROUPS CASH ON OP.MIDSERVER = CASH.SIFR "
             "JOIN RESTAURANTS REST ON CASH.RESTAURANT = REST.SIFR "
             "JOIN PRINTCHECKS PRI ON ORD.VISIT = PRI.VISIT "
             f"WHERE [DATETIME] BETWEEN CONVERT(datetime, '{previous_date_str}', 102) AND CONVERT(datetime, '{current_date_str}', 102) AND "
             "OPERATION = '463' AND PRI.[STATE] = 7 AND OP.PARAMETER = PRI.CHECKNUM "
             "ORDER BY REST.NAME, OP.[DATETIME]")
    return query


def get_query_for_payments(date):
    """Возвращает SQL-запрос для агрегации 'Payments' с использованием CTE."""
    # Преобразуем дату из формата 'дд.мм.гггг' в 'гггг.мм.дд'
    date_converted = datetime.strptime(date, '%d.%m.%Y').strftime('%Y.%m.%d')
    query = (
        "WITH CurrencyData AS ("
        "    SELECT "
        "        CURRENCYTYPES00.\"NAME\" AS \"Тип валюты\", "
        "        CURRENCIES00.\"NAME\" AS \"Валюта\", "
        "        SUM(Payments.\"BASICSUM\") AS \"Сумма\", "
        "        ROW_NUMBER() OVER(PARTITION BY CURRENCYTYPES00.\"NAME\" ORDER BY CURRENCIES00.\"NAME\") AS rn "
        "    FROM Payments "
        "    JOIN ORDERSESSIONS OrderSessions00 "
        "      ON (OrderSessions00.\"VISIT\" = Payments.\"VISIT\") "
        "      AND (OrderSessions00.\"MIDSERVER\" = Payments.\"MIDSERVER\") "
        "      AND (OrderSessions00.\"UNI\" = Payments.\"SESSIONUNI\") "
        "    JOIN ORDERS Orders00 "
        "      ON (Orders00.\"VISIT\" = OrderSessions00.\"VISIT\") "
        "      AND (Orders00.\"MIDSERVER\" = OrderSessions00.\"MIDSERVER\") "
        "      AND (Orders00.\"IDENTINVISIT\" = OrderSessions00.\"ORDERIDENT\") "
        "    LEFT JOIN CURRENCIES CURRENCIES00 "
        "      ON (CURRENCIES00.\"SIFR\" = Payments.\"SIFR\") "
        "    LEFT JOIN CURRENCYTYPES CURRENCYTYPES00 "
        "      ON (CURRENCYTYPES00.\"SIFR\" = CURRENCIES00.\"PARENT\") "
        "    JOIN GLOBALSHIFTS GLOBALSHIFTS00 "
        "      ON (GLOBALSHIFTS00.\"MIDSERVER\" = Orders00.\"MIDSERVER\") "
        "      AND (GLOBALSHIFTS00.\"SHIFTNUM\" = Orders00.\"ICOMMONSHIFT\") "
        "    WHERE "
        "      Payments.\"IGNOREINREP\" = 0 "
        "      AND Payments.\"STATE\" = 6 "
        f"      AND GLOBALSHIFTS00.\"SHIFTDATE\" = CONVERT(DATETIME, '{date_converted}', 102) "
        "      AND GLOBALSHIFTS00.\"STATUS\" = 3 "
        "      AND Payments.\"SHOWINREP\" BETWEEN 0 AND 2 "
        "    GROUP BY "
        "      CURRENCYTYPES00.\"NAME\", "
        "      CURRENCIES00.\"NAME\" "
        ") "
        "SELECT "
        "    CASE WHEN rn = 1 THEN \"Тип валюты\" END AS \"Тип валюты\", "
        "    \"Валюта\", "
        "    \"Сумма\" "
        "FROM CurrencyData "
    )
    return query


def get_order_data_query(check):
    return f"""
        SELECT TOP 1
            FORMAT(SHIFTDATE, 'dd.MM.yyyy') AS ShiftDate,
            GUESTSCOUNT, 
            TABLENAME, 
            EMPLOYEES.NAME AS EmployeeName, 
            PRICELISTSUM, 
            PAIDSUM, 
            DISCOUNTSUM
        FROM ORDERS
        LEFT JOIN EMPLOYEES ON ORDERS.MAINWAITER = EMPLOYEES.SIFR
        LEFT JOIN GLOBALSHIFTS ON ORDERS.ICOMMONSHIFT = GLOBALSHIFTS.SHIFTNUM
        WHERE ORDERS.VISIT = (SELECT MAX(PRINTCHECKS.VISIT) FROM PRINTCHECKS WHERE CHECKNUM = {check})
        ORDER BY SHIFTDATE DESC
    """

def get_session_dishes_data_query(check):
    return f"""
        WITH NumberedDishes AS (
            SELECT 
                REPLACE(MENUITEMS.NAME, '<', '&lt;') AS MenuItemName, 
                SESSIONDISHES.QUANTITY AS Quantity, 
                SESSIONDISHES.PRLISTSUM AS PRListSum,
                SESSIONDISHES.ISCOMBOCOMP,
                REPLACE(DISHMODIFIERS.OPENNAME, '<', '&lt;') AS ModifierOpenName,
                DISHMODIFIERS.PIECES AS ModifierPieces,
                ORDERVOIDS.NAME AS VoidName,
                LAG(REPLACE(MENUITEMS.NAME, '<', '&lt;')) OVER (ORDER BY SESSIONDISHES.UNI) AS PrevMenuItemName,
                LAG(SESSIONDISHES.QUANTITY) OVER (ORDER BY SESSIONDISHES.UNI) AS PrevMenuItemQuantity,
                LAG(REPLACE(DISHMODIFIERS.OPENNAME, '<', '&lt;')) OVER (ORDER BY SESSIONDISHES.UNI) AS PrevModifierOpenName
            FROM SESSIONDISHES
            LEFT JOIN MENUITEMS ON SESSIONDISHES.SIFR = MENUITEMS.SIFR
            LEFT JOIN DISHMODIFIERS ON SESSIONDISHES.UNI = DISHMODIFIERS.DISHUNI AND SESSIONDISHES.VISIT = DISHMODIFIERS.VISIT AND DISHMODIFIERS.COMBODISHUNI = 0
            LEFT JOIN DISHVOIDS ON SESSIONDISHES.UNI = DISHVOIDS.DISHUNI AND SESSIONDISHES.VISIT = DISHVOIDS.VISIT
            LEFT JOIN ORDERVOIDS ON DISHVOIDS.SIFR = ORDERVOIDS.SIFR
            WHERE SESSIONDISHES.VISIT = (SELECT MAX(VISIT) FROM PRINTCHECKS WHERE CHECKNUM = {check})
        )
        SELECT 
            CASE WHEN MenuItemName = PrevMenuItemName and Quantity = PrevMenuItemQuantity THEN NULL ELSE MenuItemName END AS DisplayName,
            Quantity,
            PRListSum,
            ISCOMBOCOMP,
            CASE WHEN ModifierOpenName = PrevModifierOpenName THEN NULL ELSE ModifierOpenName END AS DisplayModifierOpenName,
            ModifierPieces,
            VoidName
        FROM NumberedDishes;
    """

def get_payment_data_query(check):
    return f"""
        SELECT 
            CURRENCIES.NAME AS CurrencyName, 
            EMPLOYEES.NAME AS EmployeeName, 
            PAYMENTS.NATIONALSUM AS PaymentNationalSum
        FROM PAYMENTS
        LEFT JOIN CURRENCIES ON PAYMENTS.SIFR = CURRENCIES.SIFR
        LEFT JOIN EMPLOYEES ON PAYMENTS.IAUTHOR = EMPLOYEES.SIFR
        WHERE PAYMENTS.VISIT = (SELECT MAX(VISIT) FROM PRINTCHECKS WHERE CHECKNUM = {check})
    """

def get_discount_data_query(check):
    return f"""
        SELECT 
            DISCOUNTS.NAME AS DiscountName, 
            DISHDISCOUNTS.CALCAMOUNT AS DiscountAmount, 
            DISHDISCOUNTS.HOLDER AS Holder
        FROM DISHDISCOUNTS
        LEFT JOIN DISCOUNTS ON DISHDISCOUNTS.SIFR = DISCOUNTS.SIFR
        WHERE DISHDISCOUNTS.VISIT = (SELECT MAX(VISIT) FROM PRINTCHECKS WHERE CHECKNUM = {check})
    """


def connectbd(database_id, ip_address, username, password, query_type, date=None, end=None, check=None, classic=None):
    """Подключается к базе данных и возвращает данные в виде DataFrame."""
    # Формируем строку подключения с использованием SQLAlchemy и указанной базы данных
    connection_string = (f"mssql+pyodbc://"
                         f"{username}:{password}@{ip_address}/{database_id}?"
                         f"driver=ODBC+Driver+18+for+SQL+Server"
                         f"&TrustServerCertificate=yes")

    # Создаем движок SQLAlchemy
    engine = create_engine(connection_string)

    # Выбор запроса в зависимости от query_type
    if query_type == 'operation':
        query = get_query_for_operations(date, end)
    elif query_type == 'payment':
        query = get_query_for_payments(date)
    elif query_type == 'order':
        query = get_order_data_query(check)
    elif query_type == 'session':
        query = get_session_dishes_data_query(check)
    elif query_type == 'payment_data':
        query = get_payment_data_query(check)
    elif query_type == 'discount':
        query = get_discount_data_query(check)
    elif query_type == 'discount_data':
        query = get_discounted_check_details(date)
    elif query_type == 'get_check':
        query = get_check(date)
    elif query_type == 'sales_dish':
        query = get_sales_dish(date, end, classic)

    else:
        raise ValueError("Invalid query type specified")

    # Используем движок для выполнения запроса
    df = pd.read_sql(query, engine)

    return df