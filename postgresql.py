import psycopg2
from faker import Faker
import random
from datetime import timedelta

# Підключення до PostgreSQL
def create_server_connection():
    try:
        connection = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="password",
            host="192.168.0.3",
            port="5432"
        )
        connection.autocommit = True
        return connection
    except psycopg2.Error as e:
        print(f"Помилка підключення до сервера бази даних: {e}")
        return None

# Створення бази даних
def create_database():
    connection = create_server_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_database WHERE datname='shipping_company';")
                exists = cursor.fetchone()
                if not exists:
                    cursor.execute("CREATE DATABASE shipping_company;")
                    print("Базу даних 'shipping_company' створено успішно.")
                else:
                    print("База даних 'shipping_company' вже існує.")
        except psycopg2.Error as e:
            print(f"Помилка при створенні бази даних: {e}")
        finally:
            connection.close()

# Підключення до створеної бази даних
def create_connection():
    try:
        connection = psycopg2.connect(
            dbname="shipping_company",
            user="postgres",
            password="password",
            host="192.168.0.3",
            port="5432"
        )
        connection.autocommit = True
        return connection
    except psycopg2.Error as e:
        print(f"Помилка підключення до бази даних: {e}")
        return None

# Створення таблиць
def create_tables(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Vessels (
            registration_number SERIAL PRIMARY KEY,
            vessel_name VARCHAR(100) NOT NULL,
            captain_name VARCHAR(100) NOT NULL,
            vessel_type VARCHAR(50) NOT NULL CHECK (vessel_type IN ('Танкер', 'Суховантажний')),
            capacity INTEGER NOT NULL CHECK (capacity > 0),
            year_built INTEGER NOT NULL CHECK (year_built BETWEEN 1900 AND EXTRACT(YEAR FROM CURRENT_DATE))
        );

        CREATE TABLE IF NOT EXISTS Ports (
            port_number SERIAL PRIMARY KEY,
            port_name VARCHAR(100) NOT NULL,
            country VARCHAR(50) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Goods (
            goods_code SERIAL PRIMARY KEY,
            goods_name VARCHAR(100) NOT NULL,
            goods_type VARCHAR(50) NOT NULL CHECK (goods_type IN ('Пально-мастильні', 'Суміші', 'Побутова техніка', 'Великогабаритний вантаж')),
            unit VARCHAR(20) NOT NULL,
            price_per_unit NUMERIC(10, 2) NOT NULL CHECK (price_per_unit > 0),
            requires_customs BOOLEAN NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Shipments (
            shipment_number SERIAL PRIMARY KEY,
            shipment_name VARCHAR(100) NOT NULL,
            goods_code INTEGER NOT NULL REFERENCES Goods(goods_code),
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            destination_port_number INTEGER NOT NULL REFERENCES Ports(port_number)
        );

        CREATE TABLE IF NOT EXISTS Dispatches (
            dispatch_code SERIAL PRIMARY KEY,
            shipment_number INTEGER NOT NULL REFERENCES Shipments(shipment_number),
            dispatch_date DATE NOT NULL,
            delivery_days INTEGER NOT NULL CHECK (delivery_days > 0),
            vessel_number INTEGER NOT NULL REFERENCES Vessels(registration_number)
        );
        """)
        print("Таблиці створено успішно.")

# Заповнення таблиць
def populate_tables(connection):
    fake = Faker()
    with connection.cursor() as cursor:
        # Заповнення суден
        vessel_types = ['Танкер', 'Суховантажний']
        for _ in range(3):  # 3 судна
            cursor.execute("""
            INSERT INTO Vessels (vessel_name, captain_name, vessel_type, capacity, year_built)
            VALUES (%s, %s, %s, %s, %s);
            """, (
                fake.company(),
                fake.name(),
                random.choice(vessel_types),
                random.randint(5000, 100000),
                random.randint(1980, 2023)
            ))

        # Заповнення портів
        for _ in range(5):  # 5 портів
            cursor.execute("""
            INSERT INTO Ports (port_name, country)
            VALUES (%s, %s);
            """, (
                fake.city(),
                fake.country()
            ))

        # Заповнення товарів
        goods_types = ['Пально-мастильні', 'Суміші', 'Побутова техніка', 'Великогабаритний вантаж']
        units = ['кг', 'л', 'шт']
        for _ in range(15):  # 15 товарів
            cursor.execute("""
            INSERT INTO Goods (goods_name, goods_type, unit, price_per_unit, requires_customs)
            VALUES (%s, %s, %s, %s, %s);
            """, (
                fake.word(),
                random.choice(goods_types),
                random.choice(units),
                round(random.uniform(10, 1000), 2),
                fake.boolean(chance_of_getting_true=50)
            ))

        # Заповнення партій
        for _ in range(5):  # 5 партій
            cursor.execute("""
            INSERT INTO Shipments (shipment_name, goods_code, quantity, destination_port_number)
            VALUES (%s, %s, %s, %s);
            """, (
                fake.word(),
                random.randint(1, 15),
                random.randint(10, 1000),
                random.randint(1, 5)
            ))

        # Заповнення відправок
        for _ in range(5):  # 5 відправок
            cursor.execute("""
            INSERT INTO Dispatches (shipment_number, dispatch_date, delivery_days, vessel_number)
            VALUES (%s, %s, %s, %s);
            """, (
                random.randint(1, 5),
                fake.date_this_year(),
                random.randint(1, 30),
                random.randint(1, 3)
            ))

        print("Таблиці заповнено даними.")

# Запити
def execute_queries(connection):
    with connection.cursor() as cursor:
        # Запит 1: Всі судна обраного типу
        vessel_type = 'Танкер'
        cursor.execute("""
        SELECT * FROM Vessels WHERE vessel_type = %s;
        """, (vessel_type,))
        print("Судна обраного типу:", cursor.fetchall())

        # Запит 2: Вартість кожного товару в кожній партії
        cursor.execute("""
        SELECT S.shipment_name, G.goods_name, G.price_per_unit * S.quantity AS total_price
        FROM Shipments S
        JOIN Goods G ON S.goods_code = G.goods_code;
        """)
        print("Вартість кожного товару в партії:", cursor.fetchall())

        # Запит 3: Вартість кожної партії товарів
        cursor.execute("""
        SELECT S.shipment_name, SUM(G.price_per_unit * S.quantity) AS total_party_price
        FROM Shipments S
        JOIN Goods G ON S.goods_code = G.goods_code
        GROUP BY S.shipment_name;
        """)
        print("Вартість кожної партії:", cursor.fetchall())

        # Запит 4: Кількість кожного типу товарів у кожній партії
        cursor.execute("""
        SELECT S.shipment_name, G.goods_type, COUNT(*) AS count_goods
        FROM Shipments S
        JOIN Goods G ON S.goods_code = G.goods_code
        GROUP BY S.shipment_name, G.goods_type;
        """)
        print("Кількість кожного типу товарів у партії:", cursor.fetchall())

        # Запит 5: Дата прибуття в порт призначення
        cursor.execute("""
        SELECT D.dispatch_code, D.dispatch_date + D.delivery_days AS arrival_date
        FROM Dispatches D;
        """)
        print("Дата прибуття в порт:", cursor.fetchall())

        # Запит 6: Всі товари типу "Побутова техніка"
        cursor.execute("""
        SELECT * FROM Goods WHERE goods_type = 'Побутова техніка' ORDER BY goods_name;
        """)
        print("Товари типу 'Побутова техніка':", cursor.fetchall())

if __name__ == "__main__":
    create_database()
    conn = create_connection()
    if conn:
        create_tables(conn)
        populate_tables(conn)
        execute_queries(conn)
        conn.close()