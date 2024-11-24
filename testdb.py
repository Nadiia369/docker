import psycopg2
from prettytable import PrettyTable

# Підключення до PostgreSQL
def create_connection():
    try:
        connection = psycopg2.connect(
            dbname="shipping_company",
            user="postgres",
            password="password",
            host="192.168.0.3",
            port="5432"
        )
        return connection
    except psycopg2.Error as e:
        print(f"Помилка підключення до бази даних: {e}")
        return None

# Отримання списку таблиць
def get_tables(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
        """)
        tables = cursor.fetchall()
        return [table[0] for table in tables]

# Виведення структури таблиці
def show_table_structure(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute(f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name}';
        """)
        structure = cursor.fetchall()
        table = PrettyTable(["Column Name", "Data Type", "Is Nullable"])
        for column in structure:
            table.add_row(column)
        print(f"Структура таблиці '{table_name}':")
        print(table)

# Виведення даних з таблиці
def show_table_data(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        table = PrettyTable(column_names)
        for row in rows:
            table.add_row(row)
        print(f"Дані таблиці '{table_name}':")
        print(table)

# Виконання запитів
def execute_queries(connection):
    queries = {
        "Судна обраного типу": """
            SELECT * FROM Vessels WHERE vessel_type = 'Танкер';
        """,
        "Вартість кожного товару в кожній партії": """
            SELECT S.shipment_name, G.goods_name, G.price_per_unit * S.quantity AS total_price
            FROM Shipments S
            JOIN Goods G ON S.goods_code = G.goods_code;
        """,
        "Вартість кожної партії": """
            SELECT S.shipment_name, SUM(G.price_per_unit * S.quantity) AS total_party_price
            FROM Shipments S
            JOIN Goods G ON S.goods_code = G.goods_code
            GROUP BY S.shipment_name;
        """,
        "Кількість кожного типу товарів у кожній партії": """
            SELECT S.shipment_name, G.goods_type, COUNT(*) AS count_goods
            FROM Shipments S
            JOIN Goods G ON S.goods_code = G.goods_code
            GROUP BY S.shipment_name, G.goods_type;
        """,
        "Дата прибуття в порт": """
            SELECT D.dispatch_code, D.dispatch_date + D.delivery_days AS arrival_date
            FROM Dispatches D;
        """,
        "Товари типу 'Побутова техніка'": """
            SELECT * FROM Goods WHERE goods_type = 'Побутова техніка' ORDER BY goods_name;
        """
    }
    for query_name, query in queries.items():
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            table = PrettyTable(column_names)
            for row in rows:
                table.add_row(row)
            print(f"Запит: {query_name}")
            print(table)

# Головна функція
if __name__ == "__main__":
    conn = create_connection()
    if conn:
        print("Підключено до бази даних!")
        tables = get_tables(conn)
        print("\nСписок таблиць:")
        for table in tables:
            print(f"- {table}")
            show_table_structure(conn, table)
            show_table_data(conn, table)
        print("\nРезультати запитів:")
        execute_queries(conn)
        conn.close()