"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv
import os


# Пути к csv-файлам.
PATH_EMPLOYEES_DATA = os.path.join("north_data", "employees_data.csv")
PATH_CUSTOMERS_DATA = os.path.join("north_data", "customers_data.csv")
PATH_ORDERS_DATA = os.path.join("north_data", "orders_data.csv")

# Переменные для работы с базой данных
USERNAME = os.getenv("USERNAME_DB")
PASSWORD = os.getenv("PASSWORD_DB")
DATABASE_NAME = os.getenv("NAME_DB")
HOST = os.getenv("HOST")


def get_north_data(path: str) -> list:
    """Функция, которая возвращает данные из csv-файла"""
    with open(path) as file:
        data = csv.reader(file)
        return list(data)[1:]


def push_to_employees(conn, data) -> None:
    """Функция для отправки данных в таблицу employees"""
    with conn.cursor() as cur:
        for d in data:
            cur.execute(f"INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                        (d[0], d[1], d[2], d[3], d[4], d[5]))


def push_to_customers(conn, data) -> None:
    """Функция для отправки данных в таблицу customers"""
    with conn.cursor() as cur:
        for d in data:
            cur.execute(f"INSERT INTO customers VALUES (%s, %s, %s)",
                        (d[0], d[1], d[2]))


def push_to_orders(conn, data) -> None:
    """Функция для отправки данных в таблицу orders"""
    with conn.cursor() as cur:
        for d in data:
            cur.execute(f"INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                        (d[0], d[1], d[2], d[3], d[4]))


def main():
    """Главная функция. Получает данные из csv-файлов и отправляет их в базу данных north"""
    employees_data = get_north_data(PATH_EMPLOYEES_DATA)
    customers_data = get_north_data(PATH_CUSTOMERS_DATA)
    orders_data = get_north_data(PATH_ORDERS_DATA)

    conn = psycopg2.connect(dbname=DATABASE_NAME, user=USERNAME, password=PASSWORD, host=HOST)

    try:
        with conn:
            push_to_employees(conn, employees_data)
            push_to_customers(conn, customers_data)
            push_to_orders(conn, orders_data)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
