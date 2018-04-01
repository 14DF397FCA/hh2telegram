import json
import logging
from typing import List, Dict
import sqlite3

from utils.utils import string_to_tuple


def connect_db(db_name):
    return sqlite3.connect(database=db_name)


def create_table_vacancies(conn, table_name, table_schema):
    def create_table_vacancies_sql():
        return f"CREATE TABLE IF NOT EXISTS {table_name} ({table_schema});"

    sql = create_table_vacancies_sql()
    execute_query(cur=conn.cursor(), sql=sql)


def execute_query(sql, cur):
    logging.debug("Try to execute query %s", sql)
    if cur is None and sql is None and len(sql) == 0:
        logging.error("Cursor or SQL are empty")
        return
    try:
        cur.execute(sql)
        cur.connection.commit()
        return cur.rowcount
    except:
        return -1


def __extract_values_from_json(columns: str, data: json) -> str:
    columns = string_to_tuple(columns)
    r = ""
    for c in columns:
        d = data[c]
        if d is None:
            r += "'',"
        else:
            r += repr(data[c]) + ","
    return r[:-1]


def convert_vacancies_to_insert_sql(vacancies: List[Dict], table_name: str, columns: str) -> List[str]:
    logging.debug("Converting vacancies to SQL")
    if len(vacancies) == 0 and len(table_name) == 0 and len(columns) == 0:
        logging.error("Empty list of vacancies")
        return []
    sqls = []
    for vac in vacancies:
        s = f"INSERT INTO {table_name} ({columns}) VALUES ({__extract_values_from_json(columns=columns, data=vac)})"
        logging.info(s)
        sqls.append(s)
    return sqls


def save_vacancies_to_db(vacancies: List[str], conn) -> int:
    logging.debug("Saving vacancies to DB")
    if len(vacancies) == 0:
        logging.debug("Empty list of vacancies")
        return -1
    rows = 0
    for i in vacancies:
        rows += execute_query(sql=i, cur=conn.cursor())
    return rows


def get_vacancies_from_db(connection, table_name):
    cur = connection.cursor()
    s = f"SELECT id FROM {table_name};"
    cur.execute(s)
    r = cur.fetchall()
    ids = set()
    for i in r:
        ids.add(i[0])
    return ids
