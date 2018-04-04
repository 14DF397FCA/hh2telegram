import json
import logging
from json import loads, dumps
from typing import Dict, List

import requests
from requests import Response

from utils.db import convert_vacancies_to_insert_sql, connect_db, create_table_vacancies, save_vacancies_to_db, \
    get_vacancies_from_db
from utils.utils import get_json_columns_for_table_vacancies, get_db_name, compare_vacancies, prepare_message, \
    get_vacancy_table_name, get_vacancy_table_schema, get_base_url, get_vacancy_on_page

SUF_PER_PAGE = "per_page"
SUF_PAGE = "page"


def __make_url(url: str, page: int, per_page: int):
    return f"{url}&{SUF_PER_PAGE}={per_page}&{SUF_PAGE}={page}"


def __get_raw_page(url: str, page: int, per_page: int) -> Response:
    if url is not None and len(url) > 0 and page >= 0 and per_page >= 1:
        try:
            url = __make_url(url=url, page=page, per_page=per_page)
            logging.info("Generated url - %s", url)
            return requests.get(url)
        except:
            logging.error("Can't get page %s", url)
            return None
    else:
        return None


def __page_to_json(resp: Response) -> json:
    if resp is not None:
        try:
            return loads(resp.text)
        except:
            logging.error("Can't convert page to JSON")
    else:
        return dumps({})


def page_to_json(url: str, page: int, per_page: int) -> json:
    return __page_to_json(resp=__get_raw_page(url=url, page=int(page), per_page=int(per_page)))


def __get_data_for_vacancy(data, key):
    if key in data:
        return data[key]
    else:
        return ''


def __get_vacancy(vacancy: json) -> {}:
    try:
        return {"id": vacancy["id"],
                "requirement": __get_data_for_vacancy(__get_data_for_vacancy(vacancy, "snippet"), "requirement"),
                "alternate_url": __get_data_for_vacancy(vacancy, "alternate_url"),
                "created_at": __get_data_for_vacancy(vacancy, "created_at"),
                "published_at": __get_data_for_vacancy(vacancy, "published_at"),
                "name": __get_data_for_vacancy(vacancy, "name"),
                "responsibility": __get_data_for_vacancy(__get_data_for_vacancy(vacancy, "snippet"), "responsibility")}
    except:
        logging.error("Can't parse input json, %s", vacancy)
        return {}


def get_vacancies(url: str, on_page: int = 100) -> List[Dict]:
    logging.info("Receive vacancies from hh.ru")
    page = 0
    first_page = page_to_json(url=url, page=page, per_page=on_page)
    page_num = first_page["pages"]
    vacancies = []
    while page < page_num:
        data = page_to_json(url=url, page=page, per_page=on_page)
        for i in data['items']:
            vacancies.append(__get_vacancy(i))
        page += 1
    logging.info("Received %s vacancies from hh.ru", len(vacancies))
    return vacancies


def save_vacancies(vacancies: List[Dict], connection, columns):
    logging.info("Save vacancies to database")
    if len(vacancies) == 0:
        logging.error("Empty list of vacancies")
        return
    r = convert_vacancies_to_insert_sql(vacancies=vacancies,
                                        table_name="vacancies",
                                        columns=columns)

    i = save_vacancies_to_db(conn=connection, vacancies=r)

    logging.info("Vacancies were saved to database. Affected rows - %s", i)


def get_new_vacancies(conf) -> List[str]:
    vacancy_table_name = get_vacancy_table_name(config=conf)
    vacancy_table_schema = get_vacancy_table_schema(config=conf, vacancy_table=vacancy_table_name)
    columns = get_json_columns_for_table_vacancies(config=conf)
    vacancy_on_page = get_vacancy_on_page(config=conf)
    url = get_base_url(config=conf)
    new_vacancies = get_vacancies(url=url, on_page=vacancy_on_page)
    conn = connect_db(db_name=get_db_name(config=conf))
    create_table_vacancies(conn=conn, table_name=vacancy_table_name, table_schema=vacancy_table_schema)
    old_ids = get_vacancies_from_db(connection=conn, table_name=vacancy_table_name)
    new = compare_vacancies(old_vacancies=old_ids, new_vacancies=new_vacancies)
    logging.info("New vacancies - %s", new)
    save_vacancies(vacancies=new, connection=conn, columns=columns)
    conn.close()
    return prepare_message(vacancies=new)
