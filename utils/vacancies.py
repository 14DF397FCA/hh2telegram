import json
import logging
import time
from json import loads
from typing import Dict, List, Optional

from requests import Response

from utils.db import convert_vacancies_to_insert_sql, connect_db, create_table_vacancies, save_vacancies_to_db, \
    get_vacancies_from_db
from utils.utils import get_json_columns_for_table_vacancies, get_db_name, compare_vacancies, prepare_message, \
    get_vacancy_table_name, get_vacancy_table_schema, get_base_url, get_vacancy_on_page, exec_http_request, \
    get_http_delay, get_http_attempt, get_vacancy

SUF_PER_PAGE = "per_page"
SUF_PAGE = "page"


class HHGetVacancies:
    def __init__(self, config):
        self.config = config
        self.http_delay = get_http_delay(config)
        self.http_attempt = get_http_attempt(config)
        self.vacancy_on_page = get_vacancy_on_page(config)
        self.url: str = get_base_url(config)
        self.on_page: int = get_vacancy_on_page(config)

    @staticmethod
    def __make_url(url: str, page: int, per_page: int):
        return f"{url}&{SUF_PER_PAGE}={per_page}&{SUF_PAGE}={page}"

    def __get_raw_page(self, url: str, page: int, per_page: int) -> Optional[Response]:
        if url is not None and len(url) > 0 and page >= 0 and per_page >= 1:
            try:
                url = self.__make_url(url=url, page=page, per_page=per_page)
                logging.info("Generated url - %s", url)
                return self.run_request(url)
            except:
                logging.error("Can't get page %s", url)
                return None
        else:
            return None

    def run_request(self, url: str) -> Optional[Response]:
        delay = self.http_delay
        for i in range(self.http_attempt):
            r = exec_http_request(url)
            if r is not None:
                return r
            else:
                logging.error("Wait %s seconds before next attempt (%s)", delay, i)
                time.sleep(delay)
                delay += self.http_delay
        logging.error("Can't get url (%s)", url)
        return None

    @staticmethod
    def __page_to_json(resp: Response) -> json:
        if resp is not None:
            try:
                return loads(resp.text)
            except:
                logging.error("Can't convert page to JSON")
        else:
            return None

    def page_to_json(self, url: str, page: int, per_page: int) -> json:
        return self.__page_to_json(resp=self.__get_raw_page(url=url, page=int(page), per_page=int(per_page)))

    def get_vacancies(self) -> Optional[List[Dict]]:
        logging.info("Receive vacancies from hh.ru")
        page = 0
        first_page = self.page_to_json(url=self.url, page=page, per_page=self.on_page)
        if first_page is not None:
            page_num = first_page["pages"]
            vacancies = []
            while page < page_num:
                data = self.page_to_json(url=self.url, page=page, per_page=self.on_page)
                for i in data['items']:
                    vacancies.append(get_vacancy(i))
                page += 1
            logging.info("Received %s vacancies from hh.ru", len(vacancies))
            return vacancies
        else:
            return []


def save_vacancies(vacancies: List[Dict], connection, columns):
    logging.info("Save vacancies to database")
    if len(vacancies) == 0:
        logging.error("List of new vacancies is empty, nothing to save")
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

    hh_api = HHGetVacancies(config=conf)
    new_vacancies = hh_api.get_vacancies()
    conn = connect_db(db_name=get_db_name(config=conf))
    create_table_vacancies(conn=conn, table_name=vacancy_table_name, table_schema=vacancy_table_schema)
    old_ids = get_vacancies_from_db(connection=conn, table_name=vacancy_table_name)
    new = compare_vacancies(old_vacancies=old_ids, new_vacancies=new_vacancies)
    logging.info("New vacancies - %s", new)
    save_vacancies(vacancies=new, connection=conn, columns=columns)
    conn.close()
    return prepare_message(vacancies=new)
