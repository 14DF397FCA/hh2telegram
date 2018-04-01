import os
import logging
import configparser
from collections import Set
from configparser import ConfigParser

import argparse
from typing import Dict, List

JOB_NAME = "hh_checker_task"


def read_app_config(args):
    if os.path.isfile(args.app_conf):
        config = configparser.ConfigParser()
        config.read(args.app_conf)
        return config
    else:
        logging.error(f"Can't find config file {args.app_conf}")
        return None


def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--app_conf", type=str, help="Config file path", required=True)
    args = parser.parse_args()
    return args


def configure_logger(args):
    args.log_level = "INFO"
    if args.log_level in logging._nameToLevel:
        level = logging._nameToLevel.get(args.log_level)
        logger = logging.getLogger()
        logger.setLevel(level)
        # fh = logging.FileHandler('/var/log/hh2telegram/main.log')
        # fh.setLevel(level)
        ch = logging.StreamHandler()
        ch.setLevel(level)
        formatter = logging.Formatter(
            '%(asctime)s [%(filename)s.%(lineno)d] %(processName)s %(levelname)-1s %(name)s - %(message)s')
        ch.setFormatter(formatter)
        # fh.setFormatter(formatter)
        logger.addHandler(ch)
        # logger.addHandler(fh)
    else:
        raise Exception(f"Can't recognize log level: {args.log_level}")


def is_section_exist(sections: list, section: str) -> bool:
    return section in sections


def is_param_exists(config: ConfigParser, param) -> bool:
    try:
        _ = config[param]
        return True
    except:
        return False


def string_to_tuple(data: str, separator: str = ",") -> tuple:
    return tuple([x.strip() for x in data.lower().split(separator)])


def __get_param(config: ConfigParser, section: str, param: str):
    if config is None:
        return None
    if is_section_exist(config.sections(), section):
        conf = config[section]
        if is_param_exists(conf, param):
            return conf[param]
        else:
            logging.error("Can't find parameter '%s' in specified configuration file", param, )
            return None
    else:
        logging.error("Can't find section %s in specified configuration file", section)
        return None


def extract_id_from_new_vacancies(vacancies: List[Dict]) -> Set:
    logging.info("Extracting vacancy id from list of new vacancies")
    if len(vacancies) == 0:
        logging.debug("Empty list of vacancies")
        return {}
    ids = set()
    for r_id, vac in enumerate(vacancies):
        ids.add(int(vac["id"]))
    return ids


def compare_vacancies(new_vacancies: List[Dict], old_vacancies: Set) -> List[Dict]:
    logging.info("Compare new and exists vacancies")
    if len(new_vacancies) == 0:
        logging.error("Empty list of new vacancies")
        return []
    if len(old_vacancies) == 0:
        return new_vacancies
    else:
        vacancies = []
        for _, vac in enumerate(new_vacancies):
            x = int(vac["id"])
            if x not in old_vacancies:
                vacancies.append(vac)
        return vacancies


def prepare_message(vacancies: List[Dict]) -> List[str]:
    if len(vacancies) == 0:
        logging.error("There are not new vacancies in list")
        return []
    r = []
    for _, vac in enumerate(vacancies):
        r.append(f"{vac['name']}\n"
                 f"{vac['requirement']}\n"
                 f"{vac['responsibility']}\n"
                 f"{vac['alternate_url']}")
    return r


def get_telegram_token(config: ConfigParser):
    return __get_param(config=config, section="main", param="token")


def get_db_name(config: ConfigParser):
    return __get_param(config=config, section="main", param="db_name")


def get_json_columns_for_table_vacancies(config: ConfigParser):
    return __get_param(config=config, section="vacancies", param="json_columns")


def get_vacancy_table_name(config: ConfigParser):
    return __get_param(config=config, section="main", param="vacancy_table_name")


def get_vacancy_table_schema(config: ConfigParser, vacancy_table):
    return __get_param(config=config, section=vacancy_table, param="table_schema")
