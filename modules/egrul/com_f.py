"""
Общие процедуры используемые для парсинга данных ЕГРЮЛ
Author: Gansior Alexander, gansior@gansior.ru, +79173383804
Starting 2025/04/20
Ending 2025//
"""

import os
import re
from datetime import datetime
import sqlite3
import hashlib
import logging
import requests
import json
import time


# Устанавливаем все константы
# Устанавливаем форматы ФНС с которыми будем работать
VERS_FORMAT_EGRIP = "format_4.06"
VERS_FORMAT_EGRUL = "format_4.07"

# Значение полей которые приходят в СМЭВ
# для идентификации набора сведений
INPUT_EGRUL = "ЕГРЮЛ_ОТКР_СВЕД"
INPUT_EGRIP = "ЕГРИП_ОТКР_СВЕД"

AUTH_CLIENT = os.environ.get("AUTH_CLIENT")
AUTH_PASSWORD = os.environ.get("AUTH_PASSWORD")
SSO_ENDPOINT = os.environ.get("SSO_ENDPOINT")
B_SCHEMA = 'gansior'
BORDER = 3300 # Время жизни токена в секундах
CONN_GET = os.environ.get("AIRFLOW_CONN_SPPR_KORNILIN")

all_tables_egrul = [
        's_local_company_actioners_registry_keeper_info',
        's_local_company_address_fias_info',
        's_local_company_address_info',
        's_local_company_assignee_info',
        's_local_company_attorney_info',
        's_local_company_capital_share_company_info',
        's_local_company_company_authorized_capital_info',
        's_local_company_egrul_history',
        's_local_company_email_info',
        's_local_company_founders_info_fl',
        's_local_company_founders_info_pif',
        's_local_company_founders_info_rfsubmo',
        's_local_company_founders_info_ulin',
        's_local_company_founders_info_ulros',
        's_local_company_is_liquidated_info',
        's_local_company_legal_name',
        's_local_company_licenses_info',
        's_local_company_managing_organization_info',
        's_local_company_okveds_info',
        's_local_company_pension_fund_info',
        's_local_company_predecessor_info',
        's_local_company_registration_info',
        's_local_company_registrator_info',
        's_local_company_reorganisation_info',
        's_local_company_social_insurance_fund_info',
        's_local_company_state_info',
        's_local_company_subdivisions_info',
        's_local_company_tax_authority_registration_info',
        'address_fias_mapping',
        'h_local_company_egrul_main'
    ]

all_tables_egrip = [
        's_individual_entrepreneur_address_fias_info',
        's_individual_entrepreneur_address_info',
        's_individual_entrepreneur_citizenship_info',
        's_individual_entrepreneur_egrip_history',
        's_individual_entrepreneur_email_info',
        's_individual_entrepreneur_individual_info',
        's_individual_entrepreneur_is_liquidated_info',
        's_individual_entrepreneur_licenses_info',
        's_individual_entrepreneur_main_info',
        's_individual_entrepreneur_okveds_info',
        's_individual_entrepreneur_pension_fund_info',
        's_individual_entrepreneur_registration_info',
        's_individual_entrepreneur_registrator_info',
        's_individual_entrepreneur_social_insurance_fund_info',
        's_individual_entrepreneur_state_info',
        's_individual_entrepreneur_tax_authority_registration_info',
        'h_individual_entrepreneur_egrip_main'
    ]


def cnst():
    """Определение констант

    Returns:
        _type_: _description_
    """
    # out_dict - переменная в которой храняться все константы
    # в зависимости от окружения
    out_dict = {
        # COMMON VARIABLES
        "data_load": "20250826",
        "type_data": "egrul_consum",
        # 'conn_get'- соединение с БД где храняться таблицы с форматами ФНС
        # 'schema_get'- схема в БД где храняться таблицы с форматами ФНС
        # 'conn_get': os.environ.get("AIRFLOW_CONN_SANDBOX"),  # 10.5.1.14 gansior
        # 'schema_get': "egrul",
        "conn_get": os.environ.get("AIRFLOW_CONN_SPPR_KORNILIN"),  # schema gansior
        # "schema_get": "egrul",
        "schema_get": "gansior",
        "egrul": "EGRUL",
        "egrip": "EGRIP",
        "input_egrul": INPUT_EGRUL,
        "input_egrip": INPUT_EGRIP,
        "vers_format_egrip": VERS_FORMAT_EGRIP,
        "vers_format_egrul": VERS_FORMAT_EGRUL,
        "format_egrip": f"format_{VERS_FORMAT_EGRIP.replace('.', '_')}",
        "format_egrul": f"format_{VERS_FORMAT_EGRUL.replace('.', '_')}",

        "info_out": "",
        "conn_out": "",
        "folder_tmp": os.environ.get("AIRFLOW_TEMP_DIR"),
        "owner": "airflow2",
        "path_tmp": "/opt/airflow/temp/",
        "local_file_path": "",
        "gar": "",
        # Таблицы егрюл которые парсятся одним способом
        "tabl_egrul": [
            "s_local_company_legal_name",
            "s_local_company_legal_name",
            "s_local_company_registration_info",
            "s_local_company_registrator_info",
            "s_local_company_state_info",
            "s_local_company_tax_authority_registration_info",
            "s_local_company_pension_fund_info",
            "s_local_company_social_insurance_fund_info",
            "s_local_company_company_authorized_capital_info",
            "s_local_company_managing_organization_info",
            "s_local_company_attorney_info",
            "s_local_company_actioners_registry_keeper_info",
            "s_local_company_capital_share_company_info",
            "s_local_company_reorganisation_info",
            "s_local_company_predecessor_info",
            "s_local_company_assignee_info",
            "s_local_company_subdivisions_info",
            "s_local_company_founders_info_ulros",
            "s_local_company_founders_info_ulin",
            "s_local_company_founders_info_fl",
            "s_local_company_founders_info_rfsubmo",
            "s_local_company_founders_info_pif",
        ],
        # Таблицы egrip которые парсятся одним способом
        "tabl_egrip": [
            "s_individual_entrepreneur_citizenship_info",
            "s_individual_entrepreneur_email_info",
            "s_individual_entrepreneur_registration_info",
            "s_individual_entrepreneur_registrator_info",
            "s_individual_entrepreneur_pension_fund_info",
            "s_individual_entrepreneur_social_insurance_fund_info",
            "s_individual_entrepreneur_tax_authority_registration_info",
            "s_individual_entrepreneur_individual_info",
            "s_individual_entrepreneur_main_info",
        ],
    }
    return out_dict


def get_gar(adress: str, cv: dict):
    logger = get_logger()
    logger.warning("<========= GET_GAR =========>")
    try:
        # print(f"cv: {cv}")
        gar = cv['gar']
        headers = gar['headers']

        json_data = gar['search']['json_data']
        json_data['search_string'] = adress
        gar_url = gar['search']['url']
        # print(f"URL сервиса ГАР: {gar_url}")
        # print(f"json_data сервиса ГАР: {json_data}")
        # print(f"header сервиса ГАР: {headers}")
        response = requests.post(
            gar_url,
            headers=headers,
            json=json_data,
            timeout=60)

        # Формируем результат для вывода
        result = {
            'status_code': response.status_code,
            'response_body': response.json() if response.content else 'Пустой ответ'
        }

        print(f"Код ответа: {result['status_code']}")
        print(f"Тело ответа: {result['response_body']}")
        if result['status_code'] == 200:
            k_adr = len(result['response_body']['results'])
            print(f"Количество адресов: {k_adr}")
            if k_adr > 0:
                adr = result['response_body']['results'][0]
                print(f"guid: {adr['guid']}")
                print(f"Adress: {adr['full']}")
                print("*************************************")
                return adr['guid'], adr['full']
            else:
                return None, None
        else:
            return None, None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе к сервису ГАР: {str(e)}")
        print(f"header сервиса ГАР: {headers}")
        print(f"status_code: {response.status_code}")
        return None, None


def get_gar_id(adress_id: str, cv: dict):
    """_summary_

    Args:
        adress_id (str): _description_
        cv (dict): _description_

    Returns:
        _type_: _description_
    """    
    logger = get_logger()
    logger.warning("<========= GET_GAR_ID =========>")
    try:
        # print(f"cv: {cv}")
        gar = cv['gar']
        headers = gar['headers']

        json_data = gar["houses"]['json_data']
        json_data["guid"] = adress_id
        gar_url = gar['search']['url']
        # print(f"URL сервиса ГАР: {gar_url}")
        # print(f"json_data сервиса ГАР: {json_data}")
        # print(f"header сервиса ГАР: {headers}")
        response = requests.post(
            gar_url,
            headers=headers,
            json=json_data,
            timeout=30)

        # Формируем результат для вывода
        result = {
            'status_code': response.status_code,
            'response_body': response.json() if response.content else 'Пустой ответ'
        }

        print(f"Код ответа: {result['status_code']}")
        print(f"Тело ответа: {result['response_body']}")
        if result['status_code'] == 200:
            print(f"Количество адресов: {len(result['response_body']['results'])}")
            adr = result['response_body']['results'][0]
            print(f"guid: {adr['guid']}")
            print(f"Adress: {adr['full']}")
            print("*************************************")
            return adr['guid'], adr['full']
        else:
            return None, None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе к сервису ГАР из get_gar_id: {str(e)}")
        print(f"header сервиса ГАР: {headers}")
        print(f"status_code: {response.status_code}")
        return None, None


def convert_windows1251_to_utf8(windows1251_bytes):
    """_summary_

    Args:
        windows1251_bytes (_type_): объект байтов в кодировке WINDOWS-1251
        или строка в кодировке WINDOWS-1251

    Returns:
        _type_: _description_
    """

    if isinstance(windows1251_bytes, str):
        # Encode the string to bytes using WINDOWS-1251
        bytes_data = windows1251_bytes.encode("windows-1251")

        # # Decode the bytes to a UTF-8 string
        utf8_string = bytes_data.decode("windows-1251")
        # utf8_string = bytes_data.encode('utf-8', errors='replace')
        # Use 'replace' to handle errors gracefully

        return utf8_string
    else:
        return None
        # # Decode the bytes from WINDOWS-1251 to a string
        # windows1251_string = windows1251_bytes.decode('windows-1251')

        # # Encode the string to UTF-8 bytes
        # utf8_string = windows1251_string.encode('utf-8')

        # return utf8_string.decode('utf-8')  # Return as a UTF-8 string


def xml_clear(ddd: str) -> str:
    """_summary_

    Args:
        ddd (_type_): XML из СМЭВ в нем заменяются дублирующиеся двойные ковычки на
                    &quot
    Returns:
        _type_: XML
    """
    # Очистка xml от артифактов двойных ковычек
    new_str = []
    ser_list = ddd.split('>')
    com_str_error = 0
    for fr1_str in ser_list:
        ser1_list = fr1_str.split('=')
        new_str2 = []
        if len(ser1_list) > 1:
            for fr2_str in ser1_list:
                if fr2_str.count('"') > 2:
                    com_str_error +=1
                    fr3_str = fr2_str[fr2_str.find('"') + 1:]
                    fr4_str = fr3_str[:fr3_str.rfind('"')].replace('"', '&quot;')
                    serv_str = ('=' + fr2_str[:fr2_str.find('"')+2] + fr4_str +
                                fr2_str[fr2_str.rfind('"'):])
                    new_str2.append(serv_str)
                elif fr2_str.count('"') <= 2 and fr2_str.count('"') > 0:
                    new_str2.append('=' + fr2_str)
                else:
                    new_str2.append(fr2_str)
        else:
            new_str2.append(ser1_list[0])
        new_str.append(''.join(new_str2[:]))
    rez_str = '>'.join(new_str)
    return rez_str


def hash_f(ss: str):
    """Функция получения хэша из строки

    Args:
        ss (_type_): _description_

    Returns:
        _type_: _description_
    """
    return hashlib.md5(str(ss).encode()).hexdigest()


def now_f():
    """_summary_

    Returns:
        _type_: _description_
    """
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def const_f(c='EGRUL'):
    """_summary_

    Args:
        c (str, optional): _description_. Defaults to 'EGRUL'.

    Returns:
        _type_: _description_
    """
    return c


def conn_base(var_con):
    """Connect to the PostgreSQL database server"""
    try:
        conn = sqlite3.connect('/home/gansior/MyProject/parser_xml_egrul_egrip/dataset/egrul_egrip.db')
        return conn
    except Exception as error:
        print(error)
        return None



def get_conn():
    """_summary_

    Returns:
        _type_: _description_
    """
    conn = sqlite3.connect('/home/gansior/MyProject/parser_xml_egrul_egrip/dataset/egrul_egrip.db')
    return conn


def get_connect():
    """_summary_

    Returns:
        _type_: _description_
    """
    conn = sqlite3.connect('/home/gansior/MyProject/parser_xml_egrul_egrip/dataset/egrul_egrip.db')
    curs = conn.cursor()
    return conn, curs


def get_zerro_data(fformat, name_tbl, erul_egrip):
    """Считывание всех полей для заполнения
    таблицы определенного формата и определенного типа данных

    Args:
        fformat (_type_): формат данных в таблице описания форматов
        name_tbl (_type_): название таблицы данных в базе adb
        erul_egrip (_type_): тип данных ЕГРЮЛ - EGRUL или ЕГРИП - EGRIP

    Returns:
        _type_: _description_
    """

    conn, curs = get_connect()
    sql = ""
    if erul_egrip == "EGRUL":
        sql = (
            f"SELECT name_fld, {fformat} "
            f"FROM formats_egrul_fni "
            f"WHERE name_tbl = '{name_tbl}' and is_active_{fformat} = TRUE"
        )
    if erul_egrip == "EGRIP":
        sql = (
            f"SELECT name_fld, {fformat} "
            f"FROM formats_egrip_fni "
            f"WHERE name_tbl = '{name_tbl}' and is_active_{fformat} = TRUE"
        )
    curs.execute(sql)
    rows = curs.fetchall()
    data = {}
    print("rows =>> ", rows)
    for row in rows:
        # print('Current row:', row)
        if "Документ" in row[1]:
            data[row[0]] = {"key": row[1].replace("Файл^Документ^", ""), "value": ""}
        elif "load_dtm" in row[0]:
            data[row[0]] = {"key": "load_dtm", "value": now_f()}
        elif "local_company_pk" in row[0]:
            data[row[0]] = {"key": "local_company_pk", "value": ""}
        elif "individual_entrepreneur_pk" in row[0]:
            data[row[0]] = {"key": "individual_entrepreneur_pk", "value": ""}
        elif "hash_diff" in row[0]:
            data[row[0]] = {"key": "hash_diff", "value": ""}
        elif "ogrn" in row[0]:
            data[row[0]] = {"key": "ogrn", "value": ""}
        elif "ogrnip" in row[0]:
            data[row[0]] = {"key": "ogrnip", "value": ""}
        elif "statement_dt" in row[1]:
            data[row[0]] = {"key": "statement_dt", "value": ""}
        elif "rec_src" in row[0]:
            data[row[0]] = {"key": "rec_src", "value": const_f(erul_egrip)}
    conn.close()
    return data


def write_db(data: dict, schema: str, name_tbl: str):
    """Writes data to the specified database table if conditions are met.

    Args:
        data (dict): The data to write, where each key maps to a dictionary
        with a "value" key.
        name_tbl (str): The name of the table to write to.
    """

    print(f"write_db ==> {name_tbl}")
    key_write = False
    kye_exepr = [
        "ogrn",
        "ogrnip",
        "local_company_pk",
        "individual_entrepreneur_pk",
        "rec_src",
        "load_dtm",
        "statement_dt",
        "hash_diff",
    ]

    for key in data:
        # Check if the value is a dictionary and has the "value" key
        if isinstance(data[key], dict) and (key not in kye_exepr):
            value = data[key].get("value")
            if value is not None and value != "":
                key_write = True

    if key_write:
        conn, curs = get_connect()
        k = 0
        str_first = f"INSERT INTO {schema}.{name_tbl} ("
        str_second = "VALUES("

        print(f"write_db data ==> {data}")
        for key in data:
            if isinstance(data[key], dict):  # Ensure it's a dictionary
                value = data[key].get("value")

                # Avoid empty strings and None
                if value is not None and value != "":
                    k = 1
                    str_first += key + ", "
                    # Convert "None" to NULL
                    if value == "None":
                        value = "NULL"
                    str_second += f"'{value}', "
            if isinstance(data[key], str):
                if data[key] != "":
                    k = 1
                    str_first += key + ", "
                    str_second += f"'{data[key]}', "
        str_first = str_first[:-2] + ") "
        str_second = str_second[:-2] + ");"

        print(str_first + str_second)

        try:
            curs.execute(str_first + str_second)
            conn.commit()
        except Exception as e:
            print(e)
            print(f"No data for {name_tbl}")

        conn.close()


def write_adress_fias_mapping(data: dict, schema: str):
    """Writes data to the specified database table if conditions are met.

    Args:
        data (dict): The data to write, where each key maps to a dictionary
        with a "value" key.
        name_tbl (str): The name of the table to write to.
    """
    name_tbl = 'address_fias_mapping'
    
    # fields of table (entity_id, entity_type, hierarchy_fulltext_admin, 
    # hierarchy_fulltext_municipal,
    #     fias_standardized_address, fias_guid, status, 
    #     error_message, created_at, updated_at, hash_diff)
    
    print(f"write_db ==> {name_tbl}")
    if data['adr_uuid']:
        conn, curs = get_connect()
        entity_id = hash_f(data['adr_uuid'] + data['adr_fias_f'])
        str_first = f"INSERT INTO {schema}.{name_tbl} (entity_id, fias_standardized_address, fias_guid, hash_diff) "
        str_second = f"""VALUES('{entity_id}','{data["adr_fias_f"]}','{hash_f(data["adr_uuid"])}', '{entity_id}');"""
        try:
            curs.execute(str_first + str_second)
            conn.commit()
            print(f"write_db data ==> {data}")
        except Exception as e:
            print(e)
            print(f"No data for {name_tbl}")
        conn.close()
    else:
        print(f"NOT write_db ==> {name_tbl}")


def get_codes_fns():
    """_summary_

    Returns:
        list: список кодов относящиеся к ликвидации
    """
    conn, cur = get_connect()
    sql = 'SELECT cod_fns FROM codes_fns where is_active = true;'
    cur.execute(sql)
    coddes = cur.fetchall()
    print(coddes)
    rezz = [item[0] for item in coddes]
    conn.close()
    return rezz


def get_numb_rows_table(schema: str, name_table):
    """_summary_

    Returns:
        list: список кодов относящиеся к ликвидации
    """
    conn, cur = get_connect()
    sql = f'SELECT count(*) FROM {schema}.{name_table};'
    cur.execute(sql)
    num_rows = cur.fetchall()[0][0]
    conn.close()
    return num_rows


def update_field(dr: dict, n_f: str, vvv):
    """ Процедура обогащает dr по ключу поля n_f значением vvv
    Args:
        dr (dict): словарь надо обогатить значениями
        n_f (str): ключ поля которое надо обогатить
        vvv (_type_): значение которое надо записать

    Returns:
        _type_: _description_
    """    
    for k in dr:
        if dr[k]['key'] == n_f:
            dr[k]['value'] = vvv
            return dr
    return dr


def update_field_2(dr: dict, n_f: str, field2: str, ddis2_mapping: dict):
    """Процедура подставляет значение поля field2 dr key

    Args:
        dr (dict): словарь надо обогатить значениями из ddis2
        n_f (str): ключ поля для куда записывается значение
        field2 (str): ключ поля откуда берется значение
        ddis2 (dict): словарь со значениями полей которые надо вставить в dr

    Returns:
        _type_: _description_
    """

    # Update the value in dr if the keys match
    for item in dr.values():
        if item["key"] == n_f and field2 in ddis2_mapping:
            item["value"] = ddis2_mapping[field2]
            break  # Exit the loop after the first match
    return dr


def get_logger():
    """Определяем общий логгер для всех функций

    Returns:
        _type_: _description_
    """
    logger = logging.getLogger("DAG_CREATE_ALL_TABLES_IMF_WB")
    console_handler = logging.StreamHandler()
    # console_handler.setLevel(logging.DEBUG)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    return logger


def get_value(i_d: dict, key: str):
    """_summary_

    Args:
        i_d (dict): _description_
        key (str): _description_

    Returns:
        _type_: _description_
    """
    keyl = key.split("^")
    result = i_d
    try:
        for part in keyl:
            if not isinstance(result, dict) or part not in result:
                return None  # Handle missing keys or invalid structure
            result = result[part]
        # print('get_volue =>> ', key, ' ==>> ', result)
    except Exception as e:
        print(e)
        result = None
    return result


def common_write_one(
    i_d: dict, fformat: str, name_tbl: str,
    egrul_egrip, base_data: dict,
    schema: str
):
    """_summary_

    Args:
        i_d (dict): _description_
        fformat (_type_): _description_
        name_tbl (_type_): _description_

    Returns:
        _type_: _description_
    """
    # testing connect
    data = get_zerro_data(fformat, name_tbl, egrul_egrip)
    print(data)
    # print(' =================  СвИП  ======================')
    # print(i_d['СвИП'])
    str_con_hash = ''
    for key, sect in data.items():
        if isinstance(sect["key"], str) and "^" in list(sect["key"]):
            print(key)
            print(sect["key"])
            sect["value"] = get_value(i_d, sect["key"])
            if isinstance(sect["value"], list):
                # Учтен случай когда много <НаимЛицВидДеят> в </СвЛицензия>
                data_str = ''
                for ss in sect["value"]:
                    data_str += (ss + '; ')
                sect["value"] = data_str
            if sect["value"] is not None:
                str_con_hash += sect["value"]
            print(sect["value"])
        match sect["key"]:  # проверка на совпадение ключей
            case "rec_src":
                sect["value"] = base_data["rec_src"]
            case "statement_dt":
                sect["value"] = base_data["statement_dt"]
            case "local_company_pk":
                sect["value"] = base_data["local_company_pk"]
            case "individual_entrepreneur_pk":
                sect["value"] = base_data["individual_entrepreneur_pk"]
            case "ogrn":
                sect["value"] = base_data["ogrn"]
            case "ogrnip":
                sect["value"] = base_data["ogrnip"]
            case "hash_diff":
                if egrul_egrip == "EGRUL":
                    sect["value"] = hash_f(base_data["ogrn"] + str_con_hash)
                else:
                   sect["value"] = hash_f(base_data["ogrnip"] + str_con_hash)
    write_db(data, schema, name_tbl)
