"""
Модуль содержит процедуры для обработки и записи данных из EGRUL
раздел СвЗапЕГРЮЛ

Author: Gansior Alexander, gansior@gansior.ru, +79173383804
Starting 2025/04/25
Ending 2025//

"""

import sys

new_path = "/opt/airflow2/"
sys.path.append(new_path)

from modules.egrul.com_f import (
    get_zerro_data,
    hash_f,
    write_db,
    update_field,
    update_field_2,
)

NAME_TBL_SV = "s_local_company_egrul_history"
EGRUL_EGRIP = "EGRUL"


def write_liquid(ddis, fformat, schema):
    """_summary_

    Args:
        dr (_type_): _description_
        fformat (_type_): _description_
    """
    print("===== write_liquid =====")
    NAME_TBL_LIQUID = "s_local_company_is_liquidated_info"
    dr_new = get_zerro_data(fformat, NAME_TBL_LIQUID, EGRUL_EGRIP)
    print(dr_new)
    fields_to_update = {
        "СвЮЛ^СвПрекрЮЛ^ГРНДата^@ДатаЗаписи": "СвЮЛ^СвЗапЕГРЮЛ^ГРНДата^@ДатаЗап",
        "СвЮЛ^СвПрекрЮЛ^СвРегОрг^@НаимНО": "СвЮЛ^СвЗапЕГРЮЛ^СвРегОрг^@НаимНО",
        "СвЮЛ^СвПрекрЮЛ^СвРегОрг^@КодНО": "СвЮЛ^СвЗапЕГРЮЛ^СвРегОрг^@КодНО",
        "СвЮЛ^СвПрекрЮЛ^@ДатаПрекрЮЛ": "СвЮЛ^СвЗапЕГРЮЛ^ГРНДата^@ДатаЗап",
        "СвЮЛ^СвПрекрЮЛ^ГРНДата^@ГРН": "СвЮЛ^СвЗапЕГРЮЛ^ГРНДата^@ГРН",
        "СвЮЛ^СвПрекрЮЛ^СпПрекрЮЛ^@КодСпПрекрЮЛ": "СвЮЛ^СвЗапЕГРЮЛ^ВидЗап^@КодСПВЗ",
        "СвЮЛ^СвПрекрЮЛ^СпПрекрЮЛ^@НаимСпПрекрЮЛ": "СвЮЛ^СвЗапЕГРЮЛ^ВидЗап^@НаимВидЗап",
        "rec_src": "rec_src",
        "statement_dt": "statement_dt",
        "local_company_pk": "local_company_pk",
        "ogrn": "ogrn",
        "hash_diff": "hash_diff",
    }
    ddis2_mapping = {item["key"]: item["value"] for item in ddis.values()}
    for field, value in fields_to_update.items():
        dr_new = update_field_2(dr_new, field, value, ddis2_mapping)
    write_db(dr_new, schema, NAME_TBL_LIQUID)


def process_item(item, dr, path_prefix, schema):
    """
    Обрабатываем один элемент списка или словарь целиком
    :param item: элемент (словарь), содержащий информацию о документе
    :param dr: объект записи
    :param path_prefix: префикс пути для update_field
    """
    fields_to_process = {"НаимДок": "НаимДок", "НомДок": "НомДок",
                         "ДатаДок": "ДатаДок"}

    for field_name, field_path_suffix in fields_to_process.items():
        if field_name in item:
            full_path = f"{path_prefix}^{field_path_suffix}"
            dr = update_field(dr, full_path, item[field_name])
            dr["hash_diff"]["value"] = hash_f(
                dr["hash_diff"]["value"] + item[field_name]
            )

    write_db(dr, schema, NAME_TBL_SV)


def sv_zap_egrul(ddis: dict,
                 fformat,
                 base_data: dict,
                 codes_fns: list,
                 schema: str):
    """_summary_

    Args:
        ddis (dict): _description_
        fformat (_type_): _description_
        base_data (dict): _description_
        codes_fns (list): _description_
    """
    print("sv_zap_egrul get", ddis)
    dr = get_zerro_data(fformat, NAME_TBL_SV, EGRUL_EGRIP)
    print(dr)
    print(base_data)

    # Основные поля для обновления
    fields_to_update = {
        "СвЮЛ^СвЗапЕГРЮЛ^@ИдЗап": ddis.get("@ИдЗап"),
        "СвЮЛ^СвЗапЕГРЮЛ^ГРНДата^@ГРН": ddis.get("@ГРН", ""),
        "СвЮЛ^СвЗапЕГРЮЛ^ГРНДата^@ДатаЗап": ddis.get("@ДатаЗап"),
        "СвЮЛ^СвЗапЕГРЮЛ^ВидЗап^@КодСПВЗ": ddis["ВидЗап"].get("@КодСПВЗ"),
        "СвЮЛ^СвЗапЕГРЮЛ^ВидЗап^@НаимВидЗап": ddis["ВидЗап"].get("@НаимВидЗап"),
        "rec_src": base_data.get("rec_src"),
        "statement_dt": base_data.get("statement_dt"),
        "local_company_pk": base_data.get("local_company_pk"),
        "ogrn": base_data.get("ogrn"),
        "hash_diff": base_data.get("hash_diff"),
    }

    if "@ГРН" in ddis:
        fields_to_update["СвЮЛ^СвЗапЕГРЮЛ^ГРНДата^@ГРН"] = ddis["@ГРН"]
    # Update fields in a loop
    for field, value in fields_to_update.items():
        dr = update_field(dr, field, value)

    if "СвРегОрг" in ddis:
        dr = update_field(
            dr, "СвЮЛ^СвЗапЕГРЮЛ^СвРегОрг^@КодНО", ddis["СвРегОрг"]["@КодНО"]
        )
        dr = update_field(
            dr, "СвЮЛ^СвЗапЕГРЮЛ^СвРегОрг^@НаимНО", ddis["СвРегОрг"]["@НаимНО"]
        )

    if ddis["ВидЗап"]["@КодСПВЗ"] in codes_fns:
        print("liquid =>> ", dr)
        write_liquid(dr, fformat, schema)

    if "СвСвид" in ddis:
        dr = update_field(
            dr, "СвЮЛ^СвЗапЕГРЮЛ^СвСвид^@ДатаВыдСвид",
            ddis["СвСвид"]["@ДатаВыдСвид"]
        )
        dr = update_field(dr, "СвЮЛ^СвЗапЕГРЮЛ^СвСвид^@Номер",
                          ddis["СвСвид"]["@Номер"])
        dr = update_field(dr, "СвЮЛ^СвЗапЕГРЮЛ^СвСвид^@Серия",
                          ddis["СвСвид"]["@Серия"])

    if "СвСтатусЗап" in ddis:
        if "ГРНДатаНед" in ddis["СвСтатусЗап"]:
            dr = update_field(
                dr,
                "СвЮЛ^СвЗапЕГРЮЛ^СвСтатусЗап^ГРНДатаНед^@ИдЗап",
                ddis["СвСтатусЗап"]["ГРНДатаНед"]["@ИдЗап"],
            )
            dr = update_field(
                dr,
                "СвЮЛ^СвЗапЕГРЮЛ^СвСтатусЗап^ГРНДатаНед^@ГРН",
                ddis["СвСтатусЗап"]["ГРНДатаНед"]["@ГРН"],
            )
            dr = update_field(
                dr,
                "СвЮЛ^СвЗапЕГРЮЛ^СвСтатусЗап^ГРНДатаНед^@ДатаЗап",
                ddis["СвСтатусЗап"]["ГРНДатаНед"]["@ДатаЗап"],
            )

    if "ГРНДатаНедПред" in ddis:
        dr = update_field(
            dr,
            "СвЮЛ^СвЗапЕГРЮЛ^ГРНДатаНедПред^@ИдЗап",
            ddis["ГРНДатаНедПред"]["@ИдЗап"],
        )
        if "@ГРН" in ddis["ГРНДатаНедПред"]:
            dr = update_field(
                dr,
                "СвЮЛ^СвЗапЕГРЮЛ^ГРНДатаНедПред^@ГРН",
                ddis["ГРНДатаНедПред"]["@ГРН"],
            )
        dr = update_field(
            dr,
            "СвЮЛ^СвЗапЕГРЮЛ^ГРНДатаНедПред^@ДатаЗап",
            ddis["ГРНДатаНедПред"]["@ДатаЗап"],
        )

    if "СведПредДок" in ddis:
        items = ddis["СведПредДок"]

        # Проверяем тип поля 'СведПредДок'
        if isinstance(items, list):
            for i, item in enumerate(items):
                process_item(item, dr,
                             f"СвЮЛ^СвЗапЕГРЮЛ^СведПредДок^{i}",
                             schema)

        elif isinstance(items, dict):
            process_item(items, dr, "СвЮЛ^СвЗапЕГРЮЛ^СведПредДок", schema)
    else:
        write_db(dr, schema, NAME_TBL_SV)
