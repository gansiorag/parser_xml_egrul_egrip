'''
Модуль содержит процедуры для обработки и записи данных из EGRIP
по всем разделам

Author: Gansior Alexander, gansior@gansior.ru, +79173383804
Starting 2025/06/20
Ending 2025//

'''

import sys
import xmltodict as xd

new_path = "/opt/airflow2/"
sys.path.append(new_path)

from modules.egrul.com_f import xml_clear, hash_f
from modules.egrul.egrip_adres import address_info_egrip
from modules.egrul.egrip_sv_history import sv_zap_egrip
from modules.egrul.egrip_svokved import egrip_svokved
from modules.egrul.com_f import cnst, get_logger, common_write_one


# Устанавливаем все константы
CV = cnst()
EGRIP = CV["egrip"]
INPUT_EGRIP = CV["input_egrip"]
VERS_FORMAT_EGRIP = CV["vers_format_egrip"]
FORMAT_EGRIP = CV["format_egrip"]

# task ARCH-623 common
# task ARCH-772	proba_egrip

logger = get_logger()


def work_dict_egrip(i_d: dict, base_key: str, name_table: str,
                    base_data: dict):
    """_summary_

    Args:
        i_d (dict): _description_

    Returns:
        _type_: _description_
    """
    print("dict 1 ->", base_key)

    # Устанавливаем общие данные
    document = i_d["Файл"]["Документ"]["СвИП"]
    common_data = {"@ОГРНИП": document["@ОГРНИП"],
                   "@ДатаОГРНИП": document["@ДатаОГРНИП"]}
    print("kol object = ", len(document[base_key]))
    for nm_f in document[base_key]:
        print("list cc = ", nm_f)  # , document[base_key][nm_f])
        if isinstance(document[base_key][nm_f], list):
            print("list 3 kol_obj = ", len(document[base_key][nm_f]))
            for nm_ff in document[base_key][nm_f]:
                print(nm_ff)
                isd_dd = {"СвИП": {base_key: {nm_f: nm_ff, **common_data}}}
                print('isd_dd ===>', isd_dd)
                common_write_one(isd_dd, FORMAT_EGRIP,
                                 name_table, EGRIP,
                                 base_data,
                                 CV['schema_get'])
        elif isinstance(document[base_key][nm_f], dict):
            print("dict 2", base_key, nm_f)
            print(document[base_key][nm_f])
            isd_dd = {
                "СвИП": {base_key: {nm_f: document[base_key][nm_f]},
                         **common_data}
            }
            print(isd_dd)
            common_write_one(isd_dd, FORMAT_EGRIP,
                             name_table,
                             EGRIP, base_data,
                             CV['schema_get'])


def process_list2(l_d: list, b_key: str, n_t: str, i_d, base_data: dict):
    """_summary_

    Args:
        data (_type_): _description_

    Returns:
        _type_: _description_
    """
    print("list 1 kol obj = ", len(l_d))
    for nm_f in l_d:
        print("New row ->", nm_f)
        if isinstance(nm_f, list):
            print("_egrip list 2")
            for nm_ff in l_d[nm_f]:
                print(nm_ff)
        if isinstance(nm_f, dict):
            print("process_list_egrip dict 1")
            isd_dd = {
                "СвИП": {
                    b_key: nm_f,
                    "@ОГРНИП": i_d["@ОГРНИП"],
                    "@ДатаВып": i_d["@ДатаОГРНИП"],
                }
            }
            common_write_one(isd_dd, FORMAT_EGRIP, n_t, EGRIP,
                             base_data, CV['schema_get'])
            for nm_ff in nm_f:
                print(nm_ff, " ->", nm_f[nm_ff])


def process_list_egrip(l_d: list, b_key: str, n_t: str, base_data: dict):
    """_summary_

    Args:
        data (_type_): _description_

    Returns:
        _type_: _description_
    """
    print("_egrip list 1 kol obj = ", len(l_d))
    for nm_f in l_d:
        print("New row ->", nm_f)
        if isinstance(nm_f, list):
            print("list 2")
            for nm_ff in l_d[nm_f]:
                print(nm_ff)
        if isinstance(nm_f, dict):
            print("process_list_egrip dict 1")
            isd_dd = {
                "СвИП": {
                    b_key: nm_f,
                    "@ОГРНИП": base_data["ogrn"],
                    "@ДатаВып": base_data["statement_dt"],
                }
            }
            common_write_one(isd_dd, FORMAT_EGRIP, n_t, EGRIP,
                             base_data, CV['schema_get'])
            for nm_ff in nm_f:
                print(nm_ff, " ->", nm_f[nm_ff])


# task ARCH-772	proba_egrip


def parser_svip(doc_source, rez_dict: dict, CODES_FNS: list):
    """_summary_

    Args:
        message (_type_): _description_
        cur (_type_): _description_
        logger (_type_): _description_
    """
    k_s_z = 0
    isd = {"СвИП": doc_source}

    base_data = {
        "rec_src": EGRIP,
        "statement_dt": doc_source["@ДатаВып"],
        "individual_entrepreneur_pk": hash_f(doc_source["@ОГРНИП"]),
        "ogrnip": doc_source["@ОГРНИП"],
        "hash_diff": hash_f(doc_source["@ОГРНИП"]),
    }

    common_write_one(
        isd, FORMAT_EGRIP, "h_individual_entrepreneur_egrip_main",
        EGRIP, base_data, CV['schema_get']
    )  # checked

    for kk in doc_source:
        print(kk, "->")
        match kk:
            # case "СвОКВЭД":
            #     # print(kk, '->',doc_source[kk])
            #     if isinstance(doc_source[kk], list):
            #         l_data = doc_source[kk]
            #         process_list_egrip(l_data, kk,
            #                            "s_individual_entrepreneur_okveds_info",
            #                            base_data)
            #     elif isinstance(doc_source[kk], dict):
            #         print("СвОКВЭД EGRIP dict")
            #         print(kk, '->', doc_source[kk])
            #         egrip_svokved(doc_source[kk],
            #                       FORMAT_EGRIP,
            #                       base_data
            #                       )
            # case "СвЛицензия":
            #     print(kk, "->", doc_source[kk])
            #     if isinstance(doc_source[kk], list):
            #         print("list 1 kol_obj = ", len(doc_source[kk]))
            #         for nm_f in doc_source[kk]:
            #             print("data1 = ", nm_f)
            #             # print(doc_source[kk][nm_f])
            #             if isinstance(nm_f, list):
            #                 l_data = doc_source[kk][nm_f]
            #                 process_list_egrip(l_data,
            #                                    kk,
            #                                    "s_individual_entrepreneur_licenses_info",
            #                                    base_data)
            #             else:
            #                 # Добавление лицензии прямо в список объектов
            #                 list_dict = {
            #                     "СвИП": {
            #                         "СвЛицензия": nm_f,
            #                         "@ОГРНИП": doc_source["@ОГРНИП"],
            #                         "@ДатаВып": doc_source["@ДатаВып"],
            #                     }
            #                 }
            #                 print("list_dict1 = ", list_dict)
            #                 base_data["hash_diff"] = hash_f(nm_f["@НомЛиц"])
            #                 common_write_one(
            #                     list_dict,
            #                     FORMAT_EGRIP,
            #                     "s_individual_entrepreneur_licenses_info",
            #                     EGRIP, base_data, CV['schema_get']
            #                 )
            #     elif isinstance(doc_source[kk], dict):
            #         print("data1 dict", doc_source[kk])
            #         list_dict = {
            #                     "СвИП": {
            #                         "СвЛицензия": doc_source[kk],
            #                         "@ОГРНИП": doc_source["@ОГРНИП"],
            #                         "@ДатаВып": doc_source["@ДатаВып"],
            #                     }
            #                 }
            #         base_data["hash_diff"] = hash_f(doc_source[kk]["@НомЛиц"])
            #         common_write_one(
            #             list_dict,
            #             FORMAT_EGRIP,
            #             "s_individual_entrepreneur_licenses_info",
            #             EGRIP, base_data, CV['schema_get']
            #         )
            # case "СвПрекращ":
            #     isd = {"СвИП": doc_source}
            #     print(kk, "->", doc_source[kk])
            #     common_write_one(
            #         isd,
            #         FORMAT_EGRIP,
            #         "s_individual_entrepreneur_is_liquidated_info",
            #         EGRIP, base_data, CV['schema_get']
            #     )
            # case "СвЗапЕГРИП":
            #     # Обработка раздела СвЗапЕГРИП
            #     print(doc_source[kk])
            #     print("len list - >", len(doc_source[kk]))
            #     if isinstance(doc_source[kk], list):
            #         print(kk, "-> list")
            #         l_data = doc_source[kk]

            #         for dsv in l_data:
            #             print("k_s_z = ", k_s_z)
            #             base_data["hash_diff"] = hash_f(dsv['@ИдЗап'])
            #             sv_zap_egrip(dsv, FORMAT_EGRIP,
            #                          CV['schema_get'], 
            #                          base_data, CODES_FNS)
            #             k_s_z += 1
            #     if isinstance(doc_source[kk], dict):
            #         print("dict")
            #         sv_zap_egrip(doc_source[kk], FORMAT_EGRIP,
            #                      CV['schema_get'],
            #                      base_data, CODES_FNS)
            case "СвАдрМЖ":
                address_info_egrip(
                    doc_source["СвАдрМЖ"],
                    FORMAT_EGRIP,
                    CV["schema_get"],
                    EGRIP,
                    base_data,
                )
    # Таблицы которые парсятся одним способом
    for n_t in CV['tabl_egrip']:
        common_write_one(isd, FORMAT_EGRIP, n_t, EGRIP,
                         base_data, CV['schema_get'])


def parser_mesage_egrip(mess_i, codes_fns: list, token: str):
    """_summary_

    Args:
        message (_type_): _description_
        cur (_type_): _description_
        logger (_type_): _description_
    """
    # try:
    # Первичная информация о сообщении
    print("EGRIP message!!!!")
    print(f"Message length: {len(mess_i)}")
    logger.debug(codes_fns)
    print("Processing message...")
    # Process the message
    # Определяем версию формата vers_form
    vers_form = mess_i.split('ВерсФорм="')[1].split('" ТипИнф=')[0]

    if vers_form == VERS_FORMAT_EGRIP:
        # Убераем недопустимые символы
        rez_clear = xml_clear(mess_i)
        # Преобразуем xml в словарь
        rez_dict = xd.parse(rez_clear)
        num_doc = 0

        # Сообщения могут приходить только с одним документом тогда
        # rez_dict["Файл"]["Документ"] - это словарь
        # если несколько документов то список.
        # Определяем это

        documents = rez_dict["Файл"]["Документ"]

        # Обрабатываем документ(ы) одинаково, в зависимости от типа
        if isinstance(documents, list):
            print(f'=== Документов {len(documents)} в списке ')
            for dd in documents:
                num_doc += 1
                print(f'=== Документ {num_doc} === from {len(documents)}')
                logger.debug('step 1 egrip', dd)
                if "СвИП" in dd:
                    doc_source = dd["СвИП"]
                    new_d = {"Файл": {"Документ": {"СвИП": doc_source}}}
                    parser_svip(doc_source, new_d, codes_fns)
        elif isinstance(documents, dict):
            num_doc += 1
            print(f"=== Документ {num_doc} === from dict")
            if "СвИП" in documents:
                parser_svip(documents["СвИП"], rez_dict, codes_fns)

    # except Exception as process_error:
    #     logger.error(f"Error processing message: {process_error}")
