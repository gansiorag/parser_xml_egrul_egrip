"""_summary_"""

import sys, os

import xmltodict as xd

new_path = "/opt/airflow2/"
sys.path.append(new_path)

from modules.egrul.com_f import xml_clear, get_zerro_data, write_db, hash_f
from modules.egrul.egrul_adres import address_info
from modules.egrul.egrul_sv_history import sv_zap_egrul
from modules.egrul.egrul_svokved import egrul_svokved
from modules.egrul.com_f import cnst, get_logger, common_write_one

# Устанавливаем все константы
CV = cnst()
EGRUL = CV["egrul"]
INPUT_EGRUL = CV["input_egrul"]
VERS_FORMAT_EGRUL = CV["vers_format_egrul"]
FORMAT_EGRUL = CV["format_egrul"]

logger = get_logger()

# task ARCH-772	proba_egrip


def work_dict(i_d: dict, base_key: str, name_table: str, base_data: dict):
    """_summary_

    Args:
        i_d (dict): _description_

    Returns:
        _type_: _description_
    """
    print("dict 1 ->", base_key)

    # Устанавливаем общие данные
    document = i_d["Файл"]["Документ"]["СвЮЛ"]
    common_data = {"@ОГРН": document["@ОГРН"], "@ДатаВып": document["@ДатаВып"]}
    print("kol object = ", len(document[base_key]))
    base_dict = 0
    for nm_f in document[base_key]:
        print("list cc = ", nm_f)  # , document[base_key][nm_f])
        if isinstance(document[base_key][nm_f], list):
            print("list 3 kol_obj = ", len(document[base_key][nm_f]))
            for nm_ff in document[base_key][nm_f]:
                print(nm_ff)
                isd_dd = {"СвЮЛ": {base_key: {nm_f: nm_ff, **common_data}}}
                common_write_one(isd_dd, FORMAT_EGRUL, name_table,
                                 EGRUL,
                                 base_data, CV['schema_get'])
        elif isinstance(document[base_key][nm_f], dict):
            print("dict 2", base_key, nm_f)
            print(document[base_key][nm_f])
            isd_dd = {
                "СвЮЛ": {base_key: {nm_f: document[base_key][nm_f]}, **common_data}
            }
            print(isd_dd)
            common_write_one(isd_dd, FORMAT_EGRUL, name_table,
                             EGRUL,
                             base_data, CV['schema_get'])
        elif isinstance(document[base_key][nm_f], str):
            print("dict 3", base_key, nm_f)
            base_dict = 1

    if base_dict == 1:
        isd_dd = {"СвЮЛ": {base_key: document[base_key], **common_data}}
        print(isd_dd)
        common_write_one(isd_dd, FORMAT_EGRUL, name_table,
                         EGRUL,
                         base_data, CV['schema_get'])


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
            print("list 2")
            for nm_ff in l_d[nm_f]:
                print(nm_ff)
        if isinstance(nm_f, dict):
            print("process_list dict 1")
            isd_dd = {
                "СвЮЛ": {
                    b_key: nm_f,
                    "@ОГРН": i_d["@ОГРН"],
                    "@ДатаВып": i_d["@ДатаВып"],
                }
            }
            common_write_one(isd_dd, FORMAT_EGRUL, n_t, EGRUL,
                             base_data, CV['schema_get'])
            for nm_ff in nm_f:
                print(nm_ff, " ->", nm_f[nm_ff])


def process_list(l_d: list, b_key: str, n_t: str, base_data: dict):
    """Обрабатывает данные если они список

    Args:
        l_d (list): _description_
        b_key (str): Наименование поля ФНС
        n_t (str): name table
        i_d (_type_): _description_
        base_data (dict): _description_
    """

    print("list 1 kol obj = ", len(l_d))
    for nm_f in l_d:
        print("New row ->", nm_f)
        if isinstance(nm_f, list):
            print("list 2")
            for nm_ff in l_d[nm_f]:
                print(nm_ff)

        if isinstance(nm_f, dict):
            print("process_list dict 1")
            base_data["hash_diff"] = hash_f(nm_f["@ГРН"])
            isd_dd = {
                "СвЮЛ": {
                    b_key: nm_f,
                    "@ОГРН": base_data["ogrn"],
                    "@ДатаВып": base_data["statement_dt"],
                }
            }
            common_write_one(isd_dd, FORMAT_EGRUL, n_t, EGRUL,
                             base_data, CV['schema_get'])
            for nm_ff in nm_f:
                print(nm_ff, " ->", nm_f[nm_ff])


# task ARCH-772	proba_egrip
def parser_svul(doc_source: dict, codes_fns: list, cv: dict):
    """_summary_

    Args:
        message (_type_): _description_
        cur (_type_): _description_
        logger (_type_): _description_
    """
    print('Start parser_svul .')
    k_s_z = 0
    isd = {"СвЮЛ": doc_source}
    base_data = {
        "rec_src": EGRUL,
        "statement_dt": doc_source["@ДатаВып"],
        "local_company_pk": hash_f(doc_source["@ОГРН"]),
        "ogrn": doc_source["@ОГРН"],
        "hash_diff": hash_f(doc_source["@ОГРН"]),
    }

    common_write_one(
        isd, FORMAT_EGRUL, "h_local_company_egrul_main",
        EGRUL,
        base_data, CV['schema_get']
    )  # checked

    for kk in doc_source:
        print(kk, "->")
        match kk:
            # case "СвПодразд":
            #     # print(kk, '->',doc_source[kk])
            #     if isinstance(doc_source[kk], list):
            #         l_data = doc_source[kk]
            #         process_list(
            #             l_data, kk, "s_local_company_subdivisions_info",
            #             base_data
            #         )
            #     elif isinstance(doc_source[kk], dict):
            #         rez_dict = {"Файл": {"Документ": {"СвЮЛ": doc_source}}}
            #         work_dict(
            #             rez_dict, kk, "s_local_company_subdivisions_info",
            #             base_data
            #         )
            # case "СвОКВЭД":
            #     # print(kk, '->',doc_source[kk])
            #     if isinstance(doc_source[kk], list):
            #         l_data = doc_source[kk]
            #         process_list(l_data, kk, "s_local_company_okveds_info",
            #                      base_data)
            #     elif isinstance(doc_source[kk], dict):
            #         print("СвОКВЭД dict 1", doc_source[kk])
            #         egrul_svokved(
            #             doc_source[kk], FORMAT_EGRUL, base_data,
            #             cv["schema_get"]
            #         )

            # case "СвЛицензия":
            #     print(kk, "->", doc_source[kk])
            #     if isinstance(doc_source[kk], list):
            #         print(f"list 1 kol_obj = {len(doc_source[kk])}")
            #         for nm_f in doc_source[kk]:
            #             print("data1 =", nm_f)
            #             if isinstance(nm_f, list):
            #                 l_data = doc_source[kk][nm_f]
            #                 process_list(
            #                     l_data, kk, "s_local_company_licenses_info",
            #                     base_data
            #                 )
            #             else:
            #                 # Добавление лицензии прямо в список объектов
            #                 license_data = {
            #                     "СвЮЛ": {
            #                         "СвЛицензия": nm_f,
            #                         "@ОГРН": doc_source["@ОГРН"],
            #                         "@ДатаВып": doc_source["@ДатаВып"],
            #                     }
            #                 }
            #                 base_data["hash_diff"] = hash_f(nm_f["@НомЛиц"])
            #                 common_write_one(
            #                     license_data,
            #                     FORMAT_EGRUL,
            #                     "s_local_company_licenses_info",
            #                     EGRUL,
            #                     base_data, CV['schema_get']
            #                 )
            #     elif isinstance(doc_source[kk], dict):
            #         # Если СвЛицензия — это одиночный объект
            #         license_data = {
            #             "СвЮЛ": {
            #                 "СвЛицензия": doc_source[kk],
            #                 "@ОГРН": doc_source["@ОГРН"],
            #                 "@ДатаВып": doc_source["@ДатаВып"],
            #             }
            #         }
            #         base_data["hash_diff"] = hash_f(doc_source[kk]["@НомЛиц"])
            #         common_write_one(
            #             license_data,
            #             FORMAT_EGRUL,
            #             "s_local_company_licenses_info",
            #             EGRUL,
            #             base_data, CV['schema_get']
            #         )
            # case "СвПрекрЮЛ":
            #     isd = {"СвЮЛ": doc_source}
            #     print(kk, "->", doc_source[kk])
            #     common_write_one(
            #         isd,
            #         FORMAT_EGRUL,
            #         "s_local_company_is_liquidated_info",
            #         EGRUL,
            #         base_data, CV['schema_get']
            #     )
            # case "СвЗапЕГРЮЛ":
            #     # isd = {'СвЮЛ': doc_source}
            #     print(doc_source[kk])
            #     print("len list - >", len(doc_source[kk]))
            #     if isinstance(doc_source[kk], list):
            #         print(kk, "-> list")
            #         l_data = doc_source[kk]

            #         for dsv in l_data:
            #             print("k_s_z = ", k_s_z)
            #             base_data["hash_diff"] = hash_f(dsv["@ИдЗап"])
            #             sv_zap_egrul(dsv,
            #                          FORMAT_EGRUL,
            #                          base_data,
            #                          codes_fns,
            #                          CV['schema_get'])
            #             k_s_z += 1
            case "СвАдресЮЛ":
                address_info(doc_source["СвАдресЮЛ"], FORMAT_EGRUL,
                             EGRUL, base_data, cv)
    # Таблицы которые парсятся одним способом
    for n_t in cv["tabl_egrul"]:
        common_write_one(isd, FORMAT_EGRUL, n_t, EGRUL,
                         base_data,
                         CV['schema_get'])


def parser_mesage_egrul(mess_i, codes_fns: list, cv: dict):
    """_summary_

    Args:
        message (_type_): _description_
        cur (_type_): _description_
        logger (_type_): _description_
    """
    # try:
    print("parser_mesage_egrul  - EGRUL message!!!!")
    # mess_i = message.value.decode('utf-8', errors='ignore')

    # print(f"Received message: {mess_i}")
    print(f"Message length: {len(mess_i)}")
    print("codes_fns == ", codes_fns)
    if mess_i:
        print("Processing message...")
        # print(mess_i)
        # Process the message
        # Определяем тип данных type_data  и версию формата vers_form
        # type_data = mess_i.split('ТипИнф="')[1].split('" ВерсПрог=')[0]
        vers_form = mess_i.split('ВерсФорм="')[1].split('" ТипИнф=')[0]

        if vers_form == VERS_FORMAT_EGRUL:
            rez_clear = xml_clear(mess_i)
            rez_dict = xd.parse(rez_clear)
            num_doc = 0
            if isinstance(rez_dict["Файл"]["Документ"], list):
                print(f'=== Документов {len(rez_dict["Файл"]["Документ"])} в списке ')
                list_doc = rez_dict["Файл"]["Документ"]
                for dd in list_doc:
                    # doc_source = dd
                    num_doc += 1
                    print(f"=== Документ {num_doc} === from {len(list_doc)}")
                    print("content document => ",dd)
                    if "СвЮЛ" in dd:
                        doc_source = dd["СвЮЛ"]
                        new_d = {"Файл": {"Документ": {"СвЮЛ": doc_source}}}
                        parser_svul(doc_source, codes_fns, cv)
            if isinstance(rez_dict["Файл"]["Документ"], dict):
                print(f"=== Документ {num_doc} === from dict")

                if "СвЮЛ" in rez_dict["Файл"]["Документ"]:
                    parser_svul(
                        rez_dict["Файл"]["Документ"]["СвЮЛ"],
                        codes_fns, cv
                    )
                num_doc += 1

    # except Exception as process_error:
    #     logger.error(f"Error processing message: {process_error}")
