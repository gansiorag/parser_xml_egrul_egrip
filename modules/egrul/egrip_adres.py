import sys

new_path = "/opt/airflow2/"
sys.path.append(new_path)

from modules.egrul.com_f import get_zerro_data, write_db

NAME_TBL_FIAS_EGRIP = "s_individual_entrepreneur_address_fias_info"
NAME_TBL_ARF_EGRIP = "s_individual_entrepreneur_address_info"


def adr_rf_egrip(i_d, fformat, egrul_egrip, base_data: dict):
    """_summary_
     АдресРФ

    Args:
        i_d (_type_): _description_
        fformat (_type_): _description_

    Returns:
        _type_: _description_
    """
    dd = get_zerro_data(fformat, NAME_TBL_ARF_EGRIP, egrul_egrip)
    print(dd)
    print("============= АдресРФ ================")
    adrf = i_d["АдресРФ"]
    print(adrf)
    print("=============================")
    full_addr = ""
    for ff in dd:
        print(dd[ff]["key"])
        key = dd[ff]["key"]
        match key:
            case "СвИП^СвАдрМЖ^АдресРФ^@КодРегион":
                dd[ff]["value"] = adrf["@КодРегион"]
                print(dd[ff]["value"])
            case "СвИП^СвАдрМЖ^АдресРФ^Регион^@ТипРегион":
                if "@ТипРегион" in adrf["Регион"]:
                    dd[ff]["value"] = adrf["Регион"]["@ТипРегион"]
                    print(dd[ff]["value"])
                    full_addr += (dd[ff]["value"] + " ")
            case "СвИП^СвАдрМЖ^АдресРФ^Регион^@НаимРегион":
                dd[ff]["value"] = adrf["Регион"]["@НаимРегион"]
                print(dd[ff]["value"])
                full_addr += (dd[ff]["value"] + ", ")
            case "СвИП^СвАдрМЖ^АдресРФ^Город^@ТипГород":
                if "Город" in adrf:
                    dd[ff]["value"] = adrf["Город"]["@ТипГород"]
                    full_addr += (dd[ff]["value"] + " ")
            case "СвИП^СвАдрМЖ^АдресРФ^Город^@НаимГород":
                if "Город" in adrf:
                    dd[ff]["value"] = adrf["Город"]["@НаимГород"]
                    full_addr += (dd[ff]["value"] + ", ")
            case "СвИП^СвАдрМЖ^АдресРФ^НаселПункт^@ТипНаселПункт":
                if "НаселПункт" in adrf:
                    if "ТипНаселПункт" in adrf["НаселПункт"]:
                        dd[ff]["value"] = adrf["НаселПункт"]["@ТипНаселПункт"]
                        full_addr += (dd[ff]["value"] + " ")
            case "СвИП^СвАдрМЖ^АдресРФ^НаселПункт^@НаимНаселПункт":
                if "НаселПункт" in adrf:
                    dd[ff]["value"] = adrf["НаселПункт"]["@НаимНаселПункт"]
                    full_addr += (dd[ff]["value"] + ", ")
            case "СвИП^СвАдрМЖ^АдресРФ^Район^@ТипРайон":
                if "Район" in adrf:
                    dd[ff]["value"] = adrf["Район"]["@ТипРайон"]
                    full_addr += (dd[ff]["value"] + " ")
                if "Район" in adrf:
                    dd[ff]["value"] = adrf["Район"]["@НаимРайон"]
                    full_addr += (dd[ff]["value"] + ", ")
       
            case "Файл^Документ^СвИП^СвАдрМЖ^ГРНИПДата^@ГРНИП":
                dd[ff]["value"] = adrf["ГРНИПДата"]["@ГРНИП"]
            case "Файл^Документ^СвИП^СвАдрМЖ^ГРНИПДата^@ДатаЗаписи":
                dd[ff]["value"] = adrf["ГРНИПДата"]["@ДатаЗаписи"]

        if key in ("rec_src", "statement_dt", "individual_entrepreneur_pk", "ogrnip", "hash_diff"):
            dd[ff]["value"] = base_data[key]
    print('full_addr ==>> ', full_addr)
    return dd


def analiz_zdan(isdt: list) -> dict:
    """_summary_

    Args:
        isdt (list): _description_

    Returns:
        dict: _description_
    """
    dd = {"@Номер": "", "@Тип": ""}
    for ii in isdt:
        dd["@Номер"] += ii["@Тип"] + ii["@Номер"]
        dd["@Тип"] += ii["@Тип"]
    return dd


def adr_fias_egrip(i_d, fformat, egrul_egrip, base_data: dict):
    """_summary_
        Адрес ЮЛ ФИАС
    Args:
        i_d (_type_): _description_
        fformat (_type_): _description_

    Returns:
        _type_: _description_
    """
    print(" in adr_fias_egrip ")
    print(i_d["АдрМЖФИАС"])
    serv_sprav = {
        "МуниципРайон": {
            "1": "муниципальный район",
            "2": "городской округ",
            "3": "внутригородская территория города федерального значения",
            "4": "муниципальный округ",
        },
        "ГородСелПоселен": {
            "1": "городское поселение",
            "2": "сельское поселение",
            "3": "межселенная территория в составе муниципального района",
            "4": "внутригородской район городского округа",
        },
    }

    dd = get_zerro_data(fformat, NAME_TBL_FIAS_EGRIP, egrul_egrip)
    dzdan = ""
    for ff in dd:
        # print('ff ==>> ', ff)
        key = dd[ff]["key"]
        match key:
            case "СвИП^СвАдрМЖ^АдрМЖФИАС^@Индекс":
                dd[ff]["value"] = i_d["АдрМЖФИАС"]["@Индекс"]            
            case "СвИП^СвАдрМЖ^АдрМЖФИАС^Регион":
                dd[ff]["value"] = i_d["АдрМЖФИАС"]["Регион"]
            case "СвИП^СвАдрМЖ^АдрМЖФИАС^НаимРегион":
                dd[ff]["value"] = i_d["АдрМЖФИАС"]["НаимРегион"]
            case "СвИП^СвАдрМЖ^АдрМЖФИАС^МуниципРайон^@Наим":
                dd[ff]["value"] = i_d["АдрМЖФИАС"]["МуниципРайон"]["@Наим"]
            case "СвИП^СвАдрМЖ^АдрМЖФИАС^МуниципРайон^@ВидКод":
                dd[ff]["value"] = serv_sprav["МуниципРайон"][
                    i_d["АдрМЖФИАС"]["МуниципРайон"]["@ВидКод"]
                ]
            case "СвИП^СвАдрМЖ^АдрМЖФИАС^ГородСелПоселен^@Наим":
                if "ГородСелПоселен" in i_d["АдрМЖФИАС"]:
                    dd[ff]["value"] = i_d["АдрМЖФИАС"]["ГородСелПоселен"]["@Наим"]
            case "СвИП^СвАдрМЖ^АдрМЖФИАС^ГородСелПоселен^@ВидКод":
                if "ГородСелПоселен" in i_d["АдрМЖФИАС"]:
                    dd[ff]["value"] = serv_sprav["ГородСелПоселен"][
                        i_d["АдрМЖФИАС"]["ГородСелПоселен"]["@ВидКод"]
                    ]
            case "СвИП^СвАдрМЖ^АдрМЖФИАС^НаселенПункт^@Наим":
                if "НаселенПункт" in i_d["АдрМЖФИАС"]:
                    dd[ff]["value"] = i_d["АдрМЖФИАС"]["НаселенПункт"]["@Наим"]

            case "СвИП^СвАдрМЖ^АдрМЖФИАС^НаселенПункт^@Вид":
                if "НаселенПункт" in i_d["АдрМЖФИАС"]:
                    dd[ff]["value"] = i_d["АдрМЖФИАС"]["НаселенПункт"]["@Вид"]

            case "СвИП^СвАдрМЖ^АдрМЖФИАС^ЭлПланСтруктур^@Наим":
                if "ЭлПланСтруктур" in i_d["АдрМЖФИАС"]:
                    dd[ff]["value"] = i_d["АдрМЖФИАС"]["ЭлПланСтруктур"]["@Наим"]

            case "СвИП^СвАдрМЖ^АдрМЖФИАС^ЭлПланСтруктур^@Тип":
                if "ЭлПланСтруктур" in i_d["АдрМЖФИАС"]:
                    dd[ff]["value"] = i_d["АдрМЖФИАС"]["ЭлПланСтруктур"]["@Тип"]

            case "СвИП^СвАдрМЖ^АдрМЖФИАС^ЭлУлДорСети^@Наим":
                if "ЭлУлДорСети" in i_d["АдрМЖФИАС"]:
                    dd[ff]["value"] = i_d["АдрМЖФИАС"]["ЭлУлДорСети"]["@Наим"]

            case "СвИП^СвАдрМЖ^АдрМЖФИАС^ЭлУлДорСети^@Тип":
                if "ЭлУлДорСети" in i_d["АдрМЖФИАС"]:
                    dd[ff]["value"] = i_d["АдрМЖФИАС"]["ЭлУлДорСети"]["@Тип"]

            case "СвИП^СвАдрМЖ^АдрМЖФИАС^Здание^@Номер":
                if "Здание" in i_d["АдрМЖФИАС"]:
                    if isinstance(i_d["АдрМЖФИАС"]["Здание"], dict):
                        dd[ff]["value"] = i_d["АдрМЖФИАС"]["Здание"]["@Номер"]
                    if isinstance(i_d["АдрМЖФИАС"]["Здание"], list):
                        dzdan = analiz_zdan(i_d["АдрМЖФИАС"]["Здание"])
                        dd[ff]["value"] = dzdan["@Номер"]
                        print("dzdan['@Номер'] ==>> ", dzdan["@Номер"])
            case "СвИП^СвАдрМЖ^АдрМЖФИАС^Здание^@Тип":
                if "Здание" in i_d["АдрМЖФИАС"]:
                    if isinstance(i_d["АдрМЖФИАС"]["Здание"], dict):
                        dd[ff]["value"] = i_d["АдрМЖФИАС"]["Здание"]["@Тип"]
                    if isinstance(i_d["АдрМЖФИАС"]["Здание"], list):
                        dzdan = analiz_zdan(i_d["АдрМЖФИАС"]["Здание"])
                        dd[ff]["value"] = dzdan["@Тип"]
                        print("dzdan['@Тип'] ==>> ", dzdan["@Тип"])

            case "СвИП^СвАдрМЖ^АдрМЖФИАС^ПомещЗдания^@Тип":
                if "ПомещЗдания" in i_d["АдрМЖФИАС"]:
                    dd[ff]["value"] = i_d["АдрМЖФИАС"]["ПомещЗдания"]["@Тип"]

            case "СвИП^СвАдрМЖ^АдрМЖФИАС^ПомещЗдания^@Номер":
                if "ПомещЗдания" in i_d["АдрМЖФИАС"]:
                    dd[ff]["value"] = i_d["АдрМЖФИАС"]["ПомещЗдания"]["@Номер"]

            case "СвИП^СвАдрМЖ^АдрМЖФИАС^ПомещКвартиры^@Номер":
                if "ПомещКвартиры" in i_d["АдрМЖФИАС"]:
                    dd[ff]["value"] = i_d["АдрМЖФИАС"]["ПомещКвартиры"]["@Номер"]

            case "СвИП^СвАдрМЖ^АдрМЖФИАС^ПомещКвартиры^@Тип":
                if "ПомещКвартиры" in i_d["АдрМЖФИАС"]:
                    dd[ff]["value"] = i_d["АдрМЖФИАС"]["ПомещКвартиры"]["@Тип"]

            case "Файл^Документ^СвИП^СвАдрМЖ^ГРНИПДата^@ГРНИП":
                dd[ff]["value"] = adrf["ГРНИПДата"]["@ГРНИП"]
            case "Файл^Документ^СвИП^СвАдрМЖ^ГРНИПДата^@ДатаЗаписи":
                dd[ff]["value"] = adrf["ГРНИПДата"]["@ДатаЗаписи"]

        if key in ("rec_src", "statement_dt", "local_company_pk", "ogrn", "hash_diff"):
            dd[ff]["value"] = base_data[key]

    return dd


def address_info_egrip(i_d: dict, fformat: str,
                       schema: str, egrul_egrip,
                       base_data: dict):
    """_summary_

    Args:
        i_d (dict): _description_
        fformat (str): _description_

    Returns:
        _type_: _description_
    """

    if "@ВидАдрКлассиф" in i_d:
        if i_d["@ВидАдрКлассиф"] == "1":
            print(
                "адрес в выписке должен быть выведен по ГАР (ФИАС) на основании данных, содержащихся в элементе АдрМЖФИАС"
            )
            print()
            data = adr_fias_egrip(i_d, fformat, egrul_egrip, base_data)
            write_db(data, schema, NAME_TBL_FIAS_EGRIP)
        elif i_d["@ВидАдрКлассиф"] == "2":
            print(
                "адрес в выписке должен быть выведен по КЛАДР на основании данных, содержащихся в элементе АдресРФ."
            )
            print()
            data = adr_rf_egrip(i_d, fformat, egrul_egrip, base_data)
            write_db(data, schema, NAME_TBL_ARF_EGRIP)
    else:
        # Сделал 4.6
        if "АдресРФ" in i_d and "АдрМЖФИАС" in i_d:
            print("dubl АдресРФ")
        elif "АдресРФ" in i_d:
            print("only АдресРФ")
            print()
            data = adr_rf_egrip(i_d, fformat, egrul_egrip, base_data)
            write_db(data, schema, NAME_TBL_ARF_EGRIP)
        elif "АдрМЖФИАС" in i_d:
            print("only АдрМЖФИАС")
            data = adr_fias_egrip(i_d, fformat, egrul_egrip, base_data)
            write_db(data, schema, NAME_TBL_FIAS_EGRIP)
        else:
            print("Нет данных по адресу не найден")
