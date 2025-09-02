import sys

new_path = "/opt/airflow2/"
sys.path.append(new_path)

from modules.egrul.com_f import get_zerro_data, write_db, get_gar, get_gar_id, write_adress_fias_mapping

NAME_TBL_FIAS = 's_local_company_address_fias_info'
NAME_TBL_ARF = 's_local_company_address_info'


def adr_rf(i_d, fformat, egrul_egrip, base_data: dict, cv: dict, schema):
    """_summary_
     АдресРФ

    Args:
        i_d (_type_): _description_
        fformat (_type_): _description_

    Returns:
        _type_: _description_
    """

    dd = get_zerro_data(fformat, NAME_TBL_ARF, egrul_egrip)
    print(dd)
    print()
    s_data = i_d["АдресРФ"]
    print(s_data)
    print()

    for ff in dd:
        print(dd[ff]["key"])
        key = dd[ff]["key"]
        match key:
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^@КодАдрКладр":
                if "@КодАдрКладр" in s_data:
                    dd[ff]["value"] = s_data["@КодАдрКладр"]
                    print(dd[ff]["value"])
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^@КодРегион":
                dd[ff]["value"] = s_data["@КодРегион"]
                print(dd[ff]["value"])
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^@Индекс":
                print("===СвЮЛ^СвАдресЮЛ^АдресРФ^@Индекс===")
                if "@Индекс" in s_data:
                    print(s_data["@Индекс"])
                    print(dd[ff])
                    dd[ff]["value"] = s_data["@Индекс"]
                    print(dd[ff]["value"])
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^Регион^@ТипРегион":
                if "@ТипРегион" in s_data["Регион"]:
                    dd[ff]["value"] = s_data["Регион"]["@ТипРегион"]
                    print(dd[ff]["value"])
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^Регион^@НаимРегион":
                dd[ff]["value"] = s_data["Регион"]["@НаимРегион"]
                print(dd[ff]["value"])
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^Город^@ТипГород":
                if "Город" in s_data:
                    dd[ff]["value"] = s_data["Город"]["@ТипГород"]
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^Город^@НаимГород":
                if "Город" in s_data:
                    dd[ff]["value"] = s_data["Город"]["@НаимГород"]
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^НаселПункт^@ТипНаселПункт":
                if "НаселПункт" in s_data:
                    if "ТипНаселПункт" in s_data["НаселПункт"]:
                        dd[ff]["value"] = s_data["НаселПункт"]["@ТипНаселПункт"]
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^НаселПункт^@НаимНаселПункт":
                if "НаселПункт" in s_data:
                    dd[ff]["value"] = s_data["НаселПункт"]["@НаимНаселПункт"]
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^Район^@ТипРайон":
                if "Район" in s_data:
                    dd[ff]["value"] = s_data["Район"]["@ТипРайон"]
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^Район^@НаимРайон":
                if "Район" in s_data:
                    dd[ff]["value"] = s_data["Район"]["@НаимРайон"]
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^Улица^@ТипУлица":
                if "Улица" in s_data:
                    dd[ff]["value"] = s_data["Улица"]["@ТипУлица"]
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^Улица^@НаимУлица":
                if "Улица" in s_data:
                    dd[ff]["value"] = s_data["Улица"]["@НаимУлица"]
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^@Дом":
                if "@Дом" in s_data:
                    dd[ff]["value"] = s_data["@Дом"]
                    print(dd[ff]["value"])
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^@Корпус":
                if "@Корпус" in s_data:
                    dd[ff]["value"] = s_data["@Корпус"]
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^@Кварт":
                if "@Кварт" in s_data:
                    dd[ff]["value"] = s_data["@Кварт"]
                    print(dd[ff]["value"])
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^ГРНДата^@ГРН":
                dd[ff]["value"] = s_data["ГРНДата"]["@ГРН"]
            case "СвЮЛ^СвАдресЮЛ^АдресРФ^ГРНДата^@ДатаЗаписи":
                dd[ff]["value"] = s_data["ГРНДата"]["@ДатаЗаписи"]

        if key in ("rec_src", "statement_dt", "local_company_pk", "ogrn", "hash_diff"):
            dd[ff]["value"] = base_data[key]

        if "СвНедАдресЮЛ" in i_d:
            s_data2 = i_d["СвНедАдресЮЛ"]
            match dd[ff]["key"]:
                case "СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ^@ПризнНедАдресЮЛ":
                    dd[ff]["value"] = s_data2["@ПризнНедАдресЮЛ"]
                case "СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ^@ТекстНедАдресЮЛ":
                    dd[ff]["value"] = s_data2["@ТекстНедАдресЮЛ"]
                case "СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ^ГРНДата^@ГРН":
                    dd[ff]["value"] = s_data2["ГРНДата"]["@ГРН"]
                case "СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ^ГРНДата^@ДатаЗаписи":
                    dd[ff]["value"] = s_data2["ГРНДата"]["@ДатаЗаписи"]

            if "РешСудНедАдр" in s_data2:
                match dd[ff]["key"]:
                    case "СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ^РешСудНедАдр^@Дата":
                        dd[ff]["value"] = s_data2["РешСудНедАдр"]["@Дата"]
                    case "СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ@^РешСудНедАдр^@НаимСуда":
                        dd[ff]["value"] = s_data2["РешСудНедАдр"]["@НаимСуда"]
                    case "СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ@^РешСудНедАдр^@Номер":
                        dd[ff]["value"] = s_data2["РешСудНедАдр"]["@Номер"]

        if "СвРешИзмМН" in i_d:
            s_data3 = i_d["СвРешИзмМН"]
            match dd[ff]["key"]:
                case "СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ТекстРешИзмМН":
                    dd[ff]["value"] = s_data3["@ТекстРешИзмМН"]
                case "СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ГРН":
                    dd[ff]["value"] = s_data3["@ГРН"]
                case "СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ДатаЗаписи":
                    dd[ff]["value"] = s_data3["@ДатаЗаписи"]
                case "СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@НаимГород":
                    dd[ff]["value"] = s_data3["@НаимГород"]
                case "СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ТипГород":
                    dd[ff]["value"] = s_data3["@ТипГород"]
                case "СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@НаимНаселПункт":
                    dd[ff]["value"] = s_data3["@НаимНаселПункт"]
                case "СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ТипНаселПункт":
                    dd[ff]["value"] = s_data3["@ТипНаселПункт"]
                case "СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ТипРайон":
                    dd[ff]["value"] = s_data3["@ТипРайон"]
                case "СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@НаимРайон":
                    dd[ff]["value"] = s_data3["@НаимРайон"]
                case "СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@НаимРегион":
                    dd[ff]["value"] = s_data3["@НаимРегион"]
                case "СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ТипРегион":
                    dd[ff]["value"] = s_data3["@ТипРегион"]
    print('ddddddd =>', dd)
    full_addr = (
        dd["svadresul_adresrf_indeks"]["value"]
        + ", "
        + dd["svadresul_adresrf_region_tipregion"]["value"]
        + " "
        + dd["svadresul_adresrf_region_naimregion"]["value"]
        + ", "
        + dd["svadresul_adresrf_gorod_tipgorod"]["value"]
        + " "
        + dd["svadresul_adresrf_gorod_naimgorod"]["value"]
        + ", "
        + dd["svadresul_adresrf_naselpunkt_tipnaselpunkt"]["value"]
        + " "
        + dd["svadresul_adresrf_naselpunkt_naimnaselpunkt"]["value"]
        + ", "
        + dd["svadresul_adresrf_rajon_tiprajon"]["value"]
        + " "
        + dd["svadresul_adresrf_rajon_naimrajon"]["value"]
        + ", "
        + dd["svadresul_adresrf_uliza_tipuliza"]["value"]
        + " "
        + dd["svadresul_adresrf_uliza_naimuliza"]["value"]
    )
    if dd["svadresul_adresrf_dom"]["value"]:
        full_addr = full_addr + ", " + dd["svadresul_adresrf_dom"]["value"]
    if dd["svadresul_adresrf_korpus"]["value"]:
        full_addr = full_addr + ", " + dd["svadresul_adresrf_korpus"]["value"]
    full_addr = full_addr.replace(' , ' , '')
    print("full_addr == > ", full_addr)
    adr_uuid, adr_fias_f = get_gar(full_addr, cv)
    data_fias = {'adr_uuid': adr_uuid,
                 'adr_fias_f': adr_fias_f}
    write_adress_fias_mapping(data_fias, schema)
    dd["hierarchy_fulltext"] = adr_uuid
    dd["fias_guid"] = adr_fias_f
    print(dd)
    return dd


def analiz_zdan(isdt: list) -> dict:
    """_summary_

    Args:
        isdt (list): _description_

    Returns:
        dict: _description_
    """    
    dd = {'@Номер': '',
          '@Тип': ''}
    for ii in isdt:
        dd['@Номер'] += (ii['@Тип'] + ii['@Номер'])
        dd['@Тип'] += ii['@Тип']
    return dd


def adr_fias(i_d, fformat, egrul_egrip, base_data: dict, cv:dict, schema: str):
    """_summary_
        Адрес ЮЛ ФИАС
    Args:
        i_d (_type_): _description_
        fformat (_type_): _description_

    Returns:
        _type_: _description_
    """
    print(' in adr_fias ')

    serv_sprav = {'МуниципРайон': {'1': 'муниципальный район',
                                   '2': 'городской округ',
                                   '3': 'внутригородская территория города федерального значения',
                                   '4': 'муниципальный округ'},
                  'ГородСелПоселен': {'1': 'городское поселение',
                                      '2': 'сельское поселение',
                                      '3': 'межселенная территория в составе муниципального района',
                                      '4': 'внутригородской район городского округа'}
                    }
    
    dd = get_zerro_data(fformat, NAME_TBL_FIAS, egrul_egrip)
    dzdan = ''
    s_data = i_d['СвАдрЮЛФИАС']
    print(s_data)
    adr_uuid = ''
    adr_fias_f = ''
    for ff in dd:
        # print('ff ==>> ', ff)
        key = dd[ff]['key']
        match key:
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^@ИдНом':
                dd[ff]["value"] = s_data['@ИдНом']
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^@Индекс':
                dd[ff]["value"] = s_data['@Индекс']
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^Регион':
                dd[ff]["value"] = s_data['Регион']
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^НаимРегион':
                dd[ff]["value"] = s_data['НаимРегион']


            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^ГородСелПоселен^@Наим':   
                if 'ГородСелПоселен' in s_data:
                    dd[ff]["value"] = s_data['ГородСелПоселен']['@Наим']

            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^ГородСелПоселен^@ВидКод':
                if 'ГородСелПоселен' in s_data: 
                    dd[ff]["value"] = serv_sprav['ГородСелПоселен'][s_data['ГородСелПоселен']['@ВидКод']]
            
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^НаселенПункт^@Наим':
                if 'НаселенПункт' in s_data:
                    dd[ff]["value"] = s_data['НаселенПункт']['@Наим']
                    
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^НаселенПункт^@Вид':
                if 'НаселенПункт' in s_data:
                    dd[ff]["value"] = s_data['НаселенПункт']['@Вид']
            
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^МуниципРайон^@Наим':
                dd[ff]["value"] =  s_data['МуниципРайон']['@Наим']

            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^МуниципРайон^@ВидКод':
                dd[ff]["value"] = serv_sprav['МуниципРайон'][s_data['МуниципРайон']['@ВидКод']]
            
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^ЭлПланСтруктур^@Наим':
                if 'ЭлПланСтруктур' in s_data:
                    dd[ff]["value"] = s_data['ЭлПланСтруктур']['@Наим']
                
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^ЭлПланСтруктур^@Тип':
                if 'ЭлПланСтруктур' in s_data:
                    dd[ff]["value"] = s_data['ЭлПланСтруктур']['@Тип']
            
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^ЭлУлДорСети^@Наим':
                if 'ЭлУлДорСети' in s_data:
                    dd[ff]["value"] = s_data['ЭлУлДорСети']['@Наим']
                
            case'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^ЭлУлДорСети^@Тип':
                if 'ЭлУлДорСети' in s_data:
                    dd[ff]["value"] = s_data['ЭлУлДорСети']['@Тип']
                   
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^ГРНДата^@ГРН':
                dd[ff]["value"] = s_data['ГРНДата']['@ГРН']
                
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^ГРНДата^@ДатаЗаписи':
                dd[ff]["value"] = s_data['ГРНДата']['@ДатаЗаписи']
    
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^Здание^@Номер':
                if 'Здание' in s_data:
                    if isinstance(s_data['Здание'], dict):
                        dd[ff]["value"] = s_data['Здание']['@Номер']
                    if isinstance(s_data['Здание'], list):
                        dzdan = analiz_zdan(s_data['Здание'])
                        dd[ff]["value"] = dzdan['@Номер']
                        print("dzdan['@Номер'] ==>> ", dzdan['@Номер'])

            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^Здание^@Тип':
                if 'Здание' in s_data:
                    if isinstance(s_data['Здание'], dict):
                        dd[ff]["value"] =  s_data['Здание']['@Тип']
                    if isinstance(s_data['Здание'], list):
                        dzdan = analiz_zdan(s_data['Здание'])
                        dd[ff]["value"] = dzdan['@Тип']
                        print("dzdan['@Тип'] ==>> ", dzdan['@Тип'])
                
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^ПомещЗдания^@Тип':
                if 'ПомещЗдания' in s_data:
                    dd[ff]["value"] = s_data['ПомещЗдания']['@Тип']
                
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^ПомещЗдания^@Номер':
                if 'ПомещЗдания' in s_data:
                    dd[ff]["value"] = s_data['ПомещЗдания']['@Номер']
                
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^ПомещКвартиры^@Номер':
                if 'ПомещКвартиры' in s_data:
                    dd[ff]["value"] = s_data['ПомещКвартиры']['@Номер']
                
            case 'СвЮЛ^СвАдресЮЛ^СвАдрЮЛФИАС^ПомещКвартиры^@Тип':
                if 'ПомещКвартиры' in s_data:
                    dd[ff]["value"] = s_data['ПомещКвартиры']['@Тип']

        if key in ('rec_src', 'statement_dt', 'local_company_pk', 'ogrn', 'hash_diff'):
            dd[ff]["value"] = base_data[key]

        if 'СвНедАдресЮЛ' in i_d:
            s_data2 = i_d['СвНедАдресЮЛ']
            match dd[ff]['key']:
                case 'СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ^@ПризнНедАдресЮЛ':
                    dd[ff]["value"] = s_data2['@ПризнНедАдресЮЛ']
                case 'СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ^@ТекстНедАдресЮЛ':
                    dd[ff]["value"] = s_data2['@ТекстНедАдресЮЛ']
                case 'СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ^ГРНДата^@ГРН':
                    dd[ff]["value"] = s_data2['ГРНДата']['@ГРН']
                case 'СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ^ГРНДата^@ДатаЗаписи':
                    dd[ff]["value"] = s_data2['ГРНДата']['@ДатаЗаписи']
            if 'РешСудНедАдр' in s_data2:
                match dd[ff]['key']:
                    case 'СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ^РешСудНедАдр^@Дата':
                        dd[ff]["value"] = s_data2['РешСудНедАдр']['@Дата']
                    case 'СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ@^РешСудНедАдр^@НаимСуда':
                        dd[ff]["value"] = s_data2['РешСудНедАдр']['@НаимСуда']
                    case 'СвЮЛ^СвАдресЮЛ^СвНедАдресЮЛ@^РешСудНедАдр^@Номер':
                        dd[ff]["value"] = s_data2['РешСудНедАдр']['@Номер']

        if 'СвРешИзмМН' in i_d:
            s_data3 = i_d["СвРешИзмМН"]        
            match dd[ff]['key']:
                case 'СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ТекстРешИзмМН':
                    dd[ff]["value"] = s_data3["@ТекстРешИзмМН"]
                case 'СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ГРН':
                    dd[ff]["value"] = s_data3["@ГРН"]
                case 'СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ДатаЗаписи':
                    dd[ff]["value"] = s_data3["@ДатаЗаписи"]
                case 'СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@НаимГород':
                    dd[ff]["value"] = s_data3["@НаимГород"]
                case 'СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ТипГород':
                    dd[ff]["value"] = s_data3["@ТипГород"]
                case 'СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@НаимНаселПункт':
                    dd[ff]["value"] = s_data3["@НаимНаселПункт"]
                case 'СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ТипНаселПункт':
                    dd[ff]["value"] = s_data3["@ТипНаселПункт"]
                case 'СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ТипРайон':
                    dd[ff]["value"] = s_data3["@ТипРайон"]
                case 'СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@НаимРайон':
                    dd[ff]["value"] = s_data3["@НаимРайон"]
                case 'СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@НаимРегион':
                    dd[ff]["value"] = s_data3["@НаимРегион"]
                case 'СвЮЛ^СвАдресЮЛ^СвРешИзмМН^@ТипРегион':
                    dd[ff]["value"] = s_data3["@ТипРегион"]
    if dd['svadresul_adresfias_idnom']["value"]:
        adr_uuid, adr_fias_f = get_gar_id(
            dd['svadresul_adresfias_idnom']["value"],
            cv)
    else:
        full_addr = (
            dd['svadresul_adresfias_indeks']["value"]
            + ", "
            + dd["svadresul_adresfias_region_naimregion"]["value"]
            + ", "
            + dd['svadresul_adresfias_gorodselposel_vidcod']["value"]
            + " "
            + dd['svadresul_adresfias_gorodselposel_naimgorod']["value"]
            + ", "
            + dd['svadresul_adresfias_naselpunkt_vid']["value"]
            + " "
            + dd['svadresul_adresfias_naselpunkt_naimnaselpunkt']["value"]
            + ", "
            + dd['svadresul_adresfias_rajon_vidkod']["value"]
            + " "
            + dd['svadresul_adresfias_rajon_naimrajon']["value"]
            + ", "
            + dd['svadresul_adresfias_elplanstruktur_tip']["value"]
            + " "
            + dd['svadresul_adresfias_elplanstruktur_naim']["value"]
            + ", "
            + dd['svadresul_adresfias_eluldorseti_tip']["value"]
            + " "
            + dd['svadresul_adresfias_eluldorseti_naim']["value"]
        )       
        if dd['svadresul_adresfias_pomeszdania_tip']["value"]:
            full_addr = full_addr + ", " + dd['svadresul_adresfias_pomeszdania_tip']["value"]
        if dd['svadresul_adresfias_pomeskvart_nomer']["value"]:
            full_addr = full_addr + " " + dd['svadresul_adresfias_pomeskvart_nomer']["value"]
        print("full_addr == > ", full_addr)
        adr_uuid, adr_fias_f = get_gar(full_addr, cv)
    dd["fias_guid"] = adr_uuid
    dd["hierarchy_fulltext"] = adr_fias_f
    data_fias = {'adr_uuid': dd["fias_guid"],
                'adr_fias_f': dd["hierarchy_fulltext"]}
    write_adress_fias_mapping(data_fias, schema)
    return dd


def address_info(i_d: dict,
                 fformat: str,
                 egrul_egrip,
                 base_data: dict,
                 cv: dict):
    """_summary_

    Args:
        i_d (dict): _description_
        fformat (str): _description_

    Returns:
        _type_: _description_
    """
    schema = cv["schema_get"]
    if '@ВидАдрКлассиф' in i_d:
        if i_d['@ВидАдрКлассиф'] == '1':
            print('адрес в выписке должен быть выведен по ГАР (ФИАС) на\
                основании данных, содержащихся в элементе СвАдрЮЛФИАС')
            print()
            data = adr_fias(i_d, fformat, egrul_egrip, base_data, cv, schema)
            write_db(data, schema, NAME_TBL_FIAS)
        elif i_d['@ВидАдрКлассиф'] == '2':
            print('адрес в выписке должен быть выведен по КЛАДР на основании\
                  данных, содержащихся в элементе АдресРФ.')
            print()
            data = adr_rf(i_d, fformat, egrul_egrip, base_data, cv, schema)
            write_db(data, schema, NAME_TBL_ARF) 
    else:
        if 'АдресРФ' in i_d and 'СвАдрЮЛФИАС' in i_d:
            print('dubl АдресРФ')
        elif 'АдресРФ' in i_d:
            print('only АдресРФ')
            print()
            data = adr_rf(i_d, fformat, egrul_egrip, base_data, cv, schema)
            write_db(data, schema, NAME_TBL_ARF)
        elif 'СвАдрЮЛФИАС' in i_d:
            print('only СвАдрЮЛФИАС')
            data = adr_fias(i_d, fformat, egrul_egrip, base_data, cv, schema)
            write_db(data, schema, NAME_TBL_FIAS) 
        else:
            print('Нет данных по адресу не найден')
    return data
