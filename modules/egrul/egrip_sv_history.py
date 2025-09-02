'''
Модуль содержит процедуры для обработки и записи данных из EGRUL
раздел СвЗапЕГРИП

Author: Gansior Alexander, gansior@gansior.ru, +79173383804
Starting 2025/04/25
Ending 2025//

'''

import sys

new_path = "/opt/airflow2/"
sys.path.append(new_path)

from modules.egrul.com_f import get_zerro_data, write_db, update_field
from modules.egrul.com_f import update_field, update_field_2, hash_f

NAME_TBL_SV = 's_individual_entrepreneur_egrip_history'
EGRIP = "EGRIP"


def write_liquid_egrip(ddis, fformat, schema:str):
    """_summary_

    Args:
        dr (_type_): _description_
        fformat (_type_): _description_
    """
    print('===== write_liquid_egrip =====')
    NAME_TBL_LIQUID = 's_individual_entrepreneur_is_liquidated_info'
    dr_new = get_zerro_data(fformat, NAME_TBL_LIQUID, EGRIP)
    print(dr_new)
    fields_to_update = {
        'СвИП^СвПрекрЮЛ^ГРНДата^@ДатаЗаписи': 'СвИП^СвЗапЕГРИП^ГРНДата^@ДатаЗап',
        'СвИП^СвПрекрЮЛ^СвРегОрг^@НаимНО': 'СвИП^СвЗапЕГРИП^СвРегОрг^@НаимНО',
        'СвИП^СвПрекрЮЛ^СвРегОрг^@КодНО': 'СвИП^СвЗапЕГРИП^СвРегОрг^@КодНО',
        'СвИП^СвПрекрЮЛ^@ДатаПрекрЮЛ': 'СвИП^СвЗапЕГРИП^ГРНДата^@ДатаЗап',
        'СвИП^СвПрекрЮЛ^ГРНДата^@ГРН': 'СвИП^СвЗапЕГРИП^ГРНДата^@ГРН',
        'СвИП^СвПрекрЮЛ^СпПрекрЮЛ^@КодСпПрекрЮЛ': 'СвИП^СвЗапЕГРИП^ВидЗап^@КодСПВЗ',
        'СвИП^СвПрекрЮЛ^СпПрекрЮЛ^@НаимСпПрекрЮЛ': 'СвИП^СвЗапЕГРИП^ВидЗап^@НаимВидЗап',  
        'rec_src': 'rec_src',
        'statement_dt': 'statement_dt',
        'individual_entrepreneur_pk': 'individual_entrepreneur_pk',
        'ogrnip': 'ogrnip',
        'hash_diff': 'hash_diff',
        }
    ddis2_mapping = {item['key']: item['value'] for item in ddis.values()}
    for field, value in fields_to_update.items():
        dr_new = update_field_2(dr_new, field, value, ddis2_mapping)
    write_db(dr_new, schema, NAME_TBL_LIQUID)


def sv_zap_egrip(ddis: dict, fformat, schema: str,
                 base_data: dict, codes_fns: list):
    """_summary_

    Args:
        ddis (dict): _description_
        fformat (_type_): _description_
        base_data (dict): _description_
        codes_fns (list): _description_
    """    
    print('sv_zap_egrip get =>> ', ddis)
    dr = get_zerro_data(fformat, NAME_TBL_SV, EGRIP)
    print(dr)
    print(base_data)
    # Define the fields to update
    fields_to_update = {
        'СвИП^СвЗапЕГРИП^@ИдЗап': ddis.get('@ИдЗап'),
        'СвИП^СвЗапЕГРИП^@ГРНИП':  '',
        'СвИП^СвЗапЕГРИП^@ДатаЗап': ddis.get('@ДатаЗап'),
        'СвИП^СвЗапЕГРИП^ВидЗап^@КодСПВЗ': ddis['ВидЗап'].get('@КодСПВЗ'),
        'СвИП^СвЗапЕГРИП^ВидЗап^@НаимВидЗап': ddis['ВидЗап'].get('@НаимВидЗап'),
        'rec_src': base_data.get('rec_src'),
        'statement_dt': base_data.get('statement_dt'),
        'individual_entrepreneur_pk': base_data.get('individual_entrepreneur_pk'),
        'ogrnip': base_data.get('ogrnip'),
        'hash_diff': base_data.get('hash_diff')
    }
    if '@ГРНИП' in ddis:
        fields_to_update['СвИП^СвЗапЕГРИП^@ГРНИП'] = ddis['@ГРНИП']
    # Update fields in a loop
    for field, value in fields_to_update.items():
        dr = update_field(dr, field, value)
    
    if 'СвРегОрг' in ddis:
        dr = update_field(dr, 'СвИП^СвЗапЕГРИП^СвРегОрг^@КодНО',
                          ddis['СвРегОрг']['@КодНО'])
        dr = update_field(dr, 'СвИП^СвЗапЕГРИП^СвРегОрг^@НаимНО',
                          ddis['СвРегОрг']['@НаимНО'])

    if ddis['ВидЗап']['@КодСПВЗ'] in codes_fns:
        print('liquid =>> ', dr)
        write_liquid_egrip(dr, fformat, schema)

    if 'СвСвид' in ddis:
        prfx = 'СвИП^СвЗапЕГРИП^СвСвид^'
        dc = ddis['СвСвид']
        dr = update_field(dr, f'{prfx}@ДатаВыдСвид', dc['@ДатаВыдСвид'])
        dr = update_field(dr, f'{prfx}@Номер', dc['@Номер'])
        dr = update_field(dr, f'{prfx}@Серия', dc['@Серия'])

    if 'СвСтатусЗап' in ddis:
        if 'ГРНИПДатаНед' in ddis['СвСтатусЗап']:
            prfx = 'СвИП^СвЗапЕГРИП^СвСтатусЗап^ГРНИПДатаНед^'
            dc = ddis['СвСтатусЗап']['ГРНИПДатаНед']
            dr = update_field(dr, f'{prfx}@ИдЗап', dc['@ИдЗап'])
            dr = update_field(dr, f'{prfx}@ГРНИП', dc['@ГРНИП'])
            dr = update_field(dr, f'{prfx}@ДатаЗап', dc['@ДатаЗап'])

    if 'ГРНИПДатаНедПред' in ddis:
        prfx = 'СвИП^СвЗапЕГРИП^ГРНИПДатаНедПред^'
        dc = ddis['ГРНИПДатаНедПред']
        dr = update_field(dr, f'{prfx}@ИдЗап',dc['@ИдЗап'])
        if '@ГРНИП' in dc:
            dr = update_field(dr, f'{prfx}@ГРНИП', dc['@ГРНИП'])
        dr = update_field(dr, f'{prfx}@ДатаЗап', dc['@ДатаЗап'])
        
    if 'СведПредДок' in ddis:
        data_source = ddis['СведПредДок']
        field_prefix = 'СвИП^СвЗапЕГРИП^СведПредДок^'
        fields = ['НаимДок', 'НомДок', 'ДатаДок']

        # Очистка всех полей перед началом обработки
        for field in fields:
            dr = update_field(dr, f'{field_prefix}{field}', '')

        if isinstance(data_source, list):
            print('СведПредДок  list length =>> ', len(data_source))
            for s_p_d, dd1 in enumerate(data_source):
                print(f'{"="*3} СведПредДок start dd1 {s_p_d} {"="*3}', dd1)
                print(f'{"="*3} СведПредДок start dr {s_p_d} {"="*3}', dr)

                for field in fields:
                    if field in dd1:
                        dr = update_field(dr, f'{field_prefix}{field}',
                                          dd1[field])
                        dr['hash_diff']['value'] = hash_f(
                            dr['hash_diff']['value'] +
                            dd1[field])
                print(f'{"="*3} СведПредДок обагощен {s_p_d} {"="*3}', dr)
                write_db(dr, schema, NAME_TBL_SV)

        elif isinstance(data_source, dict):
            print('СведПредДок dict')
            print(data_source)

            for field in fields:
                if field in data_source:
                    dr = update_field(dr, f'{field_prefix}{field}',
                                      data_source[field])

            print(dr)
            write_db(dr, schema, NAME_TBL_SV)
    else:
        print('СведПредДок not')
        print(dr)
        write_db(dr, schema, NAME_TBL_SV)
