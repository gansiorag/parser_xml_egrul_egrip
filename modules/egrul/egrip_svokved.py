'''
Модуль содержит процедуры для обработки и записи данных из EGRIP
раздел "СвОКВЭД"

Author: Gansior Alexander, gansior@gansior.ru, +79173383804
Starting 2025/06/25
Ending 2025//

'''

import sys, os

new_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(new_path + '/')


from modules.egrul.com_f import get_zerro_data, get_value, hash_f, write_db
from modules.egrul.com_f import cnst

NAME_TBL_SV = "s_individual_entrepreneur_okveds_info"
EGRUL_EGRIP = "EGRIP"
CV = cnst()

def egrip_svokved(osn_doc: dict, format_egrip: str, base_data: dict):
    """Process EGRUL data and write to the database.

    Args:
        osn_doc (dict): The main document containing data.
        format_egrul (str): The format of the EGRUL data.
        base_data (dict): Base data for processing.
    """    
    data = get_zerro_data(format_egrip, NAME_TBL_SV, EGRUL_EGRIP)
    data['ogrnip']['value'] = base_data["ogrnip"]
    # Process основная информация
    if 'СвОКВЭДОсн' in osn_doc:
        svokved = osn_doc['СвОКВЭДОсн']
        data = update_data_with_svokved_egrip(data,
                                              svokved,
                                              base_data,
                                              CV["schema_get"])

    # Process дополнительные данные
    if 'СвОКВЭДДоп' in osn_doc:
        svokved_list = osn_doc['СвОКВЭДДоп']
        if isinstance(svokved_list, list):
            print(f"svokved dop kolvo: {len(svokved_list)}")
            print(f"svokved dop: {svokved_list}")
            for row in svokved_list:
                process_additional_svokved_egrip(row, data,
                                                 base_data,
                                                 CV["schema_get"])
        else:
            print("Error: svokved_list is not a list.")


def update_data_with_svokved_egrip(data,
                                   svokved,
                                   base_data,
                                   schema: str):
    """Update data with основная информация from СвОКВЭД."""
    i_d = {'СвИП': {'СвОКВЭД': {'СвОКВЭДОсн': svokved}}}

    for field_tab, dd in data.items():
        if 'СвОКВЭДОсн' in dd["key"]:
            dd["value"] = get_value(i_d, dd["key"])
            print(field_tab, dd["key"], dd["value"])

    base_data["hash_diff"] = hash_f(base_data["ogrnip"] +
                                    svokved["ГРНИПДата"]["@ГРНИП"])

    for key, sect in data.items():
        if sect["key"] in base_data:
            sect["value"] = base_data[sect["key"]]

    print(data)
    write_db(data, schema, NAME_TBL_SV)
    return data


def process_additional_svokved_egrip(row, data, base_data, schema):
    """Process additional СвОКВЭДДоп data."""
    print(f"Processing row: {row}")
    i_d = {'СвИП': {'СвОКВЭД': {'СвОКВЭДДоп': row}}}
    
    for field_tab in data:
        if 'СвОКВЭДДоп' in data[field_tab]["key"]:
            data[field_tab]["value"] = get_value(i_d, data[field_tab]["key"])

    base_data["hash_diff"] = (hash_f(base_data["ogrnip"] +
                                     row["ГРНИПДата"]["@ГРНИП"] +
                                     row['@КодОКВЭД']))
    data["hash_diff"]["value"] = base_data["hash_diff"]

    # Write to the database
    write_db(data, schema, NAME_TBL_SV)
