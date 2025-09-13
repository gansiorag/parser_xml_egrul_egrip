'''
Модуль содержит процедуры для обработки и записи данных из EGRIP
раздел "СвОКВЭД"

Author: Gansior Alexander, gansior@gansior.ru, +79173383804
Starting 2025/06/25
Ending 2025//

'''

import sys
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

new_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(new_path + '/')


from modules.egrul.com_f import get_zerro_data, get_value, hash_f, write_db
from modules.egrul.com_f import cnst

NAME_TBL_SV = "s_individual_entrepreneur_okveds_info"
EGRUL_EGRIP = "EGRIP"
CV = cnst()

def egrip_svokved(main_doc: dict, format_egrip: str, base_data: dict) -> None:
    """Process EGRIP data and write to the database.

    Args:
        main_doc (dict): The main document containing EGRIP data.
        format_egrip (str): The format of the EGRIP data.
        base_data (dict): Base data for processing, including ogrnip.

    Raises:
        KeyError: If required keys are missing in base_data.
        ValueError: If data structures are invalid.
    """
    try:
        data = get_zerro_data(format_egrip, NAME_TBL_SV, EGRUL_EGRIP)
        data['ogrnip']['value'] = base_data["ogrnip"]
    except KeyError as e:
        logger.error(f"Missing required key in base_data: {e}")
        raise

    # Process main OKVED information
    if 'СвОКВЭДОсн' in main_doc:
        svokved = main_doc['СвОКВЭДОсн']
        try:
            data = update_data_with_svokved_egrip(data, svokved, base_data, CV["schema_get"])
        except Exception as e:
            logger.error(f"Error processing main SVOKVED: {e}")
            raise

    # Process additional OKVED data
    if 'СвОКВЭДДоп' in main_doc:
        svokved_list = main_doc['СвОКВЭДДоп']
        if isinstance(svokved_list, list):
            logger.info(f"Processing {len(svokved_list)} additional SVOKVED entries")
            for row in svokved_list:
                try:
                    process_additional_svokved_egrip(row, data, base_data, CV["schema_get"])
                except Exception as e:
                    logger.error(f"Error processing additional SVOKVED row {row}: {e}")
                    raise
        else:
            logger.error("Error: svokved_list is not a list.")
            raise ValueError("svokved_list must be a list")


def update_data_with_svokved_egrip(data: dict, svokved: dict, base_data: dict, schema: str) -> dict:
    """Update data with main OKVED information from СвОКВЭД.

    Args:
        data (dict): Data structure to update.
        svokved (dict): Main SVOKVED data.
        base_data (dict): Base data including ogrnip.
        schema (str): Database schema.

    Returns:
        dict: Updated data.

    Raises:
        KeyError: If required keys are missing.
    """
    i_d = {'СвИП': {'СвОКВЭД': {'СвОКВЭДОсн': svokved}}}

    for field_tab, dd in data.items():
        if 'СвОКВЭДОсн' in dd["key"]:
            dd["value"] = get_value(i_d, dd["key"])
            logger.debug(f"{field_tab}: {dd['key']} = {dd['value']}")

    try:
        base_data["hash_diff"] = hash_f(base_data["ogrnip"] + svokved["ГРНИПДата"]["@ГРНИП"])
    except KeyError as e:
        logger.error(f"Missing key in svokved: {e}")
        raise

    for _, sect in data.items():
        if sect["key"] in base_data:
            sect["value"] = base_data[sect["key"]]

    logger.debug(f"Updated data: {data}")
    write_db(data, schema, NAME_TBL_SV)
    return data


def process_additional_svokved_egrip(row: dict, data: dict, base_data: dict, schema: str) -> None:
    """Process additional SVOKVED data.

    Args:
        row (dict): Individual SVOKVED entry.
        data (dict): Data structure to update.
        base_data (dict): Base data including ogrnip.
        schema (str): Database schema.

    Raises:
        KeyError: If required keys are missing in row.
    """
    logger.debug(f"Processing row: {row}")
    i_d = {'СвИП': {'СвОКВЭД': {'СвОКВЭДДоп': row}}}

    for field_tab in data:
        if 'СвОКВЭДДоп' in data[field_tab]["key"]:
            data[field_tab]["value"] = get_value(i_d, data[field_tab]["key"])

    try:
        base_data["hash_diff"] = hash_f(
            base_data["ogrnip"] + row["ГРНИПДата"]["@ГРНИП"] + row['@КодОКВЭД']
        )
    except KeyError as e:
        logger.error(f"Missing key in row: {e}")
        raise

    data["hash_diff"]["value"] = base_data["hash_diff"]

    # Write to the database
    write_db(data, schema, NAME_TBL_SV)
