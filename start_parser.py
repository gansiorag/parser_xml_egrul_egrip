import os, sys
from datetime import datetime
import json
import logging


new_path = "/home/gansior/MyProject/parser_xml_egrul_egrip/"
sys.path.append(new_path)

from modules.egrul.egrul_moduls import parser_mesage_egrul
from modules.egrul.egrip_moduls import parser_mesage_egrip
from modules.egrul.com_f import get_codes_fns, get_token
from modules.egrul.com_f import cnst, get_logger
from modules.egrul.com_f import all_tables_egrul, all_tables_egrip, get_numb_rows_table

logger = logging.getLogger("DAG_CREATE_ALL_TABLES_IMF_WB")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

logger.addHandler(console_handler)

# logger.warning("<========= PARSER START =========>")

# #COMMON VARIABLES
INPUT_EGRUL = 'ЕГРЮЛ_ОТКР_СВЕД'
INPUT_EGRIP = 'ЕГРИП_ОТКР_СВЕД'
VERS_FORMAT_EGRUL = '4.07'
VERS_FORMAT_EGRIP = '4.06'

# COMMON VARIABLES -CV
CV = cnst()


def read_egrul():
    """Reads messages from a Kafka topic, parses them, and writes data to a database."""
    logger.warning("START")
    CODES_FNS = get_codes_fns()
    print('CODES_FNS = >', CODES_FNS)
    for name_file in [new_path + 'dataset/push_mes_egrul.xml',
                      new_path + 'dataset/push_mes_egrip.xml']:
        with open(name_file, 'r', encoding="utf-8") as i_f:
            dataf = i_f.read()
            print(len(dataf))
            # Extracting data using a single split operation
            type_data = dataf.split('ТипИнф="')[1].split('" ВерсПрог=')[0]
            vers_form = dataf.split('ВерсФорм="')[1].split('" ТипИнф=')[0]
            kol_doc = int(dataf.split('КолДок="')[1].split('">')[0].strip())
            print(type_data)
            print(vers_form)
            print(kol_doc)
            if 'ТипИнф="' in dataf and 'ВерсФорм="' in dataf:
                type_data = dataf.split('ТипИнф="')[1].split('" ВерсПрог=')[0]
                vers_form = dataf.split('ВерсФорм="')[1].split('" ТипИнф=')[0]
                print(
                    f"T Y P E   M E S S A G E ================>> {type_data}"
                )
                print(
                    f"F O R M A T   M E S S A G E ================>> {vers_form}"
                )
                # token = get_token(token)
                # CV["gar"]["headers"]["Authorization"] = token["access_token"]
                if type_data == INPUT_EGRUL and vers_form == VERS_FORMAT_EGRUL:
                    parser_mesage_egrul(dataf, CODES_FNS, CV)
                if type_data == INPUT_EGRIP and vers_form == VERS_FORMAT_EGRIP:
                    parser_mesage_egrip(dataf, CODES_FNS, CV)
            else:
                logger.warning(
                    "Message format is incorrect or missing required fields."
                )
                print("Error dataf ===>>>", dataf)


if __name__ == '__main__':
    read_egrul()
