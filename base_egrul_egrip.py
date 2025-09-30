import os, sys
from datetime import datetime


new_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(new_path + "/")
print(new_path)
new_path2 = new_path + "/modules/"
sys.path.append(new_path2)

from modules.egrul.egrul_moduls import parser_egrul_mesage
from modules.egrul.egrip_moduls import parse_egrip_message
from modules.egrul.com_f import get_codes_fns
from modules.egrul.com_f import cnst, get_logger


logger = get_logger()

# COMMON VARIABLES -CV
CV = cnst()


class Egrul_egrip:

    def __init__(self, vers_format_egrip="4.06", vers_format_egrul="4.07") -> None:
        self.path_file = ()
        self.vers_format_egrip = vers_format_egrip
        self.vers_format_egrul = vers_format_egrul
        CV["vers_format_egrip"] = vers_format_egrip
        CV["vers_format_egrul"] = vers_format_egrul
        CV["format_egrip"] = f"format_{vers_format_egrip.replace('.', '_')}"
        CV["format_egrul"] = f"format_{vers_format_egrul.replace('.', '_')}"
        CV["input_egrip"] = "ЕГРИП_ОТКР_СВЕД"
        CV["input_egrul"] = "ЕГРЮЛ_ОТКР_СВЕД"

    def read_egrul_egrip_file_xml(self, name_file: str, razd: list):
        """Read from file"""
        print("START")
        CODES_FNS = get_codes_fns()
        # logger.info("CODES_FNS = >", CODES_FNS)
        with open(name_file, "r", encoding="utf-8") as i_f:
            dataf = i_f.read()
            print(f"len(dataf {len(dataf)}")
            # Extracting data using a single split operation
            if "<ЕГРИП>" in dataf:
                # dataf = dataf.replace("<ЕГРИП>", "<Файл><Документ>")
                # dataf = dataf.replace("</ЕГРИП>", "</Документ></Файл>")
                parse_egrip_message(dataf, CODES_FNS, CV, razd)
            elif "<ЕГРЮЛ>" in dataf:
                # dataf = dataf.replace("<ЕГРЮЛ>", "<Файл><Документ>")
                # dataf = dataf.replace("</ЕГРЮЛ>", "</Документ></Файл>")
                parser_egrul_mesage(dataf, CODES_FNS, CV, razd)
            elif "<?xml" in dataf:
                type_data = dataf.split('ТипИнф="')[1].split('" ВерсПрог=')[0]
                vers_form = dataf.split('ВерсФорм="')[1].split('" ТипИнф=')[0]
                kol_doc = int(dataf.split('КолДок="')[1].split('">')[0].strip())
                print(type_data)
                print(vers_form)
                print(kol_doc)

                if 'ТипИнф="' in dataf and 'ВерсФорм="' in dataf:
                    type_data = dataf.split('ТипИнф="')[1].split('" ВерсПрог=')[0]
                    vers_form = dataf.split('ВерсФорм="')[1].split('" ТипИнф=')[0]
                    print(f"T Y P E   D A T A ================>> {type_data}")
                    print(f"F O R M A T   D A T A ================>> {vers_form}")
                    # token = get_token(token)
                    # CV["gar"]["headers"]["Authorization"] = token["access_token"]
                    if (
                        type_data == CV["input_egrul"]
                        and vers_form == CV["vers_format_egrul"]
                    ):
                        parser_egrul_mesage(dataf, CODES_FNS, CV, razd)
                    if (
                        type_data == CV["input_egrip"]
                        and vers_form == CV["vers_format_egrip"]
                    ):
                        parse_egrip_message(dataf, CODES_FNS, CV)
            else:
                print("Message format is incorrect or missing required fields.")
                print("Error dataf ===>>>", dataf)
