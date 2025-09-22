import os
import sys

# Add path to modules in system
new_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(new_path)
new_path2 = os.path.join(new_path, 'modules')
sys.path.append(new_path2)

# Import base class
from base_egrul_egrip import Egrul_egrip

if __name__ == '__main__':
    b_egr = Egrul_egrip()
    # list_razd = [['Файл', 'Документ', 'СвЮЛ', 'СвАдресЮЛ',	'АдресРФ']]
    # list_razd = [['Файл', 'Документ', 'СвЮЛ', 'СвАдресЮЛ',	'АдресРФ', 'Город']]
    list_razd = [['Файл', 'Документ', 'СвЮЛ']]
    # list_razd = []
    for name_file in [
        os.path.join(new_path, 'dataset', 'egrul', 'push_mes_egrul.xml'),
        # os.path.join(new_path, 'dataset', 'egrip', 'push_mes_egrip.xml')
        # os.path.join(new_path, 'dataset', 'egrul', 'error_push_egrul.xml')
        # os.path.join(new_path, 'dataset', 'egrip', '304010131600079.xml')
        # os.path.join(new_path, 'dataset', 'egrip', '323420500054722_error.xml')
    ]:
        if list_razd:
            for razd in list_razd:
                print("name_file ", name_file)
                print("razd ", razd)
                if os.path.exists(name_file):
                    b_egr.read_egrul_egrip_file_xml(name_file, razd)
                else:
                    print(f"File not found: {name_file}")
        else:
            print("name_file ", name_file)
            if os.path.exists(name_file):
                b_egr.read_egrul_egrip_file_xml(name_file, None)
            else:
                print(f"File not found: {name_file}")
