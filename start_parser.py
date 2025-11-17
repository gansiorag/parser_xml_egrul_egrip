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
    # list_razd = [['Файл', 'Документ', 'СвЮЛ']]
    # list_razd = [['Файл', 'Документ', 'СвИП', 'СвОКВЭД']]
    # list_razd = [['Файл', 'Документ', 'СвЮЛ', 'СвОКВЭД']]
    list_razd = [['ЕГРЮЛ', 'СвЮЛ', 'СвОКВЭД', 'СвОКВЭДОсн', '@КодОКВЭД']]
    # list_razd = [['ЕГРЮЛ', 'СвЮЛ', 'СвУчредит']]
    # list_razd = []
    key_bd = 'y' # 'y' 'n' запись данных в таблицу разделов, базы данных.add()
    key_lev = '0'
    # уровень обрабоки файла 
    # 0 - Получение структуры до уровня описанного в list_razd
    # 1 - Получение структуры до уровня описанного в list_razd со значениями 
    # последнего поля
    # 2 - Получение всех атрибутов со значениями подраздела из list_razd

    for name_file in [
        # os.path.join(new_path, 'dataset', 'egrul', 'push_mes_egrul.xml'),
        # os.path.join(new_path, 'dataset', 'egrip', 'push_mes_egrip.xml')
        # os.path.join(new_path, 'dataset', 'egrul', 'error_push_egrul.xml')
        # os.path.join(new_path, 'dataset', 'egrul', '1123926035397.xml')
        # os.path.join(new_path, 'dataset', 'egrul/aronov', '7107133140.xml')
        os.path.join(new_path, 'dataset', 'egrul/ogrn_v2_17_11_2025', '1031000860109.xml')        
        # os.path.join(new_path, 'dataset', 'egrip', '304010131600079.xml')
        # os.path.join(new_path, 'dataset', 'egrip', '323420500054722_error.xml')
        # os.path.join(new_path, 'dataset', 'egrip/egrip_error', '304420535600015.xml')
    ]:
        if list_razd:
            for razd in list_razd:
                print("name_file ", name_file)
                print("razd ", razd)
                if os.path.exists(name_file):
                    b_egr.read_egrul_egrip_file_xml(name_file, razd, key_bd, key_lev)
                else:
                    print(f"File not found: {name_file}")
        else:
            print("name_file ", name_file)
            if os.path.exists(name_file):
                b_egr.read_egrul_egrip_file_xml(name_file, None, key_bd, key_lev)
            else:
                print(f"File not found: {name_file}")
