import os, sys

# Add path to modules in system
new_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(new_path + '/')
print(new_path)
new_path2 = new_path + '/modules/'
sys.path.append(new_path2)

# Import base class
from base_egrul_egrip import Egrul_egrip

if __name__ == '__main__':
    b_egr = Egrul_egrip()
    for name_file in [
        #  new_path + '/dataset/egrul/push_mes_egrul.xml',
        # #  new_path + '/dataset/egrip/push_mes_egrip.xml'
        new_path + '/dataset/egrul/error_push_egrul.xml'
    ]:
        b_egr.read_egrul_egrip_file_xml(name_file)
