import os, sqlite3


def get_connect():
    """_summary_

    Returns:
        _type_: _description_
    """
    # new_path = os.path.dirname(os.path.abspath(__file__)).split('/modules/egrul')[0] + '/dataset/egrul_egrip.db'
    new_path = '/home/gansior/MyProject/parser_xml_egrul_egrip/dataset/egrul_egrip.db'
    print(new_path)
    conn = sqlite3.connect(new_path)
    curs = conn.cursor()
    return conn, curs


def get_codes_fns():
    """_summary_

    Returns:
        list: список кодов относящиеся к ликвидации
    """
    conn, cur = get_connect()
    sql = 'SELECT cod_fns FROM codes_fns where is_active = "true";'
    cur.execute(sql)
    coddes = cur.fetchall()
    print(coddes)
    rezz = [item[0] for item in coddes]
    conn.close()
    return rezz


CODES_FNS = get_codes_fns()
print(CODES_FNS)
