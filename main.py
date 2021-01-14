from phpserialize import unserialize
import mysql.connector
import json
from datetime import datetime as dt
from collections import ChainMap
from clickhouse_driver import Client


def convert(data: dict):
    if isinstance(data, bytes):
        try:
            return json.loads(data.decode('utf8', errors="ignore").replace('\u0000', ''))
        except json.decoder.JSONDecodeError:
            return data.decode('utf8', errors="ignore").replace('\u0000', '')
    if isinstance(data, (str, int)):
        return str(data)
    if isinstance(data, dict):
        return dict(map(convert, data.items()))
    if isinstance(data, tuple):
        return tuple(map(convert, data))
    if isinstance(data, list):
        return list(map(convert, data))
    if isinstance(data, set):
        return set(map(convert, data))
    if isinstance(data, ChainMap):
        for i in data.maps:
            if isinstance(i, dict):
                return dict(map(convert, i.items()))
            if isinstance(i, list):
                return list(map(convert, i))
    return data


def insert_data(data: list):
    client = Client('localhost')
    client.execute("Insert into test.php_local (col1, col2, col3) VALUES ", data)
    client.disconnect()


def _parse_unserialize(data: bytes):
    if len(data) > 0:
        output_data = unserialize(
            data,
            object_hook=ChainMap,
        )
        # ======================================
        # print("\n", output_data, "\n")
        # ======================================
        output_data = convert(output_data)
        # ======================================
        # print("\n", output_data, "\n")
        # ======================================
        return output_data


def check_none_str(data):
    if data is None:
        return ''
    elif data == 'false':
        return ''
    else:
        return data


def check_instance(data):
    if isinstance(data, str):
        return data.encode("utf8", errors="ignore")
    else:
        return data


mydb = mysql.connector.connect(
    host="",
    user="",
    password="",
    database="",
    port=3365,
    use_unicode=True,
    charset='utf8',
)

my_cursor = mydb.cursor()
my_cursor.execute("SELECT * from test.example;")

# ======================================
begin_time = dt.now()
print(f"Fetching data MySQL ----------->  {begin_time} hours !!!!")
# ======================================
my_result = list(my_cursor.fetchall())
# ======================================
print(f"Total time taken to fetch data ----------->  {dt.now() - begin_time} !!!!\n")
# ======================================

row = []
# ======================================
begin_time = dt.now()
print(f"Script Started from ----------->  {begin_time} hours !!!!")
# ======================================
for x in my_result:

    # ======================================
    # print("\n", x[-1], "\n")
    # ======================================
    data_to_unserialize = check_instance(x[-1])
    output = _parse_unserialize(data_to_unserialize)
    # ======================================
    struct_data = json.dumps(
        output,
        indent=4,
        ensure_ascii=False,
    )
    # ======================================
    print(struct_data, "\n")
    # ======================================
    row.append(
        (
            x[0],
            data_to_unserialize,
            struct_data,
        )
    )
    # print(row)
print(f"Total time taken ----------->  {dt.now() - begin_time} !!!!")
# insert_data(row)
