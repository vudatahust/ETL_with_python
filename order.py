from email import header
from sqlite3 import Cursor
import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv(".env")

def connect():
    MYSQL_HOST = os.environ.get("MYSQL_HOST")
    MYSQL_USER = os.environ.get("MYSQL_USER")
    MYSQL_PASS = os.environ.get("MYSQL_PASS")
    MYSQL_DB = os.environ.get("MYSQL_DB")
    cnx = mysql.connector.connect(
        host=MYSQL_HOST,
        database=MYSQL_DB,
        user=MYSQL_USER,
        password=MYSQL_PASS
    )
    return cnx

def fetch_table_data(table_name):
    cnx = connect()
    cursor = cnx.cursor()
    last_id = read_last_id()
    start_id_detail = read_start_id_detail()
    cursor.execute(' select a.id, a.discount, (b.total_price - a.discount) as total_price, b.quantity, a.order_status, b.date from ' + table_name + ' as a inner join ( select order_id, sum(total_price) as total_price, count(product_id) as quantity , date from order_detail  where id > ' + start_id_detail + ' and id < ' + last_id + ' group by order_id ) as b on a.id = b.order_id' )
    header = [row[0] for row in cursor.description]
    rows = cursor.fetchall()
    cnx.close()
    return header, rows 
def read_last_id():
    last_id = open('last_id_detail.txt', 'r').read()
    return last_id

def read_start_id_detail():
    last_id = open('start_id_detail.txt', 'r').read()
    return last_id

def export(table_name,with_header=False):
    header, rows = fetch_table_data(table_name)
    f = open('order.csv', 'a')
    if with_header:
        f.write(','.join(header) + '\n')
    for row in rows:
        f.write(','.join(str(r) for r in row) + '\n')
    f.close()
    print(str(len(rows)) + ' rows written successfully to ' + f.name)
