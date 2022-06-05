from email import header
from random import expovariate
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

def fetch_table_data(table_name, limit):
    cnx = connect()
    cursor = cnx.cursor()
    last_id = read_last_id()
    cursor.execute(' select id, order_id,product_id, total_price , date from ' + table_name + ' where id > ' + last_id + ' and date > "2019-01-01" order by id limit '+ str(limit) ) 
    header = [row[0] for row in cursor.description]
    rows = cursor.fetchall()
    cnx.close()
    return header, rows 

def write_last_id(last_id):
    f = open('last_id_detail.txt', 'w')
    f.write(str(last_id))
    f.close()

def read_last_id():
    if os.path.isfile('last_id_detail.txt'):
        last_id = open('last_id_detail.txt', 'r').read()
        return last_id
    else:
        return '0'

def write_start_id(start_id):
    f = open('start_id_detail.txt', 'w')
    f.write(str(start_id))
    f.close()

def write_last_date(last_date):
    f = open('last_date_detail.txt', 'w')
    f.write(str(last_date))
    f.close()

def export(table_name,limit,with_header=False):
    header, rows = fetch_table_data(table_name, limit)

    f = open('order_detail.csv', 'a')

    if with_header:
        f.write(','.join(header) + '\n')
    for row in rows:
        f.write(','.join(str(r) for r in row) + '\n')
        last_id=row[0]
        last_date= row[4]
    all_id = [x[0] for x in rows]
    start_id = all_id[0]
    f.close()
    print(str(len(rows)) + ' rows written successfully to ' + f.name)
    write_last_id(last_id)
    write_last_date(last_date)
    write_start_id(start_id)
