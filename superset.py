#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import psycopg2
from decimal import Decimal
from config import config

# If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1YON_qTJr90vkZ6eeJvzZZnor4kEye3RyekdJrbDrK40'
SAMPLE_RANGE_NAME = 'A2:S'

sql_select = "select * from public.budget_tracker where id={0}"
sql_insert = """INSERT INTO public.budget_tracker(id, week, date, origin, description, unit_price, type, money_in, net_price, money_out, status, transaction, owed, owned_paid, cash, balance_infact, match) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
sql_update = """UPDATE public.budget_tracker SET week=%s, date=%s, origin=%s, description=%s, unit_price=%s, type=%s, money_in=%s, net_price=%s, money_out=%s, status=%s, transaction=%s, owed=%s, owned_paid=%s, cash=%s, balance_infact=%s, match=%s WHERE id=%s"""


def select_by_id(sql, conn, id):
    rows = []
    cur = conn.cursor()
    try:
        cur.execute(sql_select.format(id))
        rows = cur.fetchall()
    except Exception as error:
        print("select fail", error, sql, id)
    finally:
        cur.close()
    return rows


def update_by_id(sql, conn, row):
    cur = conn.cursor()
    try:
        cur.execute(sql, (int(row[1]), row[2], row[3], row[4], row[5], row[6],
                          float(row[7].replace(',', '').strip()), float(row[8].replace(',', '').strip()),
                          float(row[9].replace(',', '').strip()), row[10], row[11], row[12], [13], row[14],
                          row[15], row[16], int(row[0])))
        conn.commit()
    except Exception as error:
        print("update fail", error, sql, id)
    finally:
        cur.close()


def insert(sql, conn, row):
    cur = conn.cursor()
    try:
        cur.execute(sql, (int(row[0]), int(row[1]), row[2], row[3], row[4], row[5], row[6],
                          float(row[7].replace(',', '').strip()), float(row[8].replace(',', '').strip()),
                          float(row[9].replace(',', '').strip()), row[10], row[11], row[12], [13], row[14],
                          row[15], row[16]))
        conn.commit()
    except Exception as error:
        print("insert fail", error, sql, id)
    finally:
        cur.close()


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        #############################################################
        store = file.Storage('token.json')
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
            creds = tools.run_flow(flow, store)
        service = build('sheets', 'v4', http=creds.authorize(Http()))

        # Call the Sheets API
        SPREADSHEET_ID = '1YON_qTJr90vkZ6eeJvzZZnor4kEye3RyekdJrbDrK40'
        RANGE_NAME = 'A2:S'
        result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID,
                                                     range=RANGE_NAME).execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')
        else:
            for row in values:
                #print('Table Information: {0}'.format(row))
                if len(select_by_id(sql_select, conn, row[0])) >= 1:
                    update_by_id(sql_update, conn, row)
                else:
                    insert(sql_insert, conn, row)
        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
        print('Database connection closed.')


if __name__ == '__main__':
    connect()
