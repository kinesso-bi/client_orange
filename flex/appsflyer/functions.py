import json
from datetime import datetime

import mysql.connector
import requests


def success_log(get_app_id, get_report_type, code, message):
    with open('logs.txt', 'a+') as file_object:
        log = '{}, {}, {}, {}, {}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), get_app_id, get_report_type,
                                          code, message)
        file_object.seek(0)
        logs = file_object.read(100)
        if len(logs) > 0:
            file_object.write('\n')
            file_object.write(log)
        else:
            file_object.write(log)


def error_log(get_app_id, get_report_type, error_code, message):
    with open('logs.txt', 'a+') as file_object:
        log = '{}, {}, {}, {}, {}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), get_app_id, get_report_type,
                                          error_code, message)
        file_object.seek(0)
        logs = file_object.read(100)
        if len(logs) > 0:
            file_object.write('\n')
            file_object.write(log)
        else:
            file_object.write(log)


def get_stream(get_url, get_params, get_app_id, get_report_type):
    response = requests.request('GET', url=get_url, params=get_params)
    if response.status_code != 200:
        error_log(get_app_id, get_report_type, response.status_code, response.text)
        return None
    else:
        stream = response.text
        print("stream: ", len(stream))
        return stream.split('\n')[1:-1]


def get_token(app_id, report_type):
    try:
        with open('credentials.json') as file:
            credentials = json.load(file)
        return credentials
    except FileNotFoundError as f:
        error_log(app_id, report_type, 1, f)
        return None


def db_connect(app_id, report_type):
    credentials = get_token(app_id, report_type)
    try:
        cnx = mysql.connector.connect(
            user=credentials['user'],
            password=credentials['password'],
            host=credentials['host'],
            database=credentials['database'],
            port=credentials['port'])
        return cnx
    except mysql.connector.Error as err:
        error_log(app_id, report_type, err.args[0], err.args[1])
        return None


def get_cursor(app_id, report_type, cnx):
    try:
        cursor = cnx.cursor()
        return cursor
    except mysql.connector.Error as err:
        error_log(app_id, report_type, err.args[0], err.args[1])
        return None


def db_disconnect(app_id, report_type, cnx, cursor):
    try:
        cnx.commit()
        cursor.close()
        cnx.close()
        success_log(app_id, report_type, 1, "File uploaded.")
    except mysql.connector.Error as err:
        error_log(app_id, report_type, err.args[0], err.args[1])
