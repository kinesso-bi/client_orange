import json
import os
from csv import reader
from datetime import datetime
import smtplib
import mysql.connector
import requests


def success_log(get_app_id, get_report_type, code, message):
    path = os.path.dirname(os.path.realpath(__file__))
    with open('{}/logs.txt'.format(path), 'a+') as file_object:
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
    path = os.path.dirname(os.path.realpath(__file__))
    with open('{}/errors.txt'.format(path), 'a+') as file_object:
        log = '{}, {}, {}, {}, {}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), get_app_id, get_report_type,
                                          error_code, message)
        file_object.seek(0)
        logs = file_object.read(100)
        if len(logs) > 0:
            file_object.write('\n')
            file_object.write(log)
        else:
            file_object.write(log)


def mail_log(get_app_id, get_report_type, error_code, message):
    path = os.path.dirname(os.path.realpath(__file__))
    with open('{}/mail.txt'.format(path), 'a+') as file_object:
        log = '{}, {}, {}, {}, {}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), get_app_id, get_report_type,
                                          error_code, message)
        file_object.seek(0)
        logs = file_object.read(100)
        if len(logs) > 0:
            file_object.write('\n')
            file_object.write(log)
        else:
            file_object.write(log)


def send_mail_log(recipients):
    path = os.path.dirname(os.path.realpath(__file__))
    with open('{}/mail.txt'.format(path)) as file_object:
        lines = file_object.read()

    credentials = get_token(0, 0)
    gmail_user = credentials["gmail_user"]
    gmail_password = credentials["gmail_password"]
    sent_from = gmail_user
    to = recipients
    subject = 'Flex - MySQL upload notification'
    body = lines

    email_text = """\
    From: %s
    To: %s
    Subject: %s

    %s
    """ % (sent_from, ", ".join(to), subject, body)

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(gmail_user, gmail_password)
        server.sendmail(sent_from, to, email_text)
        server.close()
        print('Email sent!')
    except:
        print('Something went wrong...')


def get_stream(get_url, get_params, get_app_id, get_report_type):
    # success_log(get_app_id, get_report_type, 1, "File downloading started.")
    response = requests.request('GET', url=get_url, params=get_params)
    if response.status_code != 200:
        error_log(get_app_id, get_report_type, response.status_code, response.text)
        mail_log(get_app_id, get_report_type, response.status_code, response.text)
        return None
    else:
        stream = response.text
        result = stream.split('\n')[1:-1]
        filesize = len(result)
        print("stream: ", filesize)
        success_log(get_app_id, get_report_type, 1, "File downloading finished. Rows: {}".format(filesize))
        mail_log(get_app_id, get_report_type, 1, "File downloading finished. Rows: {}".format(filesize))
        return result


def loadbar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='#'):
    percent = ('{0:.' + str(decimals) + 'f}').format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{datetime.now().strftime("%H:%M:%S")} - {prefix} |{bar}| {percent}% {suffix}', end='\r')
    if iteration == total:
        print()


def get_token(app_id, report_type):
    path = os.path.dirname(os.path.realpath(__file__))
    try:
        with open('{}/credentials.json'.format(path)) as file:
            credentials = json.load(file)
        return credentials
    except FileNotFoundError as f:
        error_log(app_id, report_type, 1, f)
        mail_log(app_id, report_type, 1, f)
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
        mail_log(app_id, report_type, err.args[0], err.args[1])
        return None


def get_cursor(app_id, report_type, cnx):
    try:
        cursor = cnx.cursor()
        return cursor
    except mysql.connector.Error as err:
        error_log(app_id, report_type, err.args[0], err.args[1])
        mail_log(app_id, report_type, err.args[0], err.args[1])
        return None


def db_disconnect(app_id, report_type, cnx, cursor, date_target_start, date_target_end, db_target):
    try:
        cnx.commit()
        cursor.close()
        cnx.close()
        success_log(app_id, report_type, 1,
                    "File uploaded: {} {} {}".format(db_target, date_target_start, date_target_end))
        mail_log(app_id, report_type, 1,
                    "File uploaded: {} {} {}".format(db_target, date_target_start, date_target_end))
    except mysql.connector.Error as err:
        error_log(app_id, report_type, err.args[0], err.args[1])
        mail_log(app_id, report_type, err.args[0], err.args[1])


def db_quit(app_id, report_type, cnx, cursor, date_target_start, date_target_end, db_target):
    try:
        cursor.close()
        cnx.close()
        error_log(app_id, report_type, 1,
                    "Error - file not uploaded: {} {} {}".format(db_target, date_target_start, date_target_end))
        mail_log(app_id, report_type, 1,
                    "Error - file not uploaded: {} {} {}".format(db_target, date_target_start, date_target_end))
    except mysql.connector.Error as err:
        error_log(app_id, report_type, err.args[0], err.args[1])
        mail_log(app_id, report_type, err.args[0], err.args[1])


def get_data(url, params, app_id, report_type, insert_function, date_start, date_end, script_name):
    data = get_stream(get_url=url, get_params=params, get_app_id=app_id, get_report_type=report_type)
    if data is None:
        return
    else:
        cnx = db_connect(app_id, report_type)
        cursor = get_cursor(app_id, report_type, cnx)
        if cursor is None:
            return
        else:
            # data_rows = len(data)
            try:
                for i, line in enumerate(reader(data)):
                    if cnx.is_connected():
                        insert_function(cursor, line)
                        # loadbar(i + 1, total=data_rows, prefix='Progress:', suffix='Complete', length=20)
                    else:
                        # TODO send error as a message
                        db_quit(app_id, report_type, cnx, cursor, date_start, date_end, script_name)
                        break
            except Exception as e:
                # TODO send error as a message
                error_log(app_id, report_type, -1, e)
                mail_log(app_id, report_type, -1, e)
                db_quit(app_id, report_type, cnx, cursor, date_start, date_end, script_name)

            db_disconnect(app_id, report_type, cnx, cursor, date_target_start=date_start,
                          date_target_end=date_end, db_target=script_name)
