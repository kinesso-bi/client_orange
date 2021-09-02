from datetime import datetime
import requests


def success_log(get_app_id, get_report_type, code, message):
    with open('success.txt', 'a+') as file_object:
        log = '{}_{}_{}_{}_{}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), get_app_id, get_report_type,
                                      code, message)
        file_object.seek(0)
        logs = file_object.read(100)
        if len(logs) > 0:
            file_object.write('\n')
            file_object.write(log)
        else:
            file_object.write(log)


def error_log(get_app_id, get_report_type, error_code, message):
    with open('error.txt', 'a+') as file_object:
        log = '{}_{}_{}_{}_{}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), get_app_id, get_report_type,
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
    else:
        stream = response.text
        return stream.split('\n')[1:-1]


