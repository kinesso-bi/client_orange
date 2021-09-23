import json
from datetime import datetime

import mysql.connector
import pandas as pd
from google.api_core.exceptions import BadRequest, Conflict, NotFound
from google.cloud import bigquery
from google.oauth2 import service_account


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


def create_client():
    # TODO(developer): Set key_path to the path to the service account key file.
    key_path = "service_account.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project=credentials.project_id, )
    return client


def upload_data(dataset_name: str, table_name: str, input_data):
    # Call a BigQuery client object contructor.
    client = create_client()
    table_id = '{}.{}.{}'.format(client.project, dataset_name, table_name)

    # tell the client everything it needs to know to upload our csv
    table = client.get_table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
    ]

    job_config.schema = table.schema

    try:
        # load the csv into bigquery
        job = client.load_table_from_dataframe(input_data, table, job_config=job_config)
        job.result()  # Waits for table load to complete.
        print("Loaded {} rows into {}.".format(job.output_rows, table_id))

    except (BadRequest, Conflict, NotFound) as e:
        print('ERROR: {}'.format(e))


def loadbar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='#'):
    percent = ('{0:.' + str(decimals) + 'f}').format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{datetime.now().strftime("%H:%M:%S")} - {prefix} |{bar}| {percent}% {suffix}', end='\r')
    if iteration == total:
        print()


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


def db_disconnect(app_id, report_type, cnx, cursor, date_target_start, date_target_end, db_target):
    try:
        cursor.close()
        cnx.close()
        success_log(app_id, report_type, 1,
                    "Success: {} {} {}".format(db_target, date_target_start, date_target_end))
    except mysql.connector.Error as err:
        error_log(app_id, report_type, err.args[0], err.args[1])


def db_quit(app_id, report_type, cnx, cursor, date_target_start, date_target_end, db_target):
    try:
        cursor.close()
        cnx.close()
        success_log(app_id, report_type, 1,
                    "Error: {} {} {}".format(db_target, date_target_start, date_target_end))
    except mysql.connector.Error as err:
        error_log(app_id, report_type, err.args[0], err.args[1])


def migrate_data(app_id, report_type, select_function, dataset_target, table_target, date_start, date_end, script_name):
    cnx = db_connect(app_id, report_type)
    if not cnx.is_connected():
        return
    else:
        data = select_function()
        df = pd.read_sql(data, con=cnx)
        success_log(app_id, report_type, 1, "Query finished")
        print(df)
        # TODO logging and upload process
        # upload_data(dataset_name=dataset_target, table_name=table_target, input_data=df)
