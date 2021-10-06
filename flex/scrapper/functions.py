import json
import os
import smtplib
from google.api_core.exceptions import BadRequest, Conflict, NotFound
from google.cloud import bigquery
from google.oauth2 import service_account
import datetime


def create_client():
    # TODO(developer): Set key_path to the path to the service account key file.
    path = os.path.dirname(os.path.realpath(__file__))
    key_path = "{}/service_account.json".format(path)
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


def get_credentials():
    path = os.path.dirname(os.path.realpath(__file__))
    try:
        with open('{}/credentials.json'.format(path)) as file:
            credentials = json.load(file)
        return credentials
    except FileNotFoundError as f:
        return None


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


def get_token(app_id, report_type):
    path = os.path.dirname(os.path.realpath(__file__))
    try:
        with open('{}/credentials.json'.format(path)) as file:
            credentials = json.load(file)
        return credentials
    except FileNotFoundError as f:
        # error_log(app_id, report_type, 1, f)
        mail_log(app_id, report_type, 1, f)
        return None


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
    message = 'Subject: {}\n\nLogs:\n{}'.format(subject, body)
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(gmail_user, gmail_password)
        server.sendmail(sent_from, to, message)
        server.close()
        print('Email sent!')
    except:
        print('Something went wrong...')
