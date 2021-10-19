import datetime
import json
import os
import shutil
import smtplib
from time import sleep
from datetime import timedelta
import pandas as pd
from google.api_core.exceptions import BadRequest, Conflict, NotFound
from google.cloud import bigquery
from google.oauth2 import service_account


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


def suspension(driver):
    sleep(10)
    try:
        driver.find_element_by_class_name('account-status-modal-inner.pending-suspension')
        driver.find_element_by_xpath('/html/body/div[3]/div[2]/div/div/div[2]/div[2]/div/button').click()
        print("hello")
        exit()
    except:
        print("not found")
        pass


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def custom_dashboard(driver, path, target_date, element):
    driver.find_element_by_xpath(
        '//*[@id="export-wrapper"]/div[2]/div[{ELEMENT}]/div/div/div[1]/div/div/button'.format(ELEMENT=element)).click()
    sleep(1)
    driver.find_element_by_xpath(
        '//*[@id="export-wrapper"]/div[2]/div[{ELEMENT}]/div/div/div[1]/div/div/ul/li[4]'.format(
            ELEMENT=element)).click()
    sleep(5)
    filename = max([path + "/" + f for f in os.listdir(path) if ".csv" in f], key=os.path.getctime)
    shutil.move(filename, os.path.join(path, r"data.csv"))
    df = pd.read_csv("{}/data.csv".format(path))
    df['Date'] = target_date
    df['Platform'] = driver.find_element_by_xpath(
        '//*[@id="export-wrapper"]/div[2]/div[{ELEMENT}]/div/div/h3/div'.format(ELEMENT=element)).text.split(' ', 1)[0]
    df['Origin'] = driver.find_element_by_xpath(
        '//*[@id="custom-dashboard"]/div[2]/div[1]/div/div/div[1]/div/div[2]/div/div/div/div/span/div/div/div[2]/div/span/span/span').text
    df.rename({'tapregister (Event Counter)': 'tapregister__Event_Counter_',
               'tapactivate (Event Counter)': 'tapactivate__Event_Counter_'}, axis=1, inplace=True)
    upload_data("orange", "custom_wp_flex_per_ad", df)
