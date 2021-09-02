import json
from datetime import date, timedelta

with open('credentials.json') as file:
    credentials = json.load(file)

app_id = 'com.orange.rn.dop'
report_type = 'installs_report'
yesterday = date.today() - timedelta(days=1)

params = {
    'api_token': credentials['api_token'],
    'from': yesterday,
    'to': yesterday,
    'sfx': ''
}

url = 'https://hq.appsflyer.com/export/{}/{}/v5?api_token={}{}'.format(app_id, report_type, params['api_token'],
                                                                       params['sfx'])
