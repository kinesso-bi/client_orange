import json
from datetime import date, timedelta
import requests

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


def get_stream(get_url, get_params):
    response = requests.request('GET', url=get_url, params=get_params)
    if response.status_code != 200:
        if response.status_code == 400:
            # TODO error log
            print("")
        else:
            # TODO error log
            print("")
    else:
        stream = response.text
        return stream.split('\n')[1:-1]


data = get_stream(url, params)
