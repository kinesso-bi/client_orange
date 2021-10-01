import json
import os
import shutil
from time import sleep

import pandas as pd
from selenium import webdriver


def get_credentials():
    try:
        with open('credentials.json') as file:
            credentials = json.load(file)
        return credentials
    except FileNotFoundError as f:
        return None


path = os.path.dirname(os.path.realpath(__file__))
print(path
      )
username = get_credentials()['username']
password = get_credentials()['password']
p = {"download.default_directory": path, "safebrowsing.enabled": "false"}

chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option("prefs", p)
chrome_options.add_argument('--headless')
driver = webdriver.Chrome('/home/kinesso/automation/chromedriver', options=chrome_options,
                          service_args=['--verbose', '--log-path=/home/kinesso/automation/chromedriver.log'])

url = 'https://hq1.appsflyer.com/auth/login'
driver.get(url)
driver.find_element_by_name('username').send_keys(username)
driver.find_element_by_name('password').send_keys(password)
driver.find_element_by_css_selector('.submit-btn').click()
sleep(5)

from datetime import date, timedelta


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


start_date = date(2021, 9, 1)
end_date = date(2021, 9, 30)
for single_date in daterange(start_date, end_date):
    sleep(2)
    current_date = single_date.strftime("%Y-%m-%d")
    link = 'https://hq1.appsflyer.com/custom-dashboard#end={END}&grouping=attribution&pageId=89652&start={START}'.format(
        END=current_date, START=current_date)
    print(link)
    driver.get(link)
    sleep(5)
    driver.find_element_by_xpath('//*[@id="export-wrapper"]/div[2]/div[5]/div/div/div[1]/div/div/button').click()
    sleep(2)
    driver.find_element_by_xpath('//*[@id="export-wrapper"]/div[2]/div[5]/div/div/div[1]/div/div/ul/li[4]').click()
    sleep(7)
    Initial_path = path
    filename = max([Initial_path + "/" + f for f in os.listdir(Initial_path) if ".csv" in f], key=os.path.getctime)
    shutil.move(filename, os.path.join(Initial_path, r"data.csv"))
    df = pd.read_csv("data.csv")
    print(df.info())
    driver.back()

driver.close()
