import json
import os
import shutil
from time import sleep
from selenium.webdriver.common.keys import Keys
import pandas as pd
from selenium import webdriver

import functions


def get_credentials():
    try:
        with open('credentials.json') as file:
            credentials = json.load(file)
        return credentials
    except FileNotFoundError as f:
        return None

path = os.path.dirname(os.path.realpath(__file__))
print(path)

username = get_credentials()['username']
password = get_credentials()['password']

op = webdriver.ChromeOptions()
op.add_argument('--disable-browser-side-navigation')
p = {"download.default_directory": path, "safebrowsing.enabled": "false"}
op.add_experimental_option("prefs", p)
op.add_argument("--start-maximized")
driver = webdriver.Chrome(r"C:\\Users\\Tomasz.Pionka\\Downloads\\chromedriver.exe", options=op)

url = 'https://hq1.appsflyer.com/auth/login'
driver.get(url)
driver.find_element_by_name('username').send_keys(username)
driver.find_element_by_name('password').send_keys(password)
driver.find_element_by_css_selector('.submit-btn').click()
sleep(5)
functions.suspension(driver)

from datetime import date, timedelta


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


start_date = date(2021, 10, 11)
end_date = date(2021, 10, 19)
target = 5

for single_date in daterange(start_date, end_date):
    sleep(2)
    functions.suspension(driver)
    link = 'https://hq1.appsflyer.com/custom-dashboard#end={END}&grouping=attribution&pageId=89652&start={START}'.format(
        END=single_date, START=single_date)
    driver.get(link)
    sleep(5)
    for element in range(target, target + 2):
        print(element, target + 1)
        functions.custom_dashboard(driver, path, single_date, element)
    driver.back()

driver.close()
sleep(10)
driver.quit()