import json


def get_credentials():
    try:
        with open('credentials.json') as file:
            credentials = json.load(file)
        return credentials
    except FileNotFoundError as f:
        return None


username = get_credentials()['username']
password = get_credentials()['password']

print(username, password)


from selenium import webdriver
from time import sleep
import os
import shutil

path = os.path.dirname(os.path.realpath(__file__))

url = 'https://hq1.appsflyer.com/auth/login'

op = webdriver.ChromeOptions()
p = {"download.default_directory": path, "safebrowsing.enabled":"false"}
op.add_experimental_option("prefs", p)
op.add_argument("--start-maximized")
driver = webdriver.Chrome(r"C:\\Users\\Tomasz.Pionka\\Downloads\\chromedriver.exe",options=op)

driver.get(url)
driver.find_element_by_name('username').send_keys(username)
driver.find_element_by_name('password').send_keys(password)
driver.find_element_by_css_selector('.submit-btn').click()
sleep(5)
link = 'https://hq1.appsflyer.com/custom-dashboard#end=2021-09-30&grouping=attribution&pageId=89652&start=2021-09-23'
driver.get(link)
sleep(7)
driver.find_element_by_xpath('//*[@id="export-wrapper"]/div[2]/div[5]/div/div/div[1]/div/div/button').click()
sleep(2)
driver.find_element_by_xpath('//*[@id="export-wrapper"]/div[2]/div[5]/div/div/div[1]/div/div/ul/li[4]').click()
sleep(7)
driver.close()
