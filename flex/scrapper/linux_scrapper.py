import json
import os
import shutil
from datetime import date, timedelta
from time import sleep
import functions
import pandas as pd
from selenium import webdriver





path = os.path.dirname(os.path.realpath(__file__))
download_path = r"{}/".format(path)
print(download_path)
username = functions.get_credentials()['username']
password = functions.get_credentials()['password']
p = {"profile.default_content_settings.popups": 0,
     "download.default_directory": download_path,
     "download.prompt_for_download": False,
     "directory_upgrade": True}

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_experimental_option("prefs", p)
chrome_options.add_argument('--headless')
driver = webdriver.Chrome('/usr/bin/chromedriver', options=chrome_options,
                          service_args=['--verbose', '--log-path=/home/kinesso/automation/chromedriver.log'])

driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_path}}
command_result = driver.execute("send_command", params)

url = 'https://hq1.appsflyer.com/auth/login'
driver.get(url)
driver.find_element_by_name('username').send_keys(username)
driver.find_element_by_name('password').send_keys(password)
driver.find_element_by_css_selector('.submit-btn').click()
sleep(5)


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


start_date = date.today() - timedelta(days=1)
end_date = date.today()

for single_date in daterange(start_date, end_date):
    Initial_path = download_path
    sleep(2)
    current_date = single_date.strftime("%Y-%m-%d")
    link = 'https://hq1.appsflyer.com/custom-dashboard#end={END}&grouping=attribution&pageId=89652&start={START}'.format(
        END=current_date, START=current_date)
    driver.get(link)
    sleep(5)
    driver.refresh()
    sleep(10)
    """ android data """
    driver.find_element_by_xpath('//*[@id="export-wrapper"]/div[2]/div[5]/div/div/div[1]/div/div/button').click()
    sleep(2)
    driver.find_element_by_xpath('//*[@id="export-wrapper"]/div[2]/div[5]/div/div/div[1]/div/div/ul/li[4]').click()
    sleep(20)
    filename = max([Initial_path + f for f in os.listdir(Initial_path) if ".csv" in f], key=os.path.getctime)
    shutil.move(filename, os.path.join(Initial_path, r"data.csv"))
    df = pd.read_csv("data.csv")
    df['Date'] = current_date
    df['Platform'] = 'Android'
    df['Origin'] = 'WP_Flex'
    df.rename({'tapregister (Event Counter)': 'tapregister__Event_Counter_',
               'tapactivate (Event Counter)': 'tapactivate__Event_Counter_'}, axis=1, inplace=True)
    # functions.upload_data("orange", "custom_wp_flex_per_ad", df)
    print(df)
    print(df.info())
    """ ios data """
    driver.find_element_by_xpath('//*[@id="export-wrapper"]/div[2]/div[6]/div/div/div[1]/div/div/button').click()
    sleep(2)
    driver.find_element_by_xpath('//*[@id="export-wrapper"]/div[2]/div[6]/div/div/div[1]/div/div/ul/li[4]').click()
    sleep(20)
    filename = max([Initial_path + f for f in os.listdir(Initial_path) if ".csv" in f], key=os.path.getctime)
    shutil.move(filename, os.path.join(Initial_path, r"data.csv"))
    df = pd.read_csv("data.csv")
    df['Date'] = current_date
    df['Platform'] = 'iOS'
    df['Origin'] = 'WP_Flex'
    df.rename({'tapregister (Event Counter)': 'tapregister__Event_Counter_',
               'tapactivate (Event Counter)': 'tapactivate__Event_Counter_'}, axis=1, inplace=True)
    # functions.upload_data("orange", "custom_wp_flex_per_ad", df)
    print(df)
    print(df.info())
    driver.back()


driver.close()
