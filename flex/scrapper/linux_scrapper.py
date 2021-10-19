import os
from datetime import date, timedelta
from time import sleep

from selenium import webdriver

import functions

path = os.path.dirname(os.path.realpath(__file__))
download_path = r"{}/".format(path)
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
functions.suspension(driver)

start_date = date.today() - timedelta(days=1)
end_date = date.today()
target = 5

try:
    for single_date in functions.daterange(start_date, end_date):
        sleep(2)
        link = 'https://hq1.appsflyer.com/custom-dashboard#end={END}&grouping=attribution&pageId=89652&start={START}'.format(
            END=single_date, START=single_date)
        driver.get(link)
        functions.suspension(driver)
        sleep(5)
        for element in range(target, target + 2):
            print(element, target + 1)
            functions.custom_dashboard(driver, path, single_date, element)
        driver.back()

except Exception as e:
    print(e)

finally:
    driver.close()
    sleep(10)
    driver.quit()
