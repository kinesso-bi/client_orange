from selenium import webdriver

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
driver = webdriver.Chrome('/home/kinesso/automation/chromedriver', options=chrome_options,  service_args=['--verbose', '--log-path=/home/kinesso/automation/chromedriver.log'])
driver.get('https://www.python.org/')
print(driver.title)
driver.close()