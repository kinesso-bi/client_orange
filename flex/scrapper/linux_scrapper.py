from selenium import webdriver

# Option 1 - with ChromeOptions
chrome_options = webdriver.ChromeOptions()
chrome_options.use_chromium = True
chrome_options.add_argument('--headless')
chrome_options.add_argument(
    '--no-sandbox')  # required when running as root user. otherwise you would get no sandbox errors.
chrome_options.add_argument('--ignore-certificate-errors')
# Chrome:
myProxy = "10.0.x.x:yyyy"
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--proxy-server=%s' % myProxy)
path = r"/home/kinesso/automation/chromedriver"
driver = webdriver.Chrome(path, options=chrome_options,
                          service_args=['--verbose', '--log-path=/home/kinesso/automation/chromedriver.log'])

driver.get('https://python.org')
print(driver.title)
