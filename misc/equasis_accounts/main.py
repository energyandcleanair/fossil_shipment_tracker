from time import sleep

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys

from decouple import config

REGISTRATION_URL = "https://www.equasis.org/EquasisWeb/public/Registration?fs=ConditionsRegistration"
RECAPTCHA_SELECTOR="iframe[title=reCAPTCHA]"

START_RANGE = 160
END_RANGE = 200

PASSWORD = config('EQUASIS_PASSWORD')

if __name__ == "__main__":
    print("Creating accounts")

    options = webdriver.ChromeOptions()
    options.add_argument("ignore-certificate-errors")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.binary_location = "/sbin/chromium"

    service = Service(ChromeDriverManager().install())

    browser = webdriver.Chrome(service=service, options=options)

    for i in range(START_RANGE, END_RANGE):
        browser.switch_to.new_window('tab')
        browser.get(REGISTRATION_URL)
        WebDriverWait(
            browser, 10, ignored_exceptions=EC.StaleElementReferenceException
        ).until(EC.visibility_of_element_located((
            By.CSS_SELECTOR, RECAPTCHA_SELECTOR
        )))

        browser.find_element(By.CSS_SELECTOR, '#field-mail').send_keys(f'rutankers+{i}@protonmail.com')
        browser.find_element(By.CSS_SELECTOR, '#field-pwd').send_keys(PASSWORD)
        browser.find_element(By.CSS_SELECTOR, '#field-confirm-pwd').send_keys(PASSWORD)
        browser.find_element(By.CSS_SELECTOR, '#field-firstname').send_keys('A')
        browser.find_element(By.CSS_SELECTOR, '#field-name').send_keys('B')
        browser.find_element(By.CSS_SELECTOR, '#field-adress').send_keys('HK')
        browser.find_element(By.CSS_SELECTOR, '#field-city').send_keys('Hong Kong')
        browser.find_element(By.CSS_SELECTOR, '#field-postcode').send_keys('0')

        # Selects
        Select(browser.find_element(By.CSS_SELECTOR, 'select[name=p_title]')).select_by_value('Mr')
        Select(browser.find_element(By.CSS_SELECTOR, 'select[name=p_country]')).select_by_value('0275')
        Select(browser.find_element(By.CSS_SELECTOR, 'select[name=p_sector_activity]')).select_by_value('013')
        Select(browser.find_element(By.CSS_SELECTOR, 'select[name=p_how_equasis]')).select_by_value('05')

    while True:
        sleep(30)