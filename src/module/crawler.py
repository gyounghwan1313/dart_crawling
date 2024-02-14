
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


class Crawler():

    def __init__(self):
        self._chrome_options = Options()
        self._chrome_options.add_experimental_option("detach", True)
        self.driver = None

    def open(self):
        self.driver = webdriver.Chrome(options=self._chrome_options)
        self.driver.maximize_window()

        return self.driver