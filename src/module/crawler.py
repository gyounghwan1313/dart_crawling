
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


class Crawler():

    def __init__(self):
        self._chrome_options = Options()
        self._chrome_options.add_experimental_option("detach", True)
        self._chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36")
        self._chrome_options = True
        self.driver = None

    def open(self):
        self.driver = webdriver.Chrome(options=self._chrome_options)
        self.driver.maximize_window()

        return self.driver

if __name__ == '__main__':
    _chrome_options = Options()
    _chrome_options.headless = True

    from selenium import webdriver

    options = webdriver.ChromeOptions()
    options.headless = True
    options.add_argument("window-size=1920x1080")

    # user agent
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36")

    browser = webdriver.Chrome(options=options)