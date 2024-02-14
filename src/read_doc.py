
import os

import re
import time
import datetime as dt

import selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select

from bs4 import BeautifulSoup
import requests
import pandas as pd

from src.module.db_connection import PostgreSQL

from dotenv import load_dotenv
load_dotenv(".env")

class Crawler():

    def __init__(self):
        self._chrome_options = Options()
        self._chrome_options.add_experimental_option("detach", True)
        self.driver = None

        self.biz_start_date = None
        self.biz_end_date = None
        self.ceo = None

    def open(self):
        self.driver = webdriver.Chrome(options=self._chrome_options)
        self.driver.maximize_window()

        return self.driver


class DataControl(object):

    def __init__(self, db: PostgreSQL):
        self.db = db

    def get_list(self):
        list_df = self.db.sql_dataframe("""select company_nm, report_nm, report_dt, url, company_cd
                from  crawl_fs_link cfl 
                where is_read_complete is null
                ;""")
        return list_df


class ReadPage(Crawler):
    """
    1. 해당 자료 연도
     - 1 페이지 : 사업년도
    2. 기업코드
     - 이미 수집됨
    3. 기업명
     - 이미 수집됨
    4. 해당 자료의 CEO
     - 1 페이지 : 대표이사
    5. Family business CEO여부

    6. 사외 이사 수

    7. 이사회 수

    8. 해당년도 이사회장 이름
    """

    def __init__(self):
        super().__init__()

    def go_to_page(self, url: str = 'https://dart.fss.or.kr/dsab001/main.do'):
        self.driver = self.open()

        # 회사별 검색 접속
        self.driver.get(url=url)
        time.sleep(10)

    def find_biz_report(self) -> None:
        is_find = False
        for target_num in range(1, 10):
            text = self.driver.find_element(By.XPATH, f'//*[@id="{target_num}_anchor"]').text
            if "사업보고서" in "".join(text.split()):
                is_find = True
                break

        if is_find:  # 찾았으면
            self.driver.find_element(By.XPATH, f'//*[@id="{target_num}_anchor"]').click()
        else:
            raise KeyError("사업보고서 Not Founded in Side Menu")

    def find_linked_page(self):

        page_url = self.driver.find_element(By.CLASS_NAME, 'contWrap').find_element(By.ID, 'ifrm').get_attribute('src')

        return page_url

    def parsing_first_page(self, linked_url): # 1, 4
        parse_df_list = pd.read_html(linked_url)
        # biz_period_raw_df = parse_df_list[0]

        for df in parse_df_list:

            df.dropna(inplace=True)
            df.dropna(axis=1, inplace=True)
            df.reset_index(inplace=True, drop=True)
            if len(df)>1 and df.loc[0, 0] == df.loc[1, 0] == "사업연도":
                df = df.apply(lambda x: dt.datetime.strptime(x[1],"%Y년 %m월 %d일"), axis=1)
                self.biz_start_date = df.loc[0]
                self.biz_end_date = df.loc[1]


            elif len(df)>1  and "대표이사" in "".join(df.loc[1,0].split()):
                self.ceo = "".join(df.loc[1,1].split())
            else:
                continue

            return self.biz_start_date, self.biz_end_date, self.ceo


    def find_24_info_tab(self):
        for i in range(50):
            try:
                self.driver.find_element(By.XPATH, f'//*[@id="{i}"]/i').click()
                time.sleep(0.5)
            except selenium.common.exceptions.NoSuchElementException:
                pass

        menu_list = self.driver.find_element(By.ID, "listTree").find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME, 'li')

        for idx, menu in enumerate(menu_list):
            if "이사회등회사의기관에관한사항" in menu.text.replace(" ",""):
                menu.click()


    def parsing_24(self):
        # 가.이사회의 구성 개요
        self.driver.get('https://dart.fss.or.kr/report/viewer.do?rcpNo=20230321001125&dcmNo=9083205&eleId=33&offset=1259642&length=38545&dtd=dart3.xsd')
        html_text_list  = self.driver.find_elements(By.TAG_NAME, 'p')

        html_text_list[3].text

        for idx, html_text in enumerate(html_text_list):
            if "가. 이사회의 구성 개요" in html_text.text:
                print(idx, html_text.text)


"""
# class name으로 찾기
driver.find_element(By.CLASS_NAME,'btnSearch')
# tag name으로 찾기
driver.find_element(By.TAG_NAME,'textarea')
# id로 찾기
driver.find_element(By.ID,'textCrpNm')
# XPath로 찾기
driver.find_element(By.XPATH,'//*[@id="APjFqb"]')
"""

if __name__ == '__main__':
    db_host = os.environ["DB_HOST"]
    db_port = os.environ["DB_PORT"]
    db_database = os.environ["DB_DATABASE"]
    db_user = os.environ["DB_USER"]
    db_password = os.environ["DB_PASSWORD"]

    db_conn = PostgreSQL(
        host=db_host,
        port=db_port,
        database=db_database,
        user=db_user,
        password=db_password,
    )
    list_df = DataControl(db=db_conn).get_list()

    crawl = ReadPage()
    crawl.go_to_page(url=list_df.loc[0,'url'])

    crawl.find_biz_report()
    first_page = crawl.find_linked_page()

    crawl.parsing_first_page(first_page)

    ## 24
    crawl.find_24_info_tab()

    crawl.parsing_24()