
import os
import re
import time
import datetime as dt

from dotenv import load_dotenv

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select

from bs4 import BeautifulSoup
import pandas as pd

from src.module.crawler import Crawler
from src.module.db_connection import PostgreSQL


class CrawlingList(Crawler):

    def __init__(self):
        super().__init__()

    def go_to_page(self, url: str='https://dart.fss.or.kr/dsab001/main.do'):

        self.driver = self.open()

        # 회사별 검색 접속
        self.driver.get(url=url)
        time.sleep(10)

    def insert_search_word(self, code: str):
        # 회사코드 입력
        self.driver.find_element(By.XPATH, '//*[@id="textCrpNm"]').send_keys(code)
        time.sleep(1)

    def set_search_option(self):
        #10년 기간 선택
        self.driver.find_element(By.XPATH,'//*[@id="date7"]').click()
        time.sleep(1)

        # 조회건수
        Select(self.driver.find_element(By.ID,'maxResultsCb')).select_by_value("100")
        time.sleep(1)

        # 사업보고서 선택
        self.driver.find_element(By.XPATH,'//*[@id="li_01"]').click()
        self.driver.find_element(By.XPATH,'//*[@id="divPublicTypeDetail_01"]/ul/li[1]/span').click()

    def search(self):
        # 검색 클릭
        self.driver.find_element(By.CLASS_NAME,'btnSearch').click()

    def find_table(self):
        html = self.driver.page_source
        html_parse = BeautifulSoup(html, 'html.parser')
        body = html_parse.find("body")
        body.find_all("table")

        df = pd.read_html(html)[0]
        self.target_df = df.loc[df['보고서명'].apply(lambda x: len(re.findall("\w?사업보고서 \(\w?", x)))>0]

    def find_report_url(self):
        target_index = self.target_df['번호'].tolist()

        rows = self.driver.find_element(By.ID,'tbody').find_elements(By.TAG_NAME,"tr")
        for r in rows:
            td = int(r.find_element(By.TAG_NAME,'td').text)
            if td in target_index:
                report_url = r.find_elements(By.TAG_NAME,'a')[1].get_attribute("href")
                self.target_df.loc[td-1, 'url'] = report_url

        return self.target_df

    def search_clear(self):
        self.driver.find_element(By.XPATH, '//*[@id="textCrpNm"]').clear()


def load_company_df(db_conn: PostgreSQL):
    company_df = db_conn.sql_dataframe(f"""select company_cd, company_nm 
from company_code cc 
where not exists (select 'x' from crawl_fs_link cfl where cc.company_cd=cfl.company_cd) 
;""")

    return company_df


if __name__ == '__main__':

    load_dotenv(".env")
    db_host = os.environ['DB_HOST']
    db_port = os.environ['DB_PORT']
    db_database = os.environ['DB_DATABASE']
    db_user = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']
    db_conn = PostgreSQL(host=db_host,
                         port=db_port,
                         database=db_database,
                         user=db_user,
                         password=db_password)
    db_conn.sa_session()
    ## 리스트 가져오기
    company_df = load_company_df(db_conn=db_conn)

    ## 초기 설정
    get_list = CrawlingList()
    get_list.go_to_page()
    get_list.set_search_option()
    time.sleep(1)

    ## 코드 입력
    for idx, df in company_df.iterrows():
        get_list.insert_search_word(code=df['company_cd'])
        get_list.search()
        time.sleep(5)
        get_list.find_table()
        url_table = get_list.find_report_url()
        url_table.drop(['번호'], inplace=True, axis=1)
        url_table['공시대상회사'] = url_table['공시대상회사'].apply(lambda x: x.split()[1])

        url_table.rename(columns={"공시대상회사":"company_nm",
                                  "보고서명":"report_nm",
                                  "제출인":"submit_nm",
                                  "접수일자": "report_dt",
                                  "비고": "report_year_type"
                                  },
                         inplace=True
                         )
        url_table['company_cd'] = df['company_cd']
        url_table['report_dt'] = url_table['report_dt'].apply(pd.to_datetime)

        url_table.to_sql(con=db_conn.sa_conn, name="crawl_fs_link", index=False, if_exists="append")
        time.sleep(3)

        get_list.search_clear()
        if idx % 10 ==0:
            time.sleep(20)
