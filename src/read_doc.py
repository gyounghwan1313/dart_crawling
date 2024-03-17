
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


replace_null = lambda x : 'NULL' if x is None else "'"+x.replace("'", "''")+ "'"

class Crawler():

    def __init__(self):
        self._chrome_options = Options()
        self._chrome_options.add_argument("--headless=new")
        # self._chrome_options.add_experimental_option("detach", True)
        self._chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36")
        self.driver = None

        self.biz_start_date = None
        self.biz_end_date = None
        self.ceo = None

    def open(self):
        self.driver = webdriver.Chrome(options=self._chrome_options)
        self.driver.maximize_window()

        return self.driver

    def close(self):
        self.driver.close()


class DataControl(object):

    def __init__(self, db: PostgreSQL):
        self.db = db

    def get_list(self):
        list_df = self.db.sql_dataframe("""
        select company_nm, report_nm, report_dt, url, company_cd
            from  crawl_fs_link cfl 
            where  1=1
            and cfl.is_read_complete is null
            and not exists (select 'x' from report r 
                                            where cfl.company_cd = r.company_cd 
                                            and cfl.url = r.url)
            and exists (select 'x' from company_code cc where cc.company_cd = cfl.company_cd
                        and cc.is_get_list ='1' and cc.is_priority ='1')
                        and url like 'https://dart.fss.or.kr/dsaf001/main.do?rcpNo%'
            order by company_cd
            ;
        """)
        return list_df

    def collect_fail(self, url):
        query = f"""UPDATE public.crawl_fs_link 
                        SET updated_time=now(), is_read_complete='fail'
                        where url = '{url}';"""
        self.db.sql_execute(query)


    def insert_first_page(self, company_cd, report_dt, biz_start_date, biz_end_date, ceo, url, first_page_url):
        query = f"""INSERT INTO public.report (company_cd, report_dt, biz_start_date, biz_end_date, ceo, url, first_page_url) 
                       VALUES ('{company_cd}','{report_dt}','{biz_start_date}','{biz_end_date}','{ceo}', '{url}' ,'{first_page_url}')
                       On CONFLICT (company_cd, url)
                       DO UPDATE 
                       SET biz_start_date='{biz_start_date}',
                       biz_end_date='{biz_end_date}',
                       ceo='{ceo}',
                       url='{url}',
                       first_page_url='{first_page_url}',
                       updated_timestamp='now()';"""
        self.db.sql_execute(query)

    def insert_stockholder_info(self, company_cd, url, name, relation, stock_type, stockholder_url):
        query = f"""INSERT INTO public.stockholder (company_cd, url, "name", relation, stock_type, stockholder_url) 
                                        VALUES ('{company_cd}', '{url}', '{name}', '{relation}', '{stock_type}', '{stockholder_url}')
                                        On CONFLICT (company_cd, url, "name", stock_type)
                                        DO UPDATE 
                                        SET name = '{name}',
                                        relation = '{relation}',
                                        stock_type = '{stock_type}',
                                        stockholder_url = '{stockholder_url}',
                                        updated_time = 'now()'
                                        ;"""
        self.db.sql_execute(query)

    def insert_info24(self, company_cd, report_dt, info_24_group, url):
        query = f"""INSERT INTO public.report (company_cd, report_dt, info_24, url) 
                                               VALUES ('{company_cd}','{report_dt}',{'NULL' if info_24_group is None else "'"+info_24_group.replace("'", "''")+ "'"}, '{url}')
                                               On CONFLICT (company_cd, url)
                                               DO UPDATE 
                                               SET info_24={'NULL' if info_24_group is None else "'"+info_24_group.replace("'", "''")+ "'"},
                                               url = '{url}'
                                               updated_timestamp='now()';"""
        self.db.sql_execute(query)

    def check_collect_able(self, url):
        query = f"""select count(*)
                    from  crawl_fs_link cfl 
                    where 1=1
                    and cfl.is_read_complete != 'fail'
                    and url = '{url}';"""

        df = self.db.sql_dataframe(query)
        return len(df)== 0


class ReadPage(Crawler):
    """
    1. 해당 자료 연도 (o)
     - 1 페이지 : 사업년도
    2. 기업코드 (o)
     - 이미 수집됨
    3. 기업명 (o)
     - 이미 수집됨
    4. 해당 자료의 CEO (o)
     - 1 페이지 : 대표이사 (o)
    5. Family business CEO여부

    6. 사외 이사 수 (-)

    7. 이사회 수 (-)

    8. 해당년도 이사회장 이름
    """

    def __init__(self):
        super().__init__()

    def go_to_page(self, url: str = 'https://dart.fss.or.kr/dsab001/main.do'):
        self.driver = self.open()

        # 회사별 검색 접속
        self.driver.get(url=url)
        time.sleep(2)

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
                try:
                    df = df.apply(lambda x: dt.datetime.strptime(x[1],"%Y년 %m월 %d일"), axis=1)
                except ValueError:
                    df = df.apply(lambda x: dt.datetime.strptime(x[1], "%Y.%m.%d"), axis=1)
                self.biz_start_date = df.loc[0]
                self.biz_end_date = df.loc[1]


            elif len(df)>1  and "대표이사" in "".join(df.loc[1,0].split()):
                self.ceo = "".join(df.loc[1,1].split())
            else:
                continue

        return self.biz_start_date, self.biz_end_date, self.ceo

    def parsing_stockholder_page(self, linked_url):
        parse_df_list = pd.read_html(linked_url)
        # biz_period_raw_df = parse_df_list[0]

        for df in parse_df_list:

            df.dropna(inplace=True)
            df.dropna(axis=1, inplace=True)
            df.reset_index(inplace=True, drop=True)
            if isinstance(df.columns,pd.core.indexes.multi.MultiIndex):
                df.columns = [i[0].replace(" ","") for i in df.columns]
            if "관계" in df.columns:
                print(df)
                stockholder_df = df[["성명","관계","주식의종류"]]
                stockholder_df = stockholder_df.loc[stockholder_df["성명"] != "계"]
                return stockholder_df


    def close_tab_group(self):
        for i in range(50):
            try:
                self.driver.find_element(By.XPATH, f'//*[@id="{i}"]/i').click()
                time.sleep(0.5)
            except selenium.common.exceptions.NoSuchElementException:
                pass

    def find_tab(self, memu_name):
        menu_list = self.driver.find_element(By.ID, "listTree").find_element(By.TAG_NAME,'ul').find_elements(By.TAG_NAME, 'li')

        for idx, menu in enumerate(menu_list):
            if memu_name.replace(" ","") in menu.text.replace(" ",""):
                menu.click()

    def parsing_24(self, page_url):
        result = None
        # 가.이사회의 구성 개요
        self.driver.get(page_url)
        time.sleep(1)
        html_text_list  = self.driver.find_elements(By.TAG_NAME, 'p')

        for idx, html_text in enumerate(html_text_list):
            if "가. 이사회의 구성 개요" in html_text.text or ("이사회" in html_text.text and "구성" in html_text.text and "개요" in html_text.text):
                result = html_text.text
                if len(result) < 20:
                    result += "\n"
                    result += html_text_list[idx+1].text
                    break
        return result


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
    db_controller = DataControl(db=db_conn)
    list_df = db_controller.get_list()
    print(list_df)

    for idx, data in list_df.iterrows():

        print(data['company_nm'],data['company_cd'],data['url'])
        # if data['company_cd'] in ["001120","161000","138930","185490","263920","차백신연구소","247540","246830","244460","242850","239340","234100","024830"]:
        if db_controller.check_collect_able(data['url']):
            continue
        crawl = ReadPage()
        crawl.go_to_page(url=data['url'])
        try:
            crawl.find_biz_report()
        except Exception as e:
            print(f"ERROR : {e}")
            db_controller.collect_fail(url=data['url'])
            crawl.driver.close()
            continue
        first_page = crawl.find_linked_page()

        biz_start_date, biz_end_date, ceo = crawl.parsing_first_page(first_page)
        print(biz_start_date,biz_end_date,ceo)
        db_controller.insert_first_page(data['company_cd'], data['report_dt'],biz_start_date,biz_end_date,ceo, data['url'],first_page)
        # db_controller.db._connect.rollback()
        crawl.close_tab_group()

        ##
        crawl.find_tab("주주에관한사항")
        stockholder_page_url = crawl.find_linked_page()
        stockholder_df = crawl.parsing_stockholder_page(stockholder_page_url)
        for _, stock_data in stockholder_df.iterrows():
            db_controller.insert_stockholder_info(company_cd=data['company_cd'],url=data['url'], name=stock_data['성명'],relation=stock_data['관계'], stock_type=stock_data['주식의종류'], stockholder_url=stockholder_page_url)
        # db_controller.db._connect.rollback()

        ## 24
        # crawl.find_tab("이사회등회사의기관에관한사항")
        # url_24 = crawl.find_linked_page()
        # info_24_group = crawl.parsing_24(url_24)
        # print(info_24_group)
        # db_controller.insert_info24(data['company_cd'], data['report_dt'], info_24_group)
        #
        crawl.driver.close()
        time.sleep(0.7)

        if idx % 15 ==0:
            time.sleep(10)

    #
    # info_24_group
    #
    # # 대표의 의사회장
    # parse_df_list = pd.read_html("https://dart.fss.or.kr/report/viewer.do?rcpNo=20230321001125&dcmNo=9083205&eleId=33&offset=1259642&length=38545&dtd=dart3.xsd")
    # parse_df_list[0]
    # parse_df_list[2]
    #
    # # CASE 1
    # parse_df_list[0]
    # for df in parse_df_list:
    #     df = parse_df_list[0]
    #     # 컬럼 조사
    #     for col in df.columns.tolist():
    #         if col.replace(" ","") in ["이사회의장"]:
    #             chair_24 = df[col][0]