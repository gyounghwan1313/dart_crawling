
import os

from dotenv import load_dotenv
load_dotenv(".env")

import pandas as pd
from src.module.db_connection import PostgreSQL

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

code_df = pd.read_excel("./data/20240227_코스닥 14_21리스트.xlsx", converters={'stockcode_re': str})

code_df.info()

code_df = code_df[['firm','stockcode_re']]
code_df.rename(columns={'firm':'company_nm','stockcode_re':'company_cd'}, inplace=True)


master_df = db_conn.sql_dataframe("select company_nm, company_cd from company_code;")
master_df['company_cd']

db_conn.sa_session()
code_df.drop_duplicates(inplace=True)
code_df.loc[~code_df['company_cd'].isin(master_df['company_cd'])].to_sql(con=db_conn.sa_conn,name="company_code", index=False,if_exists='append')


code_df.to_sql(con=db_conn.sa_conn,name="company_code", index=False)