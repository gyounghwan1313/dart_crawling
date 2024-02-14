import os
import pandas as pd


df = pd.read_excel("./data/2011-2023_코스피 기업 리스트.xlsx")

df.info()

code_df = df[['회사명','거래소코드']].drop_duplicates()
code_df['거래소코드'] = code_df['거래소코드'].apply(lambda x: str(x) if len(str(x))==6 else "0"*(6-len(str(x)))+str(x))

code_df.rename(columns={"회사명":"company_nm","거래소코드":"company_cd"}, inplace=True)
code_df

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

db_conn.sa_session()
db_conn.sa_conn

code_df.to_sql(con=db_conn.sa_conn,name="company_code", index=False)