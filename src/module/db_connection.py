import sys
from typing import Union
import logging
import pandas as pd
import pandas.io.sql as psql
import psycopg2 as pg

class PostgreSQL(object):

    def __init__(self,
                 host,
                 port: Union[str, int],
                 database: str,
                 user: str,
                 password: str):

        self.__host = host
        self.__port = port
        self.__database = database
        self.__user = user
        self.__password = password




    def sql_execute(self, query: str) -> None:
        _connect = pg.connect(host=self.__host, port=str(self.__port), database=self.__database, user=self.__user,
                                   password=self.__password)
        self.cur = _connect.cursor()
        try:
            self.cur.execute(query)
            # self.cur.commit()
            _connect.commit()
            self.cur.close()
            _connect.close()
        except Exception as e:
            print(e)
            _connect.rollback()
            self.cur.close()
            _connect.close()
            sys.exit(1)

    def sql_dataframe(self, query: str) -> pd.DataFrame:
        _connect = pg.connect(host=self.__host, port=str(self.__port), database=self.__database, user=self.__user,
                                   password=self.__password)
        df = psql.read_sql_query(query, _connect)
        _connect.close()
        return df

    def sa_session(self):
        import sqlalchemy as sa
        self.sa_conn = sa.create_engine(f"postgresql://{self.__user}:{self.__password}@{self.__host}:{self.__port}/{self.__database}")



if __name__ == '__main__':
    __db_connector = PostgreSQL(host='ecsdfg.ap-northeast-2.compute.amazonaws.com',
                                port=5432,
                                database='asdf',
                                user='sdfg',
                                password='rsdfgsdfg')
