import os
import re
from datetime import datetime

import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


def sql_log(query: str) -> None:
    with open("sql_log.txt", "a", encoding="utf-8") as f:
        f.write(f"Query [{datetime.now()}]\n{query}\n\n")

class DBManager:
    def __init__(self,
                 db_user:str,
                 db_password:str,
                 db_host:str,
                 db_port:str,
                 db_database:str) -> None:
        url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}?charset=utf8mb4'
        self.engine = create_engine(url,
                                    pool_recycle=360,
                                    echo=False)


    async def _format_response(self, data: pd.DataFrame) -> pd.DataFrame:
        # 正規表現でリストを見つけ、リスト型に変換する
        list_pattern = re.compile(r"\[.*?\]")
        for column in data.columns:
            data[column] = data[column].apply(lambda x: eval(x) if re.match(list_pattern, str(x)) else x)

        # 正規表現で辞書型を見つけ、辞書型に変換する
        dict_pattern = re.compile(r"\{.*?\}")
        for column in data.columns:
            data[column] = data[column].apply(lambda x: eval(x) if re.match(dict_pattern, str(x)) else x)

        return data


    async def _commit(self, query) -> None:
        with self.engine.connect() as conn:
            with conn.begin():
                try:
                    conn.execute(text(query))
                except IntegrityError as e:
                    conn.rollback()
                    raise Exception(f"IntegrityError: {e}")
                else:
                    return


    async def _fetch(self, query) -> pd.DataFrame:
        with self.engine.connect() as conn:
            with conn.begin():
                try:
                    result = conn.execute(text(query))
                    data = result.fetchall()
                except Exception as e:
                    conn.rollback()
                    raise Exception(f"Error: {e}")
        data = pd.DataFrame(data, columns=result.keys())
        data = await self._format_response(data)
        return data


    async def query(self, query):
        sql_log(query)
        if "SELECT" in query:
            return await self._fetch(query)
        else:
            return await self._commit(query)