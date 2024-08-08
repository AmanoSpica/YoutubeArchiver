import os

import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

class DBManager:
    def __init__(self,
                 db_user:str,
                 db_password:str,
                 db_host:str,
                 db_port:str,
                 db_database:str) -> None:
        url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}?charset=utf8'
        self.engine = create_engine(url,
                                    pool_recycle=360,
                                    echo=False)

    async def commit(self, query) -> None:
        with self.engine.connect() as conn:
            with conn.begin():
                try:
                    conn.execute(text(query))
                except IntegrityError as e:
                    conn.rollback()
                    raise Exception(f"IntegrityError: {e}")
                else:
                    return

    async def fetch(self, query):
        with self.engine.connect() as conn:
            with conn.begin():
                try:
                    result = await conn.execute(text(query))
                    data = await result.fetchall()
                except Exception as e:
                    conn.rollback()
                    raise Exception(f"Error: {e}")
        data = pd.DataFrame(data, columns=result.keys())
        return data