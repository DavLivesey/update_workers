from typing import Union

import asyncio
import logging
import asyncpg
from asyncpg.pool import Pool
from config import HOST, PG_PSWD, PG_USER

logging.basicConfig(format=u'%(filename)s [LINE:%(lineno)d] #%(levelname)-8s'
                           u'[%(asctime)s]  %(message)s',
                    level=logging.INFO)

class DataBaseClass:
    def __init__(self) -> None:
        self.pool: Union[Pool, None] = None

    async def create_db():
        create_db_command = open('create_dbl.sql', 'r').read()
        logging.info('Connection to db...')
        conn: asyncpg.Connection = await asyncpg.connect(
            host=HOST,
            user=PG_USER,
            password=PG_PSWD,
            database='workers'
        )
        await conn.execute(create_db_command)
        logging.info('Table was created')
        await conn.close()

    async def create_pool(self):
        try:
            self.pool = await asyncpg.create_pool(
                host=HOST,
                user=PG_USER,
                password=PG_PSWD,
                database='workers'
            )
        finally:
            self.pool.close()
    
    async def execute(self, command: str, *args,
                      fetch: bool = False,
                      fetchval: bool = False,
                      fetchrow: bool = False,
                      execute: bool = False,
                      executemany:bool = False):
        self.pool = await asyncpg.create_pool(
                host=HOST,
                user=PG_USER,
                password=PG_PSWD,
                database='workers'
            )
        
        if fetch:
            result = await self.pool.fetch(command, *args)
        elif fetchval:
            result = await self.pool.fetchval(command, *args)
        elif fetchrow:
            result = await self.pool.fetchrow(command, *args)
        elif execute:
            result = await self.pool.execute(command, *args)
        elif executemany:
            result = await self.pool.executemany(command, *args)
        await self.pool.close()
        return result

DataBase = DataBaseClass()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(DataBaseClass.create_db())
