import os

from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv('HOST')
PG_PSWD = os.getenv('PG_PSWD')
PG_USER = os.getenv('PG_USER')

