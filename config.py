import os
from aiogram import Bot
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv('HOST')
PG_PSWD = os.getenv('PG_PSWD')
PG_USER = os.getenv('PG_USER')
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_CHAT = int(os.getenv('ADMIN_CHAT'))

bot = Bot(BOT_TOKEN)