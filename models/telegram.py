import aiogram
import logging

from models.config import Config

config = Config()

API_TOKEN = '1590089649:AAF4dIz3QlplEfB9-G5WZeU-OBxxJqJMIyI'

bot = aiogram.Bot(token=API_TOKEN)

async def tg_send_msg(text):
    try:
        await bot.send_message(-1001407255531, text)
    except Exception as e:
        print(e)