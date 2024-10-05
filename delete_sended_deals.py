# بِسْمِ ٱللّٰهِ ٱلرَّحْمٰنِ ٱلرَّحِيْمِ ( Bismillāhi r-Raḥmāni r-Raḥīm )

import asyncio
import traceback
import redis
import pytz
import datetime
from aiogram import Bot
from database import get_active_subscribes
from dotenv import load_dotenv
import os

# Определение пути к файлу .env
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '.env'))

# Загрузка переменных окружения из файла .env
load_dotenv(dotenv_path=env_path)

# Инициализация бота и диспетчера
bot = Bot(token=os.getenv('BOT_TOKEN'))

# Инициализация Redis клиента
redis_client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))

moscow_tz = pytz.timezone('Europe/Moscow')

async def delete_profitable_deals():
    while True:
        users = await get_active_subscribes(datetime.datetime.now(moscow_tz) - datetime.timedelta(minutes=5))
        for user in users:
            user = user["telegram_id"]
            for message_id_key in redis_client.keys(f'message_id_{user}:*'):
                # Получаем данные из Redis
                try:
                    deal_data = {key.decode('utf-8'): value.decode('utf-8') for key, value in redis_client.hgetall(f'profitable_deals:{message_id_key.decode("utf-8").replace(f"message_id_{user}:", "")}'.encode("utf-8")).items()}
                    if "type" not in deal_data:
                        deal_data = None
                except:
                    deal_data = None
                    traceback.print_exc()
                if deal_data == None:
                    # Если данные в Redis исчезли, удаляем сообщение
                    message_id = redis_client.get(message_id_key)
                    if message_id:
                        try:
                            await bot.delete_message(chat_id=user, message_id=int(message_id))
                            # Удаляем message_id из Redis
                            redis_client.delete(message_id_key)
                        except Exception as e:
                            print(f"Error deleting message: {e}")
                            if "Message can't be deleted for everyone" in str(e) or "Message to delete not found" in str(e):
                                # Удаляем message_id из Redis
                                redis_client.delete(message_id_key)

def main():
    # Запуск функции
    asyncio.run(delete_profitable_deals())

if __name__ == '__main__':
    main()