# بِسْمِ ٱللّٰهِ ٱلرَّحْمٰنِ ٱلرَّحِيْمِ ( Bismillāhi r-Raḥmāni r-Raḥīm )

import asyncio
import redis
import pytz
import datetime
from aiogram import Bot
from aiogram.types import ParseMode
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

async def send_message_to_user(user, deal_data, deal_key):
    user_settings = {key.decode('utf-8'): value.decode('utf-8') for key, value in redis_client.hgetall(f'monitoring_{user}').items()}
    if 'telegram_id' not in user_settings:
        return
    if float(user_settings['min_spread']) > float(deal_data["spread"].replace(" %", "")):
        return
    if deal_data["type"] == '1':
        if int(user_settings['usdt_balance']) / float(deal_data["bybit_price"]) < float(deal_data["inmin"]) or int(user_settings['usdt_balance']) / float(deal_data["bybit_price"]) > float(deal_data["inmax"]):
            return
    elif deal_data["type"] == '2':
        if int(user_settings['usdt_balance']) < float(deal_data["inmin"]) or int(user_settings['usdt_balance']) > float(deal_data["inmax"]):
            return
    elif deal_data["type"] == '3':
        if int(user_settings['usdt_balance']) / float(deal_data["bybit_price_0"]) < float(deal_data["inmin"]) or int(user_settings['usdt_balance']) / float(deal_data["bybit_price_0"]) > float(deal_data["inmax"]):
            return

    if int(deal_data["type"]) == 1:
        text = f"1. ByBit: {' -> '.join(reversed(deal_data['pair'].split('-')))}\n" \
            + f"Цена: {deal_data['bybit_price']}\n" \
            + f"Спот: {deal_data['bybit_url']}\n\n" \
            + f"""2. Bestchange: {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''} -> {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n""" \
            + f"Цена: {deal_data['bestchange_price']}\n" \
            + f"""Обменник: <a href="{deal_data['changer_page']}">{deal_data['changer_name']}</a>\n""" \
            + f"Рейтинг: {deal_data['changer_rating']}\n" \
            + f"Ссылка: {deal_data['bestchange_url']}\n" \
            + f"""К отдаче: ≈ {round(int(user_settings['usdt_balance']) / float(deal_data['bybit_price']), 4)} {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''}\n""" \
            + f"""К получению: ≈ {round(int(user_settings['usdt_balance']) / float(deal_data['bybit_price']) * float(deal_data['bestchange_price']), 4)} {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n\n""" \
            + f"Спред: {deal_data['spread']}\n" \
            + f"Итого: {round(int(user_settings['usdt_balance']) / float(deal_data['bybit_price']) * float(deal_data['bestchange_price']), 4)} {deal_data['pair'].split('-')[1]}"
    elif int(deal_data["type"]) == 2:
        text = f"""1. Bestchange: {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''} -> {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n""" \
            + f"Цена: {deal_data['bestchange_price']}\n" \
            + f"""Обменник: <a href="{deal_data['changer_page']}">{deal_data['changer_name']}</a>\n""" \
            + f"Рейтинг: {deal_data['changer_rating']}\n" \
            + f"Ссылка: {deal_data['bestchange_url']}\n" \
            + f"""К отдаче: ≈ {int(user_settings['usdt_balance'])} {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''}\n""" \
            + f"""К получению: ≈ {round(int(user_settings['usdt_balance']) * float(deal_data['bestchange_price']), 4)} {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n\n""" \
            + f"2. ByBit: {' -> '.join(reversed(deal_data['pair'].split('-')))}\n" \
            + f"Цена: {deal_data['bybit_price']}\n" \
            + f"Спот: {deal_data['bybit_url']}\n\n" \
            + f"Хедж: https://www.bybit.com/trade/usdt/{''.join(reversed(deal_data['pair'].split('-')))}\n\n" \
            + f"Спред: {deal_data['spread']}\n" \
            + f"Итого: {round(int(user_settings['usdt_balance']) * float(deal_data['bybit_price']) * float(deal_data['bestchange_price']), 4)} {deal_data['pair'].split('-')[0]}"
    elif int(deal_data["type"]) == 3:
        text = f"1. ByBit: USDT -> {deal_data['pair'].split('-')[0]}\n" \
            + f"Цена: {deal_data['bybit_price_0']}\n" \
            + f"Спот: {deal_data['bybit_url_0']}\n\n" \
            + f"""2. Bestchange: {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''} -> {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n""" \
            + f"Цена: {deal_data['bestchange_price']}\n" \
            + f"""Обменник: <a href="{deal_data['changer_page']}">{deal_data['changer_name']}</a>\n""" \
            + f"Рейтинг: {deal_data['changer_rating']}\n" \
            + f"Ссылка: {deal_data['bestchange_url']}\n" \
            + f"""К отдаче: ≈ {round(int(user_settings['usdt_balance']) / float(deal_data['bybit_price_0']), 4)} {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''}\n""" \
            + f"""К получению: ≈ {round(int(user_settings['usdt_balance']) / float(deal_data['bybit_price_0']) * float(deal_data['bestchange_price']), 4)} {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n\n""" \
            + f"3. ByBit: {deal_data['pair'].split('-')[1]} -> USDT\n" \
            + f"Цена: {deal_data['bybit_price_1']}\n" \
            + f"Спот: {deal_data['bybit_url_1']}\n\n" \
            + f"Хедж: https://www.bybit.com/trade/usdt/{deal_data['pair'].split('-')[1]}USDT\nv" \
            + f"Спред: {deal_data['spread']}\n" \
            + f"Итого: {round(int(user_settings['usdt_balance']) / float(deal_data['bybit_price_0']) * float(deal_data['bestchange_price']) * float(deal_data['bybit_price_1']), 4)} USDT"

    # Получаем или создаем ключ для хранения message_id
    message_id_key = f"message_id_{user}:{deal_key.decode('utf-8').replace('profitable_deals:', '')}"
    message_id = redis_client.get(message_id_key)
    if message_id:
        # Если message_id уже есть, обновляем сообщение
        try:
            await bot.edit_message_text(text=text, chat_id=user, message_id=int(message_id), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e:
            print(f"Error editing message: {e}")
    else:
        try:
            # Если message_id нет, отправляем новое сообщение
            sent_message = await bot.send_message(chat_id=user, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            # Сохраняем message_id в Redis
            redis_client.set(message_id_key, sent_message.message_id)
        except Exception as e:
            print(f"Error sending message: {e}")

async def send_profitable_deals():
    while True:
        try:
            # Получаем все ключи, которые соответствуют шаблону profitable_deals:*
            for deal_key in redis_client.keys('profitable_deals:*'):
                # Получаем данные из Redis
                try:
                    deal_data = {key.decode('utf-8'): value.decode('utf-8') for key, value in redis_client.hgetall(deal_key).items()}
                    if "type" not in deal_data:
                        deal_data = None
                except Exception as e:
                    print(f"Error fetching deal data: {e}")
                    deal_data = None

                if deal_data:
                    users = await get_active_subscribes(datetime.datetime.now(moscow_tz))
                    tasks = []
                    for user in users:
                        user_id = user["telegram_id"]
                        tasks.append(send_message_to_user(user_id, deal_data, deal_key))
                    await asyncio.gather(*tasks)
                    await asyncio.sleep(2)
        except Exception as e:
            print(f"Error in send_profitable_deals: {e}")

def main():
    # Запуск функции
    asyncio.run(send_profitable_deals())

if __name__ == '__main__':
    main()