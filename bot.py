import asyncio
import traceback
import redis
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import ParseMode

# Настройки Redis
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

# Токен вашего бота
BOT_TOKEN = '7605371190:AAGZvr1w8LjDWEYQTlDa6RnAABOlodEX52o'

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# Инициализация Redis клиента
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

users = ["474372372", "453500861", "930385675", "661325490", "1061231427", "839202506"]

async def send_profitable_deals():
    while True:
        # Получаем все ключи, которые соответствуют шаблону profitable_deals:*
        for deal_key in redis_client.keys('profitable_deals:*'):
            # Получаем данные из Redis
            try:
                deal_data = {key.decode('utf-8'): value.decode('utf-8') for key, value in redis_client.hgetall(deal_key).items()}
                if "type" not in deal_data:
                    deal_data = None
            except:
                deal_data = None
            if deal_data:
                if int(deal_data["type"]) == 1:
                    text = f"1. ByBit: {' -> '.join(reversed(deal_data['pair'].split('-')))}\n" \
                           + f"Цена: {deal_data['bybit_price']}\n" \
                           + f"Спот: {deal_data['bybit_url']}\n\n" \
                           + f"""2. Bestchange: {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''} -> {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n""" \
                           + f"Цена: {deal_data['bestchange_price']}\n" \
                           + f"Обменник: {deal_data['changer_name']}\n" \
                           + f"Рейтинг: {deal_data['changer_rating']}\n" \
                           + f"Ссылка: {deal_data['bestchange_url']}\n" \
                           + f"""К отдаче: ≈ {round(1000 / float(deal_data['bybit_price']), 4)} {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''}\n""" \
                           + f"""К получению: ≈ {round(1000 / float(deal_data['bybit_price']) * float(deal_data['bestchange_price']), 4)} {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n\n""" \
                           + f"Спред: {deal_data['spread']}\n" \
                           + f"Итого: {round(1000 / float(deal_data['bybit_price']) * float(deal_data['bestchange_price']), 4)} {deal_data['pair'].split('-')[1]}"
                elif int(deal_data["type"]) == 2:
                    text = f"""1. Bestchange: {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''} -> {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n""" \
                           + f"Цена: {deal_data['bestchange_price']}\n" \
                           + f"Обменник: {deal_data['changer_name']}\n" \
                           + f"Рейтинг: {deal_data['changer_rating']}\n" \
                           + f"Ссылка: {deal_data['bestchange_url']}\n" \
                           + f"""К отдаче: ≈ 1000 {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''}\n""" \
                           + f"""К получению: ≈ {round(1000 * float(deal_data['bestchange_price']), 4)} {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n\n""" \
                           + f"2. ByBit: {' -> '.join(reversed(deal_data['pair'].split('-')))}\n" \
                           + f"Цена: {deal_data['bybit_price']}\n" \
                           + f"Спот: {deal_data['bybit_url']}\n\n" \
                           + f"Хедж: https://www.bybit.com/trade/usdt/{''.join(reversed(deal_data['pair'].split('-')))}\n\n" \
                           + f"Спред: {deal_data['spread']}\n" \
                           + f"Итого: {round(1000 * float(deal_data['bybit_price']) * float(deal_data['bestchange_price']), 4)} {deal_data['pair'].split('-')[0]}"
                elif int(deal_data["type"]) == 3:
                    text = f"1. ByBit: USDT -> {deal_data['pair'].split('-')[0]}\n" \
                           + f"Цена: {deal_data['bybit_price_0']}\n" \
                           + f"Спот: {deal_data['bybit_url_0']}\n\n" \
                           + f"""2. Bestchange: {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''} -> {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n""" \
                           + f"Цена: {deal_data['bestchange_price']}\n" \
                           + f"Обменник: {deal_data['changer_name']}\n" \
                           + f"Рейтинг: {deal_data['changer_rating']}\n" \
                           + f"Ссылка: {deal_data['bestchange_url']}\n" \
                           + f"""К отдаче: ≈ {round(1000 / float(deal_data['bybit_price_0']), 4)} {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''}\n""" \
                           + f"""К получению: ≈ {round(1000 / float(deal_data['bybit_price_0']) * float(deal_data['bestchange_price']), 4)} {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n\n""" \
                           + f"3. ByBit: {deal_data['pair'].split('-')[1]} -> USDT\n" \
                           + f"Цена: {deal_data['bybit_price_1']}\n" \
                           + f"Спот: {deal_data['bybit_url_1']}\n\n" \
                           + f"Хедж: https://www.bybit.com/trade/usdt/{deal_data['pair'].split('-')[1]}USDT\nv" \
                           + f"Спред: {deal_data['spread']}\n" \
                           + f"Итого: {round(1000 / float(deal_data['bybit_price_0']) * float(deal_data['bestchange_price']) * float(deal_data['bybit_price_1']), 4)} USDT"
                
                for user in users:
                    # Получаем или создаем ключ для хранения message_id
                    message_id_key = f"message_id_{user}:{deal_key.decode('utf-8').replace('profitable_deals:', '')}"
                    message_id = redis_client.get(message_id_key)

                    if message_id:
                        # Если message_id уже есть, обновляем сообщение
                        try:
                            await bot.edit_message_text(text=text, chat_id=user, message_id=int(message_id), parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
                        except Exception as e:
                            print(f"Error editing message: {e}")
                    else:
                        try:
                            # Если message_id нет, отправляем новое сообщение
                            sent_message = await bot.send_message(chat_id=user, text=text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
                            # Сохраняем message_id в Redis
                            redis_client.set(message_id_key, sent_message.message_id)
                        except Exception as e:
                            print(f"Error sending message: {e}")

        for user in users:
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
                        except Exception as e:
                            print(f"Error deleting message: {e}")
                        # Удаляем message_id из Redis
                        redis_client.delete(message_id_key)
                        # print(message_id_key)
        
        # Ждем 5 секунд перед следующей проверкой
        await asyncio.sleep(15)

@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    await message.reply("Привет! Я бот, который отправляет выгодные сделки.")

async def on_startup(dp):
    # Запускаем задачу для отправки выгодных сделок
    asyncio.create_task(send_profitable_deals())

if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup)
    redis_client.close()