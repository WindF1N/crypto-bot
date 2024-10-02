# بِسْمِ ٱللّٰهِ ٱلرَّحْمٰنِ ٱلرَّحِيْمِ ( Bismillāhi r-Raḥmāni r-Raḥīm )

import asyncio
import traceback
import redis
import pytz
import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import ParseMode
from database import (
    create_tables, 
    get_user, add_user,
    get_subscribe, add_subscribe, get_active_subscribes, update_subscribe,
    add_payment
)
from dotenv import load_dotenv
import os

# Определение пути к файлу .env
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '.env'))

# Загрузка переменных окружения из файла .env
load_dotenv(dotenv_path=env_path)

# Инициализация бота и диспетчера
bot = Bot(token=os.getenv('BOT_TOKEN'))
dp = Dispatcher(bot)

# Инициализация Redis клиента
redis_client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))

moscow_tz = pytz.timezone('Europe/Moscow')

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
                           + f"""Обменник: <a href="{deal_data['changer_page']}">{deal_data['changer_name']}</a>\n""" \
                           + f"Рейтинг: {deal_data['changer_rating']}\n" \
                           + f"Ссылка: {deal_data['bestchange_url']}\n" \
                           + f"""К отдаче: ≈ {round(1000 / float(deal_data['bybit_price']), 4)} {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''}\n""" \
                           + f"""К получению: ≈ {round(1000 / float(deal_data['bybit_price']) * float(deal_data['bestchange_price']), 4)} {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n\n""" \
                           + f"Спред: {deal_data['spread']}\n" \
                           + f"Итого: {round(1000 / float(deal_data['bybit_price']) * float(deal_data['bestchange_price']), 4)} {deal_data['pair'].split('-')[1]}"
                elif int(deal_data["type"]) == 2:
                    text = f"""1. Bestchange: {deal_data['pair'].split('-')[0]}{f" {deal_data['network'].split('-')[0]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[0] != '') else ''} -> {deal_data['pair'].split('-')[1]}{f" {deal_data['network'].split('-')[1]}" if ("-" in deal_data['network'] and deal_data['network'].split('-')[1] != '') else ''}\n""" \
                           + f"Цена: {deal_data['bestchange_price']}\n" \
                           + f"""Обменник: <a href="{deal_data['changer_page']}">{deal_data['changer_name']}</a>\n""" \
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
                           + f"""Обменник: <a href="{deal_data['changer_page']}">{deal_data['changer_name']}</a>\n""" \
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
                users = await get_active_subscribes(datetime.datetime.now(moscow_tz))
                for user in users:
                    user = user["telegram_id"]
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
        
        # Ждем перед следующей проверкой
        await asyncio.sleep(int(os.getenv('TIME_TO_UPDATE_MESSAGES_IN_BOT')))

@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    moscow_now_date = datetime.datetime.now(moscow_tz)
    user = await get_user(message.from_user.id)
    if user:
        subscribe = await get_subscribe(user["telegram_id"])
        if subscribe["expire_in_datetime"].replace(tzinfo=moscow_tz) > moscow_now_date:
            await bot.send_message(message.from_user.id, f"""🟢 <b>Подписка активна до {subscribe["expire_in_datetime"].strftime('%H:%M %d.%m.%Y')}</b>""", parse_mode=ParseMode.HTML)
        else:
            keyboard = types.InlineKeyboardMarkup()
            keyboard.add(types.InlineKeyboardButton(text="Продлить за 🌟", callback_data="go_stars"))
            await bot.send_message(message.from_user.id, f"""🔴 <b>Подписка закончилась {subscribe["expire_in_datetime"].strftime('%H:%M %d.%m.%Y')}</b>""", parse_mode=ParseMode.HTML, reply_markup=keyboard)
    else:
        moscow_expire_in_datetime = datetime.datetime.now(moscow_tz) + datetime.timedelta(days=int(os.getenv('NUMBER_OF_TRIAL_DAYS')))
        await add_user(message.from_user.id, message.from_user.first_name, message.from_user.last_name, message.from_user.username, moscow_now_date)
        await add_subscribe(message.from_user.id, moscow_expire_in_datetime)
        await bot.send_message(message.from_user.id, f"""<b>{f"Привет, {message.from_user.username}!" if message.from_user.username else f"Привет!"}</b>\n\nВам выдана пробная подписка на <b>{os.getenv('NUMBER_OF_TRIAL_DAYS')} дней</b>""", parse_mode=ParseMode.HTML)
        await start(message)

# Обработчик колбэка "go_stars"
@dp.callback_query_handler(lambda c: c.data == 'go_stars')
async def process_callback_stars(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="1 неделя", callback_data="go_pay:7"), types.InlineKeyboardButton(text="2 недели", callback_data="go_pay:14"))
    keyboard.add(types.InlineKeyboardButton(text="1 месяц", callback_data="go_pay:30"), types.InlineKeyboardButton(text="3 месяца", callback_data="go_pay:90"))
    keyboard.add(types.InlineKeyboardButton(text="6 месяцев", callback_data="go_pay:180"), types.InlineKeyboardButton(text="Бессрочная", callback_data="go_pay:-1"))
    # Отправка сообщения с информацией о стоимости подписки
    await bot.send_message(callback_query.from_user.id, 
                           "Стоимость подписки:\n\n" \
                           + "- 1 неделя = 600 🌟\n" \
                           + "- 2 недели = 1000 🌟\n" \
                           + "- 1 месяц = 1600 🌟\n" \
                           + "- 3 месяца = 3500 🌟\n" \
                           + "- 6 месяцев = 6000 🌟\n" \
                           + "- бессрочная = 15300 🌟\n\n" \
                           + "Для оплаты за USDT или другим способом, свяжитесь с поддержкой - <b>@thecreatxr</b>\n\n" \
                           + "Выберите время подписки:", 
                           parse_mode=ParseMode.HTML,
                           reply_markup=keyboard)
    
# Обработчик колбэка "go_pay"
@dp.callback_query_handler(lambda c: c.data.startswith("go_pay:"))
async def process_callback_pay(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    # Получаем выбранный период подписки
    period = int(callback_query.data.split(":")[1])
    # Определяем стоимость подписки
    cost = os.getenv(f'COST_PRICE_{period}')
    if cost == None:
        return
    else:
        cost = int(cost)
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text=f"Оплатить {cost} ⭐️", pay=True))
    prices = [types.LabeledPrice(label="XTR", amount=cost)]
    await bot.send_invoice(
        chat_id=callback_query.from_user.id,
        title="Оплата подписки",
        description=f"""{f"Вы выбрали подписку на {period} дней" if period > 0 else "Вы выбрали бессрочную подписку"}. Стоимость: {cost} 🌟""",
        prices=prices,
        provider_token="",
        payload=f"{period}",
        currency="XTR",
        reply_markup=keyboard
    )

@dp.pre_checkout_query_handler()
async def pre_checkout_handler(pre_checkout_query: types.PreCheckoutQuery):  
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@dp.message_handler(content_types=types.ContentType.SUCCESSFUL_PAYMENT)
async def successful_payment_handler(message: types.Message):
    period = int(message.successful_payment.invoice_payload)
    if period == -1:
        period = 182500
    moscow_expire_in_datetime = datetime.datetime.now(moscow_tz) + datetime.timedelta(days=period)
    subscribe = await update_subscribe(message.from_user.id, moscow_expire_in_datetime)
    await add_payment(message.from_user.id, "XTR", message.successful_payment.total_amount, datetime.datetime.now(moscow_tz))
    await bot.send_message(message.from_user.id, f"""🟢 <b>Подписка активна до {subscribe["expire_in_datetime"].strftime('%H:%M %d.%m.%Y')}</b>""", parse_mode=ParseMode.HTML)

async def on_startup(db):
    # Создаём таблицы
    await create_tables()
    # Запускаем задачу для отправки выгодных сделок
    asyncio.create_task(send_profitable_deals())

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
    redis_client.close()