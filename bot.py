# بِسْمِ ٱللّٰهِ ٱلرَّحْمٰنِ ٱلرَّحِيْمِ ( Bismillāhi r-Raḥmāni r-Raḥīm )

import asyncio
import redis
import pytz
import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import ParseMode
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from database import (
    create_tables, 
    get_user, add_user,
    get_subscribe, add_subscribe, update_subscribe,
    add_payment,
    get_user_settings, update_user_settings
)
from dotenv import load_dotenv
import os

# Определение пути к файлу .env
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '.env'))

# Загрузка переменных окружения из файла .env
load_dotenv(dotenv_path=env_path)

# Инициализация бота и диспетчера
bot = Bot(token=os.getenv('BOT_TOKEN'))
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Инициализация Redis клиента
redis_client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))

moscow_tz = pytz.timezone('Europe/Moscow')

main_keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
main_keyboard.add(types.KeyboardButton(text="🔎 Активировать мониторинг"))
main_keyboard.add(types.KeyboardButton(text="⚙️ Настройки"), types.KeyboardButton(text="📒 FAQ"))

class SettingsStates(StatesGroup):
    usdt_balance = State()
    max_negative_reviews_bestchange = State()
    min_positive_reviews_bestchange = State()
    min_lifetime = State()
    min_spread = State()

@dp.message_handler(text="✖️ Отмена", state=SettingsStates)
async def cancel_handler(message: types.Message, state: FSMContext):
    if message.chat.type == 'private':
        await state.finish()
        await bot.send_message(message.chat.id, f'Редактирование отменено.', reply_markup=main_keyboard)

@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    if message.chat.type == 'private':
        await deactivate_monitoring_handler(message)
        moscow_now_date = datetime.datetime.now(moscow_tz)
        user = await get_user(message.from_user.id)
        if user:
            subscribe = await get_subscribe(user["telegram_id"])
            if subscribe["expire_in_datetime"].replace(tzinfo=moscow_tz) > moscow_now_date:
                await bot.send_message(message.from_user.id, f"""🟢 <b>Подписка активна до {subscribe["expire_in_datetime"].strftime('%H:%M %d.%m.%Y')}</b>""", parse_mode=ParseMode.HTML, reply_markup=main_keyboard)
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

# Обработчик кнопки "📒 FAQ"
@dp.message_handler(text="📒 FAQ")
async def faq_handler(message: types.Message):
    if message.chat.type == 'private':
        await deactivate_monitoring_handler(message)
        await bot.send_message(message.from_user.id, f"В разработке...", parse_mode=ParseMode.HTML)

# Обработчик кнопки "🔎 Активировать мониторинг"
@dp.message_handler(text="🔎 Активировать мониторинг")
async def activate_monitoring_handler(message: types.Message):
    if message.chat.type == 'private':
        monitoring_data = {key.decode('utf-8'): value.decode('utf-8') for key, value in redis_client.hgetall(f'monitoring_{message.from_user.id}').items()}
        if 'telegram_id' in monitoring_data:
            return
        moscow_now_date = datetime.datetime.now(moscow_tz)
        user = await get_user(message.from_user.id)
        if not user:
            await start(message)
            return
        subscribe = await get_subscribe(user["telegram_id"])
        if not subscribe:
            await start(message)
            return
        else:
            if subscribe["expire_in_datetime"].replace(tzinfo=moscow_tz) <= moscow_now_date:
                await start(message)
                return
        user_settings = await get_user_settings(message.from_user.id)
        redis_client.hset(
            f"monitoring_{message.from_user.id}",
            mapping = {
                "telegram_id": int(user_settings["telegram_id"]),
                "usdt_balance": int(user_settings["usdt_balance"]),
                "max_negative_reviews_bestchange": int(user_settings["max_negative_reviews_bestchange"]),
                "min_positive_reviews_bestchange": int(user_settings["min_positive_reviews_bestchange"]),
                "min_lifetime": int(user_settings["min_lifetime"]),
                "min_spread": str(user_settings["min_spread"]),
            }
        )
        # redis_client.expire(f"monitoring_{message.from_user.id}", 60*60)
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add("✖️ Завершить мониторинг")
        await bot.send_message(message.chat.id, "🟢 Мониторинг успешно активирован.", parse_mode=ParseMode.HTML, reply_markup=keyboard)
        # Ждем пока не выключится мониторинг
        # await asyncio.sleep(60*60)
        # await deactivate_monitoring_handler(message)

# Обработчик кнопки "✖️ Завершить мониторинг"
@dp.message_handler(text="✖️ Завершить мониторинг")
async def deactivate_monitoring_handler(message: types.Message):
    if message.chat.type == 'private':
        result = redis_client.hget(f'monitoring_{message.from_user.id}', 'telegram_id')
        if result:
            redis_client.delete(f'monitoring_{message.from_user.id}')
            for message_id_key in redis_client.keys(f'message_id_{message.from_user.id}:*'):
                message_id = redis_client.get(message_id_key)
                if message_id:
                    try:
                        await bot.delete_message(chat_id=message.from_user.id, message_id=int(message_id))
                        # Удаляем message_id из Redis
                        redis_client.delete(message_id_key)
                    except Exception as e:
                        print(f"Error deleting message: {e}")
                        if "Message can't be deleted for everyone" in str(e):
                            # Удаляем message_id из Redis
                            redis_client.delete(message_id_key)
            await bot.send_message(message.chat.id, "🔴 Мониторинг успешно завершён", reply_markup=main_keyboard)

# Обработчик кнопки "⚙️ Настройки"
@dp.message_handler(text="⚙️ Настройки")
async def settings_handler(message: types.Message):
    if message.chat.type == 'private':
        await deactivate_monitoring_handler(message)
        user = await get_user(message.from_user.id)
        if not user:
            return
        user_settings = await get_user_settings(message.from_user.id)
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton(text="Изменить баланс", callback_data="edit_settings:usdt_balance"))
        keyboard.add(types.InlineKeyboardButton(text="Изменить макс. количество негативных отзывов", callback_data="edit_settings:max_negative_reviews_bestchange"))
        keyboard.add(types.InlineKeyboardButton(text="Изменить мин. количество положительных отзывов", callback_data="edit_settings:min_positive_reviews_bestchange"))
        keyboard.add(types.InlineKeyboardButton(text="Изменить мин. время жизни связки", callback_data="edit_settings:min_lifetime"))
        keyboard.add(types.InlineKeyboardButton(text="Изменить мин. спред", callback_data="edit_settings:min_spread"))
        await bot.send_message(message.from_user.id, 
                               f"<b>Ваши настройки:</b>\n\n" \
                               + f"Баланс: <b>{user_settings['usdt_balance']} USDT</b>\n\n" \
                               + f"Максимальное количество негативных отзывов на обменниках из BestChange: <b>{user_settings['max_negative_reviews_bestchange']} шт.</b>\n\n" \
                               + f"Минимальное количество положительных отзывов на обменниках из BestChange: <b>{user_settings['min_positive_reviews_bestchange']} шт.</b>\n\n" \
                               + f"Минимальное время жизни связки: <b>{user_settings['min_lifetime']} сек</b>\n\n" \
                               + f"Минимальный спред: <b>{round(float(user_settings['min_spread']), 2)} %</b>\n\n" \
                               + f"📝 Используйте кнопки ниже для того, чтобы изменить настройки мониторинга.",
                               reply_markup=keyboard,
                               parse_mode=ParseMode.HTML)

# Обработчик колбэка "edit_settings"
@dp.callback_query_handler(lambda c: c.data.startswith("edit_settings:"))
async def process_callback_edit_settings(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    variable = callback_query.data.split(":")[1]
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add("✖️ Отмена")
    if variable == "usdt_balance":
        await bot.send_message(callback_query.from_user.id, "Введите количество USDT:", reply_markup=keyboard)
        await SettingsStates.usdt_balance.set()
    elif variable == "max_negative_reviews_bestchange":
        await bot.send_message(callback_query.from_user.id, "Введите макс. количество негативных отзывов:", reply_markup=keyboard)
        await SettingsStates.max_negative_reviews_bestchange.set()
    elif variable == "min_positive_reviews_bestchange":
        await bot.send_message(callback_query.from_user.id, "Введите мин. количество положительных отзывов:", reply_markup=keyboard)
        await SettingsStates.min_positive_reviews_bestchange.set()
    elif variable == "min_lifetime":
        await bot.send_message(callback_query.from_user.id, "Введите мин. время жизни связки:", reply_markup=keyboard)
        await SettingsStates.min_lifetime.set()
    elif variable == "min_spread":
        await bot.send_message(callback_query.from_user.id, "Введите мин. спред:", reply_markup=keyboard)
        await SettingsStates.min_spread.set()

# Обработчик количества USDT
@dp.message_handler(state=SettingsStates.usdt_balance)
async def process_usdt_balance(message: types.Message, state: FSMContext):
    try:
        value = int(message.text)
        if 999999 < value or value < 0:
            0 / 0
        await update_user_settings(message.chat.id, 'usdt_balance', value)
        await state.finish()
        await message.answer("Количество USDT успешно изменено.", reply_markup=main_keyboard)
        await settings_handler(message)
    except:
        await message.answer("Некорректный формат, количество USDT должно быть указано целым числом (макс. 999999).\n\nПопробуйте еще раз.")

# Обработчик макс. количество негативных отзывов
@dp.message_handler(state=SettingsStates.max_negative_reviews_bestchange)
async def process_max_negative_reviews_bestchange(message: types.Message, state: FSMContext):
    try:
        value = int(message.text)
        if 999999 < value or value < 0:
            0 / 0
        await update_user_settings(message.from_user.id, 'max_negative_reviews_bestchange', value)
        await state.finish()
        await message.answer("Макс. количество негативных отзывов успешно изменено.", reply_markup=main_keyboard)
        await settings_handler(message)
    except:
        await message.answer("Некорректный формат, макс. количество негативных отзывов должно быть указано целым числом (макс. 999999).\n\nПопробуйте еще раз.")

# Обработчик мин. количество положительных отзывов
@dp.message_handler(state=SettingsStates.min_positive_reviews_bestchange)
async def process_min_positive_reviews_bestchange(message: types.Message, state: FSMContext):
    try:
        value = int(message.text)
        if 999999 < value or value < 0:
            0 / 0
        await update_user_settings(message.from_user.id, 'min_positive_reviews_bestchange', value)
        await state.finish()
        await message.answer("Мин. количество положительных отзывов успешно изменено.", reply_markup=main_keyboard)
        await settings_handler(message)
    except:
        await message.answer("Некорректный формат, мин. количество положительных отзывов должно быть указано целым числом (макс. 999999).\n\nПопробуйте еще раз.")

# Обработчик мин. время жизни связки
@dp.message_handler(state=SettingsStates.min_lifetime)
async def process_min_lifetime(message: types.Message, state: FSMContext):
    try:
        value = int(message.text)
        if 999999 < value or value < 0:
            0 / 0
        await update_user_settings(message.from_user.id, 'min_lifetime', value)
        await state.finish()
        await message.answer("Мин. время жизни связки успешно изменено.", reply_markup=main_keyboard)
        await settings_handler(message)
    except:
        await message.answer("Некорректный формат, мин. время жизни связки должно быть указано целым числом (макс. 999999).\n\nПопробуйте еще раз.")

# Обработчик мин. время жизни связки
@dp.message_handler(state=SettingsStates.min_spread)
async def process_min_spread(message: types.Message, state: FSMContext):
    try:
        value = float(message.text.replace(',', '.'))
        if 999999.99 < value or value < 0:
            0 / 0
        await update_user_settings(message.from_user.id, 'min_spread', f"'{value}'")
        await state.finish()
        await message.answer("Мин. спред успешно изменён.", reply_markup=main_keyboard)
        await settings_handler(message)
    except:
        await message.answer("Некорректный формат, мин. спред должен быть указан как целое или дробное число с 2 знаками после запятой (макс. 999999,99).\n\nПопробуйте еще раз.")

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
                           + f"- 1 неделя = {os.getenv(f'COST_PRICE_7')} 🌟\n" \
                           + f"- 2 недели = {os.getenv(f'COST_PRICE_14')} 🌟\n" \
                           + f"- 1 месяц = {os.getenv(f'COST_PRICE_30')} 🌟\n" \
                           + f"- 3 месяца = {os.getenv(f'COST_PRICE_90')} 🌟\n" \
                           + f"- 6 месяцев = {os.getenv(f'COST_PRICE_180')} 🌟\n" \
                           + f"- бессрочная = {os.getenv(f'COST_PRICE_-1')} 🌟\n\n" \
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

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
    redis_client.close()
