# بِسْمِ ٱللّٰهِ ٱلرَّحْمٰنِ ٱلرَّحِيْمِ ( Bismillāhi r-Raḥmāni r-Raḥīm )

import aiomysql
import asyncio
import datetime
import pytz
from dotenv import load_dotenv
import os

# Определение пути к файлу .env
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '.env'))

# Загрузка переменных окружения из файла .env
load_dotenv(dotenv_path=env_path)

# سبحان الله ( Subhanallah ) Преславен Аллах! * 33
# الحمد لله ( Alhamdulillah ) Хвала Аллаху! * 33
# الله اكبر ( Allahu Akbar ) Аллах велик! * 33

# Создание всех таблиц MySQL
async def create_tables():
    try:
        loop = asyncio.get_event_loop()
        pool = await aiomysql.create_pool(host=os.getenv('DATABASE_HOST'), user=os.getenv('DATABASE_USER'), 
                                          password=os.getenv('DATABASE_PASSWORD'), db=os.getenv('DATABASE_NAME'), loop=loop)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        telegram_id BIGINT,
                        first_name VARCHAR(255),
                        last_name VARCHAR(255),
                        username VARCHAR(255),
                        date_of_start DATETIME,
                        banned TINYINT(1) DEFAULT 0,
                        is_admin BOOLEAN DEFAULT 0,
                        PRIMARY KEY (telegram_id),
                        INDEX (is_admin),
                        INDEX (banned),
                        INDEX (username)
                    );
                    CREATE TABLE IF NOT EXISTS subscribes (
                        telegram_id BIGINT,
                        expire_in_datetime DATETIME,
                        PRIMARY KEY (telegram_id),
                        INDEX (expire_in_datetime),
                        INDEX (telegram_id, expire_in_datetime)
                    );
                    CREATE TABLE IF NOT EXISTS payments (
                        telegram_id BIGINT,
                        currency VARCHAR(12),
                        amount INT,
                        created_at DATETIME,
                        INDEX (telegram_id),
                        INDEX (created_at),
                        INDEX (currency),
                        INDEX (amount),
                        INDEX (telegram_id, created_at, amount),
                        INDEX (created_at, amount),
                        INDEX (telegram_id, amount),
                        INDEX (telegram_id, created_at)
                    );
                    CREATE TABLE IF NOT EXISTS userSettings (
                        telegram_id BIGINT,
                        usdt_balance INT DEFAULT 1000,
                        max_negative_reviews_bestchange INT DEFAULT 0,
                        min_positive_reviews_bestchange INT DEFAULT 500,
                        min_lifetime INT DEFAULT 0,
                        min_spread DECIMAL(10, 2) DEFAULT 1,
                        PRIMARY KEY (telegram_id),
                        INDEX (telegram_id)
                    );
                """)
        pool.close()
        await pool.wait_closed()
    except aiomysql.Error as e:
        print("Ошибка при создании таблиц:", e)

# سبحان الله ( Subhanallah ) Преславен Аллах! * 33
# الحمد لله ( Alhamdulillah ) Хвала Аллаху! * 33
# الله اكبر ( Allahu Akbar ) Аллах велик! * 33

# Получение пользователя по telegram_id
async def get_user(telegram_id):
    try:
        loop = asyncio.get_event_loop()
        pool = await aiomysql.create_pool(host=os.getenv('DATABASE_HOST'), user=os.getenv('DATABASE_USER'), password=os.getenv('DATABASE_PASSWORD'), 
                                          db=os.getenv('DATABASE_NAME'), loop=loop, cursorclass=aiomysql.DictCursor)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Read a single record
                sql = f"SELECT * FROM users WHERE telegram_id = {telegram_id}"
                await cursor.execute(sql)
                result = await cursor.fetchone()
        return result
    except aiomysql.Error as e:
        print("Ошибка при получении пользователя:", e)
    finally:
        pool.close()
        await pool.wait_closed()

# Создание пользователя
async def add_user(telegram_id, first_name, last_name, username, date_of_start):
    try:
        loop = asyncio.get_event_loop()
        pool = await aiomysql.create_pool(host=os.getenv('DATABASE_HOST'), user=os.getenv('DATABASE_USER'), 
                                          password=os.getenv('DATABASE_PASSWORD'), db=os.getenv('DATABASE_NAME'), loop=loop, autocommit=True)

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO users (telegram_id, first_name, last_name, username, date_of_start)
                    VALUES (%s, %s, %s, %s, %s)
                """, (telegram_id, first_name, last_name, username, date_of_start))

        pool.close()
        await pool.wait_closed()
    except aiomysql.Error as e:
        print("Ошибка при создании пользователя:", e)

# سبحان الله ( Subhanallah ) Преславен Аллах! * 33
# الحمد لله ( Alhamdulillah ) Хвала Аллаху! * 33
# الله اكبر ( Allahu Akbar ) Аллах велик! * 33

# Получение подписки пользователя по telegram_id
async def get_subscribe(telegram_id):
    try:
        loop = asyncio.get_event_loop()
        pool = await aiomysql.create_pool(host=os.getenv('DATABASE_HOST'), user=os.getenv('DATABASE_USER'), password=os.getenv('DATABASE_PASSWORD'), 
                                          db=os.getenv('DATABASE_NAME'), loop=loop, cursorclass=aiomysql.DictCursor)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Read a single record
                sql = f"SELECT * FROM subscribes WHERE telegram_id = {telegram_id}"
                await cursor.execute(sql)
                result = await cursor.fetchone()
        return result
    except aiomysql.Error as e:
        print("Ошибка при получении подписки пользователя:", e)
    finally:
        pool.close()
        await pool.wait_closed()

# Получение активных подписок пользователей по moscow_datetime_now
async def get_active_subscribes(moscow_datetime_now):
    try:
        loop = asyncio.get_event_loop()
        pool = await aiomysql.create_pool(host=os.getenv('DATABASE_HOST'), user=os.getenv('DATABASE_USER'), password=os.getenv('DATABASE_PASSWORD'), 
                                          db=os.getenv('DATABASE_NAME'), loop=loop, cursorclass=aiomysql.DictCursor)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Read a single record
                sql = f"SELECT * FROM subscribes WHERE expire_in_datetime > '{moscow_datetime_now}'"
                await cursor.execute(sql)
                result = await cursor.fetchall()
        return result
    except aiomysql.Error as e:
        print("Ошибка при получении активных подписок пользователей:", e)
    finally:
        pool.close()
        await pool.wait_closed()

# Создание подписки пользователя
async def add_subscribe(telegram_id, expire_in_datetime):
    try:
        loop = asyncio.get_event_loop()
        pool = await aiomysql.create_pool(host=os.getenv('DATABASE_HOST'), user=os.getenv('DATABASE_USER'), 
                                          password=os.getenv('DATABASE_PASSWORD'), db=os.getenv('DATABASE_NAME'), loop=loop, autocommit=True)

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO subscribes (telegram_id, expire_in_datetime)
                    VALUES (%s, %s)
                """, (telegram_id, expire_in_datetime))

        pool.close()
        await pool.wait_closed()
    except aiomysql.Error as e:
        print("Ошибка при создании подписки пользователя:", e)

# Обновление подписки пользователя
async def update_subscribe(telegram_id, expire_in_datetime):
    try:
        loop = asyncio.get_event_loop()
        pool = await aiomysql.create_pool(host=os.getenv('DATABASE_HOST'), user=os.getenv('DATABASE_USER'), 
                                          password=os.getenv('DATABASE_PASSWORD'), db=os.getenv('DATABASE_NAME'), loop=loop, autocommit=True)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                result = await get_subscribe(telegram_id)
                if result:
                    await cursor.execute("""
                        UPDATE subscribes SET expire_in_datetime = %s WHERE telegram_id = %s
                    """, (expire_in_datetime, telegram_id))
                result = await get_subscribe(telegram_id)
        return result
    except aiomysql.Error as e:
        print("Ошибка при обновлении подписки пользователя:", e)
    finally:
        pool.close()
        await pool.wait_closed()

# سبحان الله ( Subhanallah ) Преславен Аллах! * 33
# الحمد لله ( Alhamdulillah ) Хвала Аллаху! * 33
# الله اكبر ( Allahu Akbar ) Аллах велик! * 33

# Создание платежа пользователя
async def add_payment(telegram_id, currency, amount, created_at):
    try:
        loop = asyncio.get_event_loop()
        pool = await aiomysql.create_pool(host=os.getenv('DATABASE_HOST'), user=os.getenv('DATABASE_USER'), 
                                          password=os.getenv('DATABASE_PASSWORD'), db=os.getenv('DATABASE_NAME'), loop=loop, autocommit=True)

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO payments (telegram_id, currency, amount, created_at)
                    VALUES (%s, %s, %s, %s)
                """, (telegram_id, currency, amount, created_at))

        pool.close()
        await pool.wait_closed()
    except aiomysql.Error as e:
        print("Ошибка при создании платежа пользователя:", e)

# سبحان الله ( Subhanallah ) Преславен Аллах! * 33
# الحمد لله ( Alhamdulillah ) Хвала Аллаху! * 33
# الله اكبر ( Allahu Akbar ) Аллах велик! * 33

# Получение настроек пользователя по telegram_id
async def get_user_settings(telegram_id):
    try:
        loop = asyncio.get_event_loop()
        pool = await aiomysql.create_pool(host=os.getenv('DATABASE_HOST'), user=os.getenv('DATABASE_USER'), password=os.getenv('DATABASE_PASSWORD'), 
                                          db=os.getenv('DATABASE_NAME'), loop=loop, autocommit=True, cursorclass=aiomysql.DictCursor)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Read a single record
                sql = f"SELECT * FROM userSettings WHERE telegram_id = {telegram_id}"
                await cursor.execute(sql)
                result = await cursor.fetchone()
                if not result:
                    await cursor.execute("""
                        INSERT INTO userSettings (telegram_id)
                        VALUES (%s)
                    """, (telegram_id,))
                    await cursor.execute(sql)
                    result = await cursor.fetchone()
        return result
    except aiomysql.Error as e:
        print("Ошибка при получении настроек пользователя:", e)
    finally:
        pool.close()
        await pool.wait_closed()

# Обновление настроек пользователя
async def update_user_settings(telegram_id, variable, value):
    try:
        loop = asyncio.get_event_loop()
        pool = await aiomysql.create_pool(host=os.getenv('DATABASE_HOST'), user=os.getenv('DATABASE_USER'), 
                                          password=os.getenv('DATABASE_PASSWORD'), db=os.getenv('DATABASE_NAME'), loop=loop, autocommit=True)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(f"""UPDATE userSettings SET {variable} = {value} WHERE telegram_id = {telegram_id}""")
    except aiomysql.Error as e:
        print("Ошибка при обновлении настроек пользователя:", e)
    finally:
        pool.close()
        await pool.wait_closed()