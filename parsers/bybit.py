# بِسْمِ ٱللّٰهِ ٱلرَّحْمٰنِ ٱلرَّحِيْمِ ( Bismillāhi r-Raḥmāni r-Raḥīm )

import requests
import redis
import traceback
import time
from dotenv import load_dotenv
import os

# Определение пути к файлу .env
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../.env'))

# Загрузка переменных окружения из файла .env
load_dotenv(dotenv_path=env_path)

NETWORK_SYMBOLS_STR = os.getenv('NETWORK_SYMBOLS', '')
NETWORK_SYMBOLS = NETWORK_SYMBOLS_STR.split(',') if NETWORK_SYMBOLS_STR else []

def get_all_symbols():
    client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))
    symbols = []
    try:
        for rate_key in client.keys('rate:*'):
            symbols_list = rate_key.decode('utf-8').split(':')[1].split("-")
            symbol = ''
            for smbl in symbols_list:
                _ = ""
                if smbl in NETWORK_SYMBOLS:
                    if symbol != "":
                        _ += '-' + smbl
                    else:
                        _ += smbl
                    symbol += _
                else:
                    for NETWORK_SYMBOL in NETWORK_SYMBOLS:
                        if smbl.endswith(NETWORK_SYMBOL):
                            if symbol != "":
                                _ += '-'
                            _ += smbl.replace(NETWORK_SYMBOL, '')
                            break
                    if _ == "":
                        if symbol != "":
                             _ += '-'
                        _ += smbl
                    symbol += _
            if "DASH" in symbol:
                continue
            if symbol not in symbols:
                symbols.append(symbol)
            # print(f"Найдена пара: {symbol} из {rate_key.decode('utf-8').split(':')[1]}")
    except:
        traceback.print_exc()
        return []
    finally:
        client.close()
        return symbols

def get_spot_prices(symbols):
    client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))
    try:
        # URL для получения спотовых цен
        url = "https://api.bybit.com/spot/v3/public/quote/ticker/price"
        for symbol in symbols:
            try:
                if isinstance(symbol, bytes):
                    symbol = symbol.decode('utf-8')
                # Отправка GET-запроса к API Bybit
                response = requests.get(url + "?symbol=" + symbol.replace("-", ''))
                response.raise_for_status()  # Проверка на ошибки HTTP
                
                # Парсинг JSON-ответа
                data = response.json()

                # Проверка успешности запроса
                if data["retCode"] == 0:
                    # Вывод спотовых цен
                    ticker = data["result"]
                    # print(f"Символ: {symbol.replace('-', '')}, Цена: {ticker['price']}")
                    client.hset(f"bybit_rate:{symbol.replace('-', '')}", mapping={"price": ticker['price']})
                    client.expire(f"bybit_rate:{symbol.replace('-', '')}", os.getenv('RATES_EXPIRE_TIME'))
                    rate_keys = client.lrange('bybit_symbols', 0, -1)
                    if not rate_keys:
                        rate_keys = []
                    if symbol not in [key.decode('utf-8') for key in rate_keys]:
                        rate_keys.append(symbol)
                        client.rpush('bybit_symbols', symbol)
                else:
                    client.lrem('bybit_symbols', 0, symbol)
                    # print(f"Символ: {symbol.replace('-', '')}, Ошибка: {data['retMsg']}")
                
            except requests.RequestException as e:
                print(f"Error fetching spot prices: {e}")
                time.sleep(1.5)
                get_spot_prices([symbol])
        
    except:
        traceback.print_exc()

    finally:
        client.close()

def main():
    while True:
        symbols = get_all_symbols()
        get_spot_prices(symbols)

if __name__ == "__main__":
    main()
