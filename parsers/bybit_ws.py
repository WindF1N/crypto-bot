import websocket
import json
from datetime import datetime, timedelta
import redis
from dotenv import load_dotenv
import os

# Определение пути к файлу .env
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../.env'))

# Загрузка переменных окружения из файла .env
load_dotenv(dotenv_path=env_path)

# URL для подключения к WebSocket
ws_url = "wss://ws2.bybit.com/spot/ws/quote/v2?_platform=2&timestamp=1726741315626"

# Подписанные символы
sended_symbols = []

def update_symbols(first=False):
    client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))
    try:
        if first:
            client.delete("ws_sended_symbols")
        sended_symbols = client.lrange('ws_sended_symbols', 0, -1)
        symbols = get_symbols()
        for symbol in symbols:
            if "DASH" in symbol.decode("utf-8"):
                continue
            if symbol not in sended_symbols:
                ws.send(json.dumps({
                    "event": "sub",
                    "params": {"binary": False},
                    "binary": False,
                    "symbol": symbol.decode("utf-8").replace("-", ""),
                    "topic": "realtimes"
                }))
                client.rpush('ws_sended_symbols', symbol)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()

# Функция для обработки сообщений от WebSocket
def on_message(ws, message):
    # print(f"Received message: {message}")
    data = json.loads(message)
    if "topic" in data:
        if data["topic"] == "realtimes":
            symbol = data["symbol"]
            price = data["data"][0]["c"]
            print(f"Symbol: {symbol}, Price: {price}")
            client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))
            client.hset(f"bybit_rate:{symbol}", mapping={"price": price})
            client.expire(f"bybit_rate:{symbol}", os.getenv('RATES_EXPIRE_TIME'))
            update_symbols()
            ws.send(json.dumps({
                "params": {"binary": False},
                "binary": False,
                "ping": datetime.now().timestamp() * 1000,
            }))

# Функция для обработки ошибок
def on_error(ws, error):
    print(f"Error: {error}")

# Функция для обработки закрытия соединения
def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed")
    reconnect()

# Функция для обработки открытия соединения
def on_open(ws):
    print("WebSocket connection opened")
    update_symbols(first=True)

# Функция для переподключения
def reconnect():
    print("Attempting to reconnect...")
    ws.run_forever()

def get_symbols():
    client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))
    symbols = []
    try:
        # client.delete("bybit_symbols")
        symbols = client.lrange('bybit_symbols', 0, -1)
        if not symbols:
            symbols = []
    except Exception as e:
        print(f"Error printing rates: {e}")
        return symbols
    finally:
        client.close()
        return symbols

def main():
    global ws
    # Создание WebSocketApp и установка обработчиков событий
    ws = websocket.WebSocketApp(ws_url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    # Бесконечный цикл для переподключения
    ws.run_forever()

if __name__ == "__main__":
    main()