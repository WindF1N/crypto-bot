import requests
import redis
import traceback
from dotenv import load_dotenv
import os
import json

# Определение пути к файлу .env
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../.env'))

# Загрузка переменных окружения из файла .env
load_dotenv(dotenv_path=env_path)

NETWORK_SYMBOLS_STR = os.getenv('NETWORK_SYMBOLS', '')
NETWORK_SYMBOLS = NETWORK_SYMBOLS_STR.split(',') if NETWORK_SYMBOLS_STR else []

def fetch_currencies():
    client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))
    try:
        response = requests.get(f"https://www.bestchange.app/v2/{os.getenv('BESTCHANGE_API_KEY')}/currencies/ru")
        response.raise_for_status()
        currencies = response.json()["currencies"]
        for currency in currencies:
            if not currency["crypto"]:
                # print(f'Не прошёл фильтрацию: {currency["code"]}')
                continue
            currency_key = f'currency:{currency["id"]}'
            client.hset(currency_key, mapping={str(k): str(v) for k, v in currency.items()})
            client.expire(currency_key, os.getenv('RATES_EXPIRE_TIME'))
    except requests.RequestException as e:
        print(f"Error fetching currencies: {e}")
    except:
        traceback.print_exc()
    finally:
        client.close()

def process_payload(payload, data):
    client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))
    try:
        response = requests.get(f"https://www.bestchange.app/v2/{os.getenv('BESTCHANGE_API_KEY')}/rates/{'+'.join(payload)}")
        response.raise_for_status()
        rates = response.json()["rates"]
        for rate_key, changers in rates.items():
            for changer in changers:
                symbols_list = data[rate_key].split("-")
                symbol = ''
                network = ''
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
                                    if network == '':
                                        network = '-' + NETWORK_SYMBOL
                                    else:
                                        network = network + NETWORK_SYMBOL
                                else:
                                    if network == '':
                                        network = NETWORK_SYMBOL + '-'
                                    else:
                                        network = network + NETWORK_SYMBOL
                                _ += smbl.replace(NETWORK_SYMBOL, '')
                                break
                        if _ == "":
                            if symbol != "":
                                _ += '-'
                            _ += smbl
                        symbol += _
                extra = ""
                if isinstance(changer["extra"], dict):
                    extra = json.dumps(changer["extra"])
                rate_data = {
                    "pair": symbol,
                    "network": network,
                    "changer_id": str(changer["changer"]),
                    "reserve": str(changer["reserve"]),
                    "inmin": str(changer["inmin"]),
                    "inmax": str(changer["inmax"]),
                    "rate": str(changer["rankrate"]),
                    "link": f'https://www.bestchange.ru/click.php?id={changer["changer"]}&from={rate_key.split("-")[0]}&to={rate_key.split("-")[-1]}',
                    "marks": ",".join(changer["marks"]),
                    "extra": extra
                }
                # print(symbol, network, data[rate_key])
                # print(changer)
                rate_key_ = f'rate:{data[rate_key]}:{changer["changer"]}'
                client.hset(rate_key_, mapping=rate_data)
                client.expire(rate_key_, os.getenv('RATES_EXPIRE_TIME'))
    except requests.RequestException as e:
        print(f"Error fetching rates: {e}")
    except:
        traceback.print_exc()
    finally:
        client.close()

def fetch_and_save_rates():
    client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))
    payload = []
    data = {}
    for currency_id in client.keys('currency:*'):
        try:
            currency = {key.decode('utf-8'): value.decode('utf-8') for key, value in client.hgetall(currency_id).items()}
            if 'id' not in currency:
                # print(f"Currency without 'id' key: {currency_id}")
                continue
            for currency2_id in client.keys('currency:*'):
                if currency_id != currency2_id:
                    try:
                        currency2 = {key.decode('utf-8'): value.decode('utf-8') for key, value in client.hgetall(currency2_id).items()}
                        if 'id' not in currency2:
                            # print(f"Currency without 'id' key: {currency2_id}")
                            continue
                        payload.append(f'{currency["id"]}-{currency2["id"]}')
                        data[f'{currency["id"]}-{currency2["id"]}'] = f'{currency["code"]}-{currency2["code"]}'
                        if len(payload) == 500:
                            process_payload(payload, data)
                            payload = []
                    except:
                        print(f"Error processing currency2: {traceback.print_exc()}")
        except:
            print(f"Error processing currency: {traceback.print_exc()}")
    if payload:
        process_payload(payload, data)
    client.close()

def main():
    while True:
        fetch_currencies()
        fetch_and_save_rates()

if __name__ == "__main__":
    main()