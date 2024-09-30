import redis
import traceback
from dotenv import load_dotenv
import os

# Определение пути к файлу .env
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../.env'))

# Загрузка переменных окружения из файла .env
load_dotenv(dotenv_path=env_path)

def searching_for_profitable_deals():
    client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))
    try:
        for rate_key in client.keys('rate:*'):
            rate_data = {key.decode('utf-8'): value.decode('utf-8') for key, value in client.hgetall(rate_key).items()}
            if "pair" not in rate_data:
                continue
            bybit_rate = client.hget(f'bybit_rate:{rate_data["pair"].replace("-", "")}', 'price')
            reversed_ = False
            if not bybit_rate:
                bybit_rate = client.hget(f'bybit_rate:{"".join(reversed(rate_data["pair"].split("-")))}', 'price')
                reversed_ = True
            if bybit_rate:
                if "USDT" in rate_data["pair"]:
                    if reversed_ == False:
                        bestchange_sell_price = round(1 / float(rate_data['rate']), 10)
                        bybit_price = float(bybit_rate.decode('utf-8'))
                        if bybit_price < bestchange_sell_price:
                            if 5 > bestchange_sell_price / bybit_price * 100 - 100 > 0.3:
                                if 1000 / bybit_price < float(rate_data["inmin"]):
                                    continue
                                changer = {key.decode('utf-8'): value.decode('utf-8') for key, value in client.hgetall(f'changer:{rate_data["changer_id"]}').items()}
                                client.hset(f"profitable_deals:{rate_data['pair'].split('-')[0]}-{rate_data['pair'].split('-')[1]}:{rate_data['link'].split('?')[-1]}", mapping={
                                    "type": 1,
                                    "bybit_price": str(bybit_price),
                                    "bestchange_price": str(bestchange_sell_price),
                                    "changer_name": changer["name"],
                                    "changer_rating": changer["rating"],
                                    "spread": f"{round(bestchange_sell_price / bybit_price * 100 - 100, 2)} %",
                                    "bybit_url": f"https://www.bybit.com/en/trade/spot/{rate_data['pair'].split('-')[0]}/{rate_data['pair'].split('-')[1]}",
                                    "bestchange_url": rate_data['link'] if "https://www.bestchange.ru" in changer["page"] else rate_data['link'].replace("https://www.bestchange.ru", "https://www.bestchange.com"),
                                    "pair": rate_data['pair'],
                                    "network": rate_data['network'],
                                    "marks": rate_data['marks'],
                                    "extra": rate_data['extra'],
                                    "inmin": rate_data['inmin'],
                                    "inmax": rate_data['inmax']
                                })
                                client.expire(f"profitable_deals:{rate_data['pair'].split('-')[0]}-{rate_data['pair'].split('-')[1]}:{rate_data['link'].split('?')[-1]}", os.getenv('PROFITABLE_DEALS_EXPIRE_TIME'))
                                profitable_deal = {key.decode('utf-8'): value.decode('utf-8') for key, value in client.hgetall(f"profitable_deals:{rate_data['pair'].split('-')[0]}-{rate_data['pair'].split('-')[1]}:{rate_data['link'].split('?')[-1]}").items()}
                                # print(f"1. ByBit: {' -> '.join(reversed(profitable_deal['pair'].split('-')))}")
                                # print(f"Цена: {profitable_deal['bybit_price']}")
                                # print(f"Спот: {profitable_deal['bybit_url']}\n")
                                # print(f"""2. Bestchange: {profitable_deal['pair'].split('-')[0]}{f" {profitable_deal['network'].split('-')[0]}" if profitable_deal['network'].split('-')[0] != '' else ''} -> {profitable_deal['pair'].split('-')[1]}{f" {profitable_deal['network'].split('-')[1]}" if profitable_deal['network'].split('-')[1] != '' else ''}""")
                                # print(f"Цена: {profitable_deal['bestchange_price']}")
                                # print(f"Обменник: {profitable_deal['changer_name']}")
                                # print(f"Рейтинг: {profitable_deal['changer_rating']}")
                                # print(f"Ссылка: {profitable_deal['bestchange_url']}")
                                # print(f"""К отдаче: ≈ {round(1000 / float(profitable_deal['bybit_price']), 4)} {profitable_deal['pair'].split('-')[0]}{f" {profitable_deal['network'].split('-')[0]}" if profitable_deal['network'].split('-')[0] != '' else ''}""")
                                # print(f"""К получению: ≈ {round(1000 / float(profitable_deal['bybit_price']) * float(profitable_deal['bestchange_price']), 4)} {profitable_deal['pair'].split('-')[1]}{f" {profitable_deal['network'].split('-')[1]}" if profitable_deal['network'].split('-')[1] != '' else ''}\n""")
                                # print(f"Спред: {profitable_deal['spread']}")
                                # print(f"Итого: {round(1000 / float(profitable_deal['bybit_price']) * float(profitable_deal['bestchange_price']), 4)} {profitable_deal['pair'].split('-')[1]}")
                                # print("-" * 100)
                    else:
                        bestchange_sell_price = round(1 / float(rate_data['rate']), 10)
                        bybit_price = float(bybit_rate.decode('utf-8'))
                        if bybit_price > float(rate_data['rate']):
                            if 5 > 100 - float(rate_data['rate']) / bybit_price * 100 > 0.3:
                                if 1000 < float(rate_data["inmin"]):
                                    continue
                                changer = {key.decode('utf-8'): value.decode('utf-8') for key, value in client.hgetall(f'changer:{rate_data["changer_id"]}').items()}
                                client.hset(f"profitable_deals:{rate_data['pair'].split('-')[0]}-{rate_data['pair'].split('-')[1]}:{rate_data['link'].split('?')[-1]}", mapping={
                                    "type": 2,
                                    "bybit_price": str(bybit_price),
                                    "bestchange_price": str(bestchange_sell_price),
                                    "changer_name": changer["name"],
                                    "changer_rating": changer["rating"],
                                    "spread": f"{round(100 - float(rate_data['rate']) / bybit_price * 100, 2)} %",
                                    "bybit_url": f"https://www.bybit.com/en/trade/spot/{rate_data['pair'].split('-')[1]}/{rate_data['pair'].split('-')[0]}",
                                    "bestchange_url": rate_data['link'] if "https://www.bestchange.ru" in changer["page"] else rate_data['link'].replace("https://www.bestchange.ru", "https://www.bestchange.com"),
                                    "pair": rate_data['pair'],
                                    "network": rate_data['network'],
                                    "marks": rate_data['marks'],
                                    "extra": rate_data['extra'],
                                    "inmin": rate_data['inmin'],
                                    "inmax": rate_data['inmax']
                                })
                                client.expire(f"profitable_deals:{rate_data['pair'].split('-')[0]}-{rate_data['pair'].split('-')[1]}:{rate_data['link'].split('?')[-1]}", os.getenv('PROFITABLE_DEALS_EXPIRE_TIME'))
                                profitable_deal = {key.decode('utf-8'): value.decode('utf-8') for key, value in client.hgetall(f"profitable_deals:{rate_data['pair'].split('-')[0]}-{rate_data['pair'].split('-')[1]}:{rate_data['link'].split('?')[-1]}").items()}
                                # print(f"""1. Bestchange: {profitable_deal['pair'].split('-')[0]}{f" {profitable_deal['network'].split('-')[0]}" if profitable_deal['network'].split('-')[0] != '' else ''} -> {profitable_deal['pair'].split('-')[1]}{f" {profitable_deal['network'].split('-')[1]}" if profitable_deal['network'].split('-')[1] != '' else ''}""")
                                # print(f"Цена: {profitable_deal['bestchange_price']}")
                                # print(f"Обменник: {profitable_deal['changer_name']}")
                                # print(f"Рейтинг: {profitable_deal['changer_rating']}")
                                # print(f"Ссылка: {profitable_deal['bestchange_url']}")
                                # print(f"""К отдаче: ≈ 1000 {profitable_deal['pair'].split('-')[0]}{f" {profitable_deal['network'].split('-')[0]}" if profitable_deal['network'].split('-')[0] != '' else ''}""")
                                # print(f"""К получению: ≈ {round(1000 * float(profitable_deal['bestchange_price']), 4)} {profitable_deal['pair'].split('-')[1]}{f" {profitable_deal['network'].split('-')[1]}" if profitable_deal['network'].split('-')[1] != '' else ''}\n""")
                                # print(f"2. ByBit: {' -> '.join(reversed(profitable_deal['pair'].split('-')))}")
                                # print(f"Цена: {profitable_deal['bybit_price']}")
                                # print(f"Спот: {profitable_deal['bybit_url']}\n")
                                # print(f"Хедж: https://www.bybit.com/trade/usdt/{''.join(reversed(profitable_deal['pair'].split('-')))}\n")
                                # print(f"Спред: {profitable_deal['spread']}")
                                # print(f"Итого: {round(1000 * float(profitable_deal['bybit_price']) * float(profitable_deal['bestchange_price']), 4)} {profitable_deal['pair'].split('-')[0]}")
                                # print("-" * 100)
                else:
                    if reversed_ == True:
                        bestchange_sell_price = round(1 / float(rate_data['rate']), 10)

                        bybit_rate_0 = client.hget(f'bybit_rate:{rate_data["pair"].split("-")[0]}USDT', 'price')
                        bybit_rate_1 = client.hget(f'bybit_rate:{rate_data["pair"].split("-")[1]}USDT', 'price')

                        if bybit_rate_0 == None or bybit_rate_1 == None:
                            continue

                        bybit_price_0 = float(bybit_rate_0.decode('utf-8'))
                        bybit_price_1 = float(bybit_rate_1.decode('utf-8'))

                        if 1 / bybit_price_0 * bestchange_sell_price * bybit_price_1 / 1 > 1:
                            
                            if 5 > 1 / bybit_price_0 * bestchange_sell_price * bybit_price_1 / 1 * 100 - 100 > 0.3:
                                if 1000 / bybit_price_0 < float(rate_data["inmin"]):
                                    continue
                                changer = {key.decode('utf-8'): value.decode('utf-8') for key, value in client.hgetall(f'changer:{rate_data["changer_id"]}').items()}
                                client.hset(f"profitable_deals:{rate_data['pair'].split('-')[0]}-{rate_data['pair'].split('-')[1]}:{rate_data['link'].split('?')[-1]}", mapping={
                                    "type": 3,
                                    "bybit_price_0": str(bybit_price_0),
                                    "bybit_price_1": str(bybit_price_1),
                                    "bestchange_price": str(bestchange_sell_price),
                                    "changer_name": changer["name"],
                                    "changer_rating": changer["rating"],
                                    "spread": f"{round(1 / bybit_price_0 * bestchange_sell_price * bybit_price_1 / 1 * 100 - 100, 2)} %",
                                    "bybit_url_0": f"https://www.bybit.com/en/trade/spot/{rate_data['pair'].split('-')[0]}/USDT",
                                    "bybit_url_1": f"https://www.bybit.com/en/trade/spot/{rate_data['pair'].split('-')[1]}/USDT",
                                    "bestchange_url": rate_data['link'] if "https://www.bestchange.ru" in changer["page"] else rate_data['link'].replace("https://www.bestchange.ru", "https://www.bestchange.com"),
                                    "pair": rate_data['pair'],
                                    "network": rate_data['network'],
                                    "marks": rate_data['marks'],
                                    "extra": rate_data['extra'],
                                    "inmin": rate_data['inmin'],
                                    "inmax": rate_data['inmax']
                                })
                                client.expire(f"profitable_deals:{rate_data['pair'].split('-')[0]}-{rate_data['pair'].split('-')[1]}:{rate_data['link'].split('?')[-1]}", os.getenv('PROFITABLE_DEALS_EXPIRE_TIME'))
                                profitable_deal = {key.decode('utf-8'): value.decode('utf-8') for key, value in client.hgetall(f"profitable_deals:{rate_data['pair'].split('-')[0]}-{rate_data['pair'].split('-')[1]}:{rate_data['link'].split('?')[-1]}").items()}
                                # print(f"1. ByBit: USDT -> {profitable_deal['pair'].split('-')[0]}")
                                # print(f"Цена: {profitable_deal['bybit_price_0']}")
                                # print(f"Спот: {profitable_deal['bybit_url_0']}\n")
                                # print(f"""2. Bestchange: {profitable_deal['pair'].split('-')[0]}{f" {profitable_deal['network'].split('-')[0]}" if profitable_deal['network'].split('-')[0] != '' else ''} -> {profitable_deal['pair'].split('-')[1]}{f" {profitable_deal['network'].split('-')[1]}" if profitable_deal['network'].split('-')[1] != '' else ''}""")
                                # print(f"Цена: {profitable_deal['bestchange_price']}")
                                # print(f"Обменник: {profitable_deal['changer_name']}")
                                # print(f"Рейтинг: {profitable_deal['changer_rating']}")
                                # print(f"Ссылка: {profitable_deal['bestchange_url']}")
                                # print(f"""К отдаче: ≈ {round(1000 / float(profitable_deal['bybit_price_0']), 4)} {profitable_deal['pair'].split('-')[0]}{f" {profitable_deal['network'].split('-')[0]}" if profitable_deal['network'].split('-')[0] != '' else ''}""")
                                # print(f"""К получению: ≈ {round(1000 / float(profitable_deal['bybit_price_0']) * float(profitable_deal['bestchange_price']), 4)} {profitable_deal['pair'].split('-')[1]}{f" {profitable_deal['network'].split('-')[1]}" if profitable_deal['network'].split('-')[1] != '' else ''}\n""")
                                # print(f"3. ByBit: {profitable_deal['pair'].split('-')[1]} -> USDT")
                                # print(f"Цена: {profitable_deal['bybit_price_1']}")
                                # print(f"Спот: {profitable_deal['bybit_url_1']}\n")
                                # print(f"Хедж: https://www.bybit.com/trade/usdt/{profitable_deal['pair'].split('-')[1]}USDT\n")
                                # print(f"Спред: {profitable_deal['spread']}")
                                # print(f"Итого: {round(1000 / float(profitable_deal['bybit_price_0']) * float(profitable_deal['bestchange_price']) * float(profitable_deal['bybit_price_1']), 4)} USDT")
                                # print("-" * 100)
                            
    except:
        print(f"Error printing rates: {traceback.print_exc()}")
    finally:
        client.close()

def main():
    while True:
        searching_for_profitable_deals()

if __name__ == "__main__":
    main()