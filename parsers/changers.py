# بِسْمِ ٱللّٰهِ ٱلرَّحْمٰنِ ٱلرَّحِيْمِ ( Bismillāhi r-Raḥmāni r-Raḥīm )

import requests
import redis
import traceback
from dotenv import load_dotenv
import os
import time

# Определение пути к файлу .env
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../.env'))

# Загрузка переменных окружения из файла .env
load_dotenv(dotenv_path=env_path)

def fetch_changers():
    client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=os.getenv('REDIS_DB'))
    try:
        response = requests.get(f"https://www.bestchange.app/v2/{os.getenv('BESTCHANGE_API_KEY')}/changers/ru")
        response.raise_for_status()
        changers = response.json()["changers"]
        for changer in changers:
            changer_key = f'changer:{changer["id"]}'
            client.hset(changer_key, mapping={
                "name": changer["name"],
                "page": changer["pages"]["ru"] if "ru" in changer["langs"] else changer["pages"]["en"],
                "reserve": changer["reserve"],
                "rating": changer["rating"],
                "reviews_claim": changer["reviews"]["claim"],
                "reviews_closed": changer["reviews"]["closed"],
                "reviews_neutral": changer["reviews"]["neutral"],
                "reviews_positive": changer["reviews"]["positive"],
                "active": str(changer["active"])
            })
            client.expire(changer_key, os.getenv('CHANGERS_EXPIRE_TIME'))
    except requests.RequestException as e:
        print(f"Error fetching currencies: {e}")
    except:
        traceback.print_exc()
    finally:
        client.close()

def main():
    while True:
        fetch_changers()
        time.sleep(60)

if __name__ == "__main__":
    main()