#!/bin/bash

# بِسْمِ ٱللّٰهِ ٱلرَّحْمٰنِ ٱلرَّحِيْمِ ( Bismillāhi r-Raḥmāni r-Raḥīm )
# ٱلْحَمْدُ لِلّٰهِ رَبِّ ٱلْعَالَمِيْنَ ( Al-Ḥamdu lillāhi Rabbi l-ʿālamīn )
# ٱلرَّحْمٰنِ ٱلرَّحِيْمِ ( Ar-Raḥmāni r-Raḥīm )
# مَالِكِ يَوْمِ ٱلدِّيْنِ ( Māliki yawmi d-dīn )
# إِيَّاكَ نَعْبُدُ وَإِيَّاكَ نَسْتَعِيْنُ ( Iyyāka naʿbudu wa-iyyāka nastaʿīn )
# ٱهْدِنَا ٱلصِّرَاطَ ٱلْمُسْتَقِيْمَ ( Iyyāka naʿbudu wa-iyyāka nastaʿīn )
# صِرَاطَ ٱلَّذِيْنَ أَنْعَمْتَ عَلَيْهِمْ غَيْرِ ٱلْمَغْضُوْبِ عَلَيْهِمْ وَلَا ٱلضَّالِّيْنَ ( Ṣirāṭa l-ladhīna anʿamta ʿalayhim ghayri l-maghḍūbi ʿalayhim wa-lā l-ḍāllīn )

# Создание папки logs
rm -rf logs
mkdir -p logs

# Функция для завершения всех дочерних процессов
cleanup() {
  echo "Завершение всех процессов..."
  pkill -P $$
  exit 0
}

# Устанавливаем обработчик сигналов для завершения
trap cleanup SIGINT SIGTERM

# Запуск скриптов в фоновом режиме с перенаправлением вывода в файлы логов
venv/bin/python parsers/bestchange.py > logs/bestchange.log 2>&1 &
venv/bin/python parsers/bybit.py > logs/bybit.log 2>&1 &
venv/bin/python parsers/bybit_ws.py > logs/bybit_ws.log 2>&1 &
venv/bin/python parsers/changers.py > logs/changers.log 2>&1 &
venv/bin/python parsers/search.py > logs/search.log 2>&1 &
venv/bin/python bot.py > logs/bot.log 2>&1 &
venv/bin/python send_deals.py > logs/send_deals.log 2>&1 &
venv/bin/python delete_sended_deals.py > logs/delete_sended_deals.log 2>&1 &

# Ожидание завершения всех фоновых процессов
wait