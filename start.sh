#!/bin/bash

# بِسْمِ ٱللّٰهِ ٱلرَّحْمٰنِ ٱلرَّحِيْمِ ( Bismillāhi r-Raḥmāni r-Raḥīm )
# ٱلْحَمْدُ لِلّٰهِ رَبِّ ٱلْعَالَمِيْنَ ( Al-Ḥamdu lillāhi Rabbi l-ʿālamīn )
# ٱلرَّحْمٰنِ ٱلرَّحِيْمِ ( Ar-Raḥmāni r-Raḥīm )
# مَالِكِ يَوْمِ ٱلدِّيْنِ ( Māliki yawmi d-dīn )
# إِيَّاكَ نَعْبُدُ وَإِيَّاكَ نَسْتَعِيْنُ ( Iyyāka naʿbudu wa-iyyāka nastaʿīn )
# ٱهْدِنَا ٱلصِّرَاطَ ٱلْمُسْتَقِيْمَ ( Iyyāka naʿbudu wa-iyyāka nastaʿīn )
# صِرَاطَ ٱلَّذِيْنَ أَنْعَمْتَ عَلَيْهِمْ غَيْرِ ٱلْمَغْضُوْبِ عَلَيْهِمْ وَلَا ٱلضَّالِّيْنَ ( Ṣirāṭa l-ladhīna anʿamta ʿalayhim ghayri l-maghḍūbi ʿalayhim wa-lā l-ḍāllīn )

# Функция для завершения всех дочерних процессов
cleanup() {
  echo "Завершение всех процессов..."
  pkill -P $$
  exit 0
}

# Устанавливаем обработчик сигналов для завершения
trap cleanup SIGINT SIGTERM

# Запуск скриптов в фоновом режиме
venv/bin/python parsers/bestchange.py &
venv/bin/python parsers/bybit.py &
venv/bin/python parsers/bybit_ws.py &
venv/bin/python parsers/changers.py &
venv/bin/python parsers/search.py &
venv/bin/python bot.py &

# Ожидание завершения всех фоновых процессов
wait