#!/bin/bash

# Запуск скриптов в фоновом режиме
python parsers/main.py &
python parsers/bybit.py &
python parsers/bybit_ws.py &
python parsers/changers.py &
python parsers/search.py &
python bot.py &

# Ожидание завершения всех фоновых процессов
wait