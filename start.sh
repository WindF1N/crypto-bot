#!/bin/bash

# Запуск скриптов в фоновом режиме
venv/bin/python parsers/main.py &
venv/bin/python parsers/bybit.py &
venv/bin/python parsers/bybit_ws.py &
venv/bin/python parsers/changers.py &
venv/bin/python parsers/search.py &
venv/bin/python bot.py &

# Ожидание завершения всех фоновых процессов
wait