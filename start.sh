#!/bin/sh
set -e

# Установить зависимости
pip install --no-cache-dir -r requirements.txt

# Запустить наш скрипт
python update_token_map_from_listedon.py
