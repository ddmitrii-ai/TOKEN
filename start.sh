#!/bin/sh
set -e

# На всякий случай выводим лог
echo "Starting update_token_map_from_listedon.py..."

python update_token_map_from_listedon.py

echo "Done."
