#!/bin/bash

# Ce script reçoit en argument le chemin absolu du projet, fourni par Airflow.

PROJECT_ROOT="$1"

# Définition du répertoire des scripts Python
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Exécution du script Python de transformation BTC
python3 "$SCRIPT_DIR/concatenate_btc_data.py" "$PROJECT_ROOT"
