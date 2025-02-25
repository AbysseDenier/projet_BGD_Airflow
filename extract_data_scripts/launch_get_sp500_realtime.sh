#!/bin/bash

# Ce script reçoit en argument le chemin absolu du projet, fourni par Airflow.
# .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow

PROJECT_ROOT="$1"

#  .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/extract_data_scripts
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Exécuter le script Python pour récupérer les données en temps réel
python3 "$SCRIPT_DIR/get_sp500_realtime.py" "$PROJECT_ROOT"
