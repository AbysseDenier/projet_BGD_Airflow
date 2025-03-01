#!/bin/bash

# Get path as argument
# .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow

PROJECT_ROOT="$1"

#  .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/clean_data_scripts
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

python3 "$SCRIPT_DIR/create_features.py" "$PROJECT_ROOT"