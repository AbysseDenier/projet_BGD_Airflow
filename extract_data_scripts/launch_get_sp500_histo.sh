#!/bin/bash

# .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow

PROJECT_ROOT="$1"

#  .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/extract_data_scripts
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

python3 "$SCRIPT_DIR/get_sp500_histo.py" "$PROJECT_ROOT"
