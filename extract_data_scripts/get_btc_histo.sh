#!/bin/bash

# Ce script bash reçoit en argument le chemin absolu vers la racine du projet.
# .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow

PROJECT_ROOT="$1"

# On récupère le chemin vers le dossier des résultats à partir du fichier results_path.txt
# .../data_projet_BGD_airflow
DATA_ROOT=$(cat "$PROJECT_ROOT/results_path.txt")

# On définit le dossier dans lequel seront stockées les données récupérées :
# .../data_projet_BGD_airflow/raw_data/coin_gecko/data_histo
OUTPUT_DIR="$DATA_ROOT/raw_data/coin_gecko/data_histo"

OUTPUT_FILE="btc_histo_data.json"
API_URL="https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=eur&days=365"

mkdir -p "$OUTPUT_DIR"

curl -s "$API_URL" -H "accept: application/json" -o "$OUTPUT_DIR/$OUTPUT_FILE"

if [[ -f "$OUTPUT_DIR/$OUTPUT_FILE" ]]; then
    echo "✅ Données téléchargées et enregistrées dans $OUTPUT_DIR/$OUTPUT_FILE"
else
    echo "❌ Erreur lors du téléchargement des données"
    exit 1
fi
