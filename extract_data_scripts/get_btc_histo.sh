#!/bin/bash

# Ce script bash reçoit en argument le chemin absolu vers la racine du projet.
# Exemple : .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow

PROJECT_ROOT="$1"

# On récupère le chemin vers le dossier des résultats à partir du fichier results_path.txt
# Exemple : .../data_projet_BGD_airflow
DATA_ROOT=$(cat "$PROJECT_ROOT/results_path.txt")

# On définit le dossier dans lequel seront stockées les données récupérées :
# Exemple : .../data_projet_BGD_airflow/raw_data/coin_gecko/histo_data
OUTPUT_DIR="$DATA_ROOT/raw_data/coin_gecko/histo_data"

# Chemin du fichier des données historiques
OUTPUT_FILE="$OUTPUT_DIR/btc_histo_data.json"

# URL de l'API CoinGecko pour récupérer les données historiques
API_URL="https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=eur&days=365"

# Vérifier si le fichier des données historiques existe déjà
if [[ -f "$OUTPUT_FILE" ]]; then
    echo "🚀 Le fichier des données historiques existe déjà : $OUTPUT_FILE"
    echo "⏩ Skip : Pas besoin de récupérer les données."
    exit 0  # Sortie normale sans exécuter le téléchargement
fi

# Si le fichier des données historiques n'existe pas, on le télécharge
mkdir -p "$OUTPUT_DIR"

curl -s "$API_URL" -H "accept: application/json" -o "$OUTPUT_FILE"

# Vérification du téléchargement
if [[ -f "$OUTPUT_FILE" ]]; then
    echo "✅ Données téléchargées et enregistrées dans $OUTPUT_FILE"
else
    echo "❌ Erreur lors du téléchargement des données"
    exit 1
fi
