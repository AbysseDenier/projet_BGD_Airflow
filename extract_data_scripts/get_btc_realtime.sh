#!/bin/bash

# Ce script Bash reçoit en argument le chemin absolu vers la racine du projet.
PROJECT_ROOT="$1"

# On récupère le chemin vers le dossier des résultats
DATA_ROOT=$(cat "$PROJECT_ROOT/results_path.txt")

# Dossier où seront stockées les données en temps réel
OUTPUT_DIR="$DATA_ROOT/raw_data/coin_gecko/realtime_data"

# Date du jour (format YYYY-MM-DD) pour le nom du fichier
TODAY_DATE=$(date +"%Y_%m_%d")

# Nom du fichier de sortie incluant la date
OUTPUT_FILE="${TODAY_DATE}_btc_realtime_data.json"

# URL de l'API CoinGecko pour récupérer le prix en temps réel
API_URL="https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=eur&include_market_cap=true&include_24hr_vol=true"

# Création du dossier si non existant
mkdir -p "$OUTPUT_DIR"

# Récupération des données et ajout d'un timestamp
TIMESTAMP=$(date +"%Y-%m-%d")  
DATA=$(curl -s "$API_URL" -H "accept: application/json")

# Ajout du timestamp au JSON
echo "{ \"timestamp\": \"$TIMESTAMP\", \"data\": $DATA }" > "$OUTPUT_DIR/$OUTPUT_FILE"

# Vérification et affichage du résultat
if [[ -f "$OUTPUT_DIR/$OUTPUT_FILE" ]]; then
    echo "✅ Données en temps réel enregistrées dans $OUTPUT_DIR/$OUTPUT_FILE"
    cat "$OUTPUT_DIR/$OUTPUT_FILE"  # Affiche le JSON final avec timestamp
else
    echo "❌ Erreur lors du téléchargement des données en temps réel"
    exit 1
fi
