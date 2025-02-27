#!/bin/bash

echo "🚀 Début du processus de mise à jour des données"

# Définition des chemins des fichiers CSV
SRC_BTC="/Users/etienneeskinazi/Documents/MS_BGD/P2/BigData/data_projet_BGD_airflow/formatted_data/concatenated_files/btc_concatenated.csv"
SRC_SP500="/Users/etienneeskinazi/Documents/MS_BGD/P2/BigData/data_projet_BGD_airflow/formatted_data/concatenated_files/sp500_concatenated.csv"

DEST_DIR="/private/tmp"
DEST_BTC="$DEST_DIR/btc_concatenated.csv"
DEST_SP500="$DEST_DIR/sp500_concatenated.csv"

EXPORT_DIR="/Users/etienneeskinazi/Documents/MS_BGD/P2/BigData/data_projet_BGD_airflow/formatted_data/joined_file"
EXPORT_FILE="$EXPORT_DIR/joined_sp500_btc.csv"

DB_NAME="bgd_project"
DB_USER="etienneeskinazi"

# Étape 1: Copier les fichiers CSV vers /private/tmp/
echo "📂 Copie des fichiers CSV..."
cp -f "$SRC_BTC" "$DEST_BTC"
cp -f "$SRC_SP500" "$DEST_SP500"
echo "✅ Fichiers copiés dans $DEST_DIR"

# Étape 2: Charger les fichiers CSV dans PostgreSQL
echo "📥 Chargement des données dans PostgreSQL..."
psql -U $DB_USER -d $DB_NAME <<EOF
TRUNCATE public.btc_concatenated;
COPY public.btc_concatenated(date, price, market_cap, volume) FROM '$DEST_BTC' DELIMITER ',' CSV HEADER;

TRUNCATE public.sp500_concatenated;
COPY public.sp500_concatenated(date, price) FROM '$DEST_SP500' DELIMITER ',' CSV HEADER;
EOF
if [ $? -eq 0 ]; then
    echo "✅ Données chargées dans PostgreSQL"
else
    echo "❌ Erreur lors du chargement des données dans PostgreSQL"
    exit 1
fi

# Étape 3: Exécuter DBT pour mettre à jour les marts
echo "🚀 Exécution de DBT..."
dbt run --full-refresh
if [ $? -eq 0 ]; then
    echo "✅ DBT exécuté avec succès"
else
    echo "❌ Erreur lors de l'exécution de DBT"
    exit 1
fi

# Étape 4: Vérifier la mise à jour de la table `join_sp500_btc`
echo "🔍 Vérification de la table join_sp500_btc..."
psql -U $DB_USER -d $DB_NAME -c "SELECT COUNT(*) FROM public.join_sp500_btc;"
if [ $? -eq 0 ]; then
    echo "✅ Table join_sp500_btc mise à jour"
else
    echo "❌ Erreur : la table join_sp500_btc n'a pas été mise à jour"
    exit 1
fi

# Étape 5: Exporter la table `join_sp500_btc` vers un fichier CSV
echo "📤 Exportation de la table join_sp500_btc..."
psql -U $DB_USER -d $DB_NAME -c "\COPY public.join_sp500_btc TO '$EXPORT_FILE' WITH CSV HEADER;"
if [ $? -eq 0 ]; then
    echo "✅ Exportation terminée : $EXPORT_FILE"
else
    echo "❌ Erreur lors de l'exportation"
    exit 1
fi

echo "🎯 Processus terminé avec succès !"
