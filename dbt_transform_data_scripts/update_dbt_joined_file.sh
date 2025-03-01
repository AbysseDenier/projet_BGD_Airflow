#!/bin/bash

export PATH="/Library/PostgreSQL/17/bin:$PATH"

echo "Beginning of the process"

# retrieve path as argument
PROJECT_ROOT="$1"
DATA_ROOT=$(cat "$PROJECT_ROOT/results_path.txt")

# CSV path
SRC_BTC="$DATA_ROOT/formatted_data/concatenated_files/btc_concatenated.csv"
SRC_SP500="$DATA_ROOT/formatted_data/concatenated_files/sp500_concatenated.csv"

DEST_DIR="/private/tmp"
DEST_BTC="$DEST_DIR/btc_concatenated.csv"
DEST_SP500="$DEST_DIR/sp500_concatenated.csv"

EXPORT_DIR="$DATA_ROOT/formatted_data/joined_file"
EXPORT_FILE="$EXPORT_DIR/joined_sp500_btc.csv"

DB_NAME="bgd_project"
DB_USER="etienneeskinazi"

# Copy csv file to /private/tmp/
echo "📂 Copie des fichiers CSV..."
cp -f "$SRC_BTC" "$DEST_BTC"
cp -f "$SRC_SP500" "$DEST_SP500"
echo "Files copied in $DEST_DIR"

# Load csv files in PostgreSQL
echo "Load data in PostgreSQL..."
psql -U $DB_USER -d $DB_NAME <<EOF
TRUNCATE public.btc_concatenated;
COPY public.btc_concatenated(date, price, market_cap, volume) FROM '$DEST_BTC' DELIMITER ',' CSV HEADER;

TRUNCATE public.sp500_concatenated;
COPY public.sp500_concatenated(date, price) FROM '$DEST_SP500' DELIMITER ',' CSV HEADER;
EOF
if [ $? -eq 0 ]; then
    echo "Data loaded in PostgreSQL"
else
    echo "Error with loadind data in PostgreSQL"
    exit 1
fi

# Launch DBT and the marts
echo "Launch DBT.."
cd /Users/etienneeskinazi/Documents/MS_BGD/P2/BigData/ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/dbt_project/dbt_postgres_project
dbt run --full-refresh
if [ $? -eq 0 ]; then
    echo "DBT launched !"
else
    echo "Error with DBT launching"
    exit 1
fi

# Check if `join_sp500_btc` created
echo "Check if join_sp500_btc created..."
psql -U $DB_USER -d $DB_NAME -c "SELECT COUNT(*) FROM public.join_sp500_btc;"
if [ $? -eq 0 ]; then
    echo " join_sp500_btc updated"
else
    echo "Error with updating join_sp500_btc table"
    exit 1
fi

# Export`join_sp500_btc` to csv
echo "Export join_sp500_btc..."
psql -U $DB_USER -d $DB_NAME -c "\COPY public.join_sp500_btc TO '$EXPORT_FILE' WITH CSV HEADER;"
if [ $? -eq 0 ]; then
    echo "Exportation finished : $EXPORT_FILE"
else
    echo "Error with exportation"
    exit 1
fi

echo "End of DBT process !"
