#!/bin/bash

PROJECT_ROOT="$1"

DATA_ROOT=$(cat "$PROJECT_ROOT/results_path.txt")
OUTPUT_DIR="$DATA_ROOT/raw_data/coin_gecko/histo_data"
OUTPUT_FILE="$OUTPUT_DIR/btc_histo_data.json"

# CoinGecko API URL for historical data
API_URL="https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=eur&days=365"

# Check if historical file exists
if [[ -f "$OUTPUT_FILE" ]]; then
    echo "historical file already exists: $OUTPUT_FILE"
    echo "No need to retrieve data."
    exit 0
fi

# if histo file does not exist, we download the data
mkdir -p "$OUTPUT_DIR"
curl -s "$API_URL" -H "accept: application/json" -o "$OUTPUT_FILE"

if [[ -f "$OUTPUT_FILE" ]]; then
    echo "Data loaded in $OUTPUT_FILE"
else
    echo "Error in loading data"
    exit 1
fi
