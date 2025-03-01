#!/bin/bash

PROJECT_ROOT="$1"
DATA_ROOT=$(cat "$PROJECT_ROOT/results_path.txt")
OUTPUT_DIR="$DATA_ROOT/raw_data/coin_gecko/realtime_data"

TODAY_DATE=$(date +"%Y_%m_%d")
OUTPUT_FILE="${TODAY_DATE}_btc_realtime_data.json"

# CoinGecko API URL for realtime data
API_URL="https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=eur&include_market_cap=true&include_24hr_vol=true"

# retrieve data
mkdir -p "$OUTPUT_DIR"
TIMESTAMP=$(date +"%Y-%m-%d")  
DATA=$(curl -s "$API_URL" -H "accept: application/json")

# Add timestamp to json
echo "{ \"timestamp\": \"$TIMESTAMP\", \"data\": $DATA }" > "$OUTPUT_DIR/$OUTPUT_FILE"

if [[ -f "$OUTPUT_DIR/$OUTPUT_FILE" ]]; then
    echo "Data loaded in $OUTPUT_DIR/$OUTPUT_FILE"
    cat "$OUTPUT_DIR/$OUTPUT_FILE"  
else
    echo "Error with loading realtime data"
    exit 1
fi
