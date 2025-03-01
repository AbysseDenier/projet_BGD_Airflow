#!/usr/bin/env python3
import json
import pandas as pd
from datetime import datetime, timezone
import os
import glob

# Retrieve paths
script_dir = os.path.dirname(os.path.realpath(__file__))            # .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/extract_data_scripts
project_root = os.path.abspath(os.path.join(script_dir, ".."))      # .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow
with open(os.path.join(project_root, "results_path.txt"), "r") as f:
    data_root = f.read().strip()
histo_file = os.path.join(data_root, "raw_data", "coin_gecko", "histo_data", "btc_histo_data.json")
realtime_data_dir = os.path.join(data_root, "raw_data", "coin_gecko", "realtime_data")
output_file = os.path.join(data_root, "formatted_data", "concatenated_files", "btc_concatenated.csv")

###############################################################

def convert_timestamp(ts):
    if isinstance(ts, str) and '-' in ts:
        return ts
    return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).strftime('%Y-%m-%d')

def load_historical_data(file_path):
    with open(file_path, "r") as f:
        histo_data = json.load(f)
    
    return pd.DataFrame({
        "date": [convert_timestamp(x[0]) for x in histo_data["prices"]],
        "price": [x[1] for x in histo_data["prices"]],
        "market_cap": [x[1] for x in histo_data["market_caps"]],
        "volume": [x[1] for x in histo_data["total_volumes"]]
    })

def load_realtime_data(file_path):
    with open(file_path, "r") as f:
        realtime_data = json.load(f)
    
    return pd.DataFrame([{
        "date": realtime_data["timestamp"],
        "price": realtime_data["data"]["bitcoin"]["eur"],
        "market_cap": realtime_data["data"]["bitcoin"]["eur_market_cap"],
        "volume": realtime_data["data"]["bitcoin"]["eur_24h_vol"]
    }])

###############################################################

# histo file
if not os.path.exists(histo_file):
    print(f"Error : historical file not found : {histo_file}")
    exit(1)
btc_histo = load_historical_data(histo_file)

# realtime file
realtime_files = glob.glob(os.path.join(realtime_data_dir, "*_btc_realtime_data.json"))
if not realtime_files:
    print(f"Error : realtime files not found : {realtime_data_dir}")
    exit(1)
realtime_files.sort()

# Load and transform data
if os.path.exists(output_file):

    # add most recent data
    existing_data = pd.read_csv(output_file)
    latest_date = existing_data['date'].max()
    
    realtime_files = [f for f in realtime_files 
                     if datetime.strptime(os.path.basename(f).split('_btc_')[0].replace('_', '-'), '%Y-%m-%d').strftime('%Y-%m-%d') > latest_date]
    
    if realtime_files:
        new_data = pd.concat([load_realtime_data(f) for f in realtime_files])
        btc_final = pd.concat([existing_data, new_data])
    else:
        print("No new data")
        exit(0)
else:
    # concatenate
    realtime_data = pd.concat([load_realtime_data(f) for f in realtime_files])
    btc_final = pd.concat([btc_histo, realtime_data])

btc_final = btc_final.drop_duplicates(subset=['date']).sort_values('date')

# Save
btc_final.to_csv(output_file, index=False)
print(f"Data saved in {output_file}")