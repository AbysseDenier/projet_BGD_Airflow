#!/usr/bin/env python3
import os
import glob
import pandas as pd

# Retrieve files
script_dir = os.path.dirname(os.path.realpath(__file__))            # .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/extract_data_scripts
project_root = os.path.abspath(os.path.join(script_dir, ".."))      # .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow

with open(os.path.join(project_root, "results_path.txt"), "r") as f:
    data_root = f.read().strip()

histo_file = os.path.join(data_root, "raw_data", "yahoo_finance", "histo_data", "sp500_histo_close_prices.csv")
realtime_data_dir = os.path.join(data_root, "raw_data", "yahoo_finance", "realtime_data")
output_file = os.path.join(data_root, "formatted_data", "concatenated_files", "sp500_concatenated.csv")

###########################################################################
def load_historical_data(file_path):
    df = pd.read_csv(file_path)
    df = df.rename(columns={'close': 'price'})
    df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d') 
    return df

def load_realtime_data(file_path):
    df = pd.read_csv(file_path)
    df = df.rename(columns={'timestamp': 'date'})
    df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d') 
    return df
###########################################################################

if not os.path.exists(histo_file):
    print(f"Error : File not found : {histo_file}")
    exit(1)
sp500_histo = load_historical_data(histo_file)


realtime_files = glob.glob(os.path.join(realtime_data_dir, "*_sp500_realtime_price.csv"))
if not realtime_files:
    print(f"Error : No realtime files found in : {realtime_data_dir}")
    exit(1)
realtime_files.sort() 

# Load data
if os.path.exists(output_file):

    existing_data = pd.read_csv(output_file)
    latest_date = existing_data["date"].max()
    
    # Filter most recent files
    filtered_files = []
    for f in realtime_files:
        temp_df = load_realtime_data(f)
        if temp_df["date"].max() > latest_date:
            filtered_files.append(f)
    
    realtime_files = filtered_files
    
    if realtime_files:
        # Concatenate
        new_data = pd.concat([load_realtime_data(f) for f in realtime_files])
        sp500_final = pd.concat([existing_data, new_data])
    else:
        print("No new data to add.")
        exit(0)
else:
    # if output file does not exist, concatenate all data
    realtime_data = pd.concat([load_realtime_data(f) for f in realtime_files])
    sp500_final = pd.concat([sp500_histo, realtime_data])

sp500_final = sp500_final.drop_duplicates(subset=["date"]).sort_values("date")

# Save
sp500_final.to_csv(output_file, index=False)
print(f"Data saved in {output_file}")
