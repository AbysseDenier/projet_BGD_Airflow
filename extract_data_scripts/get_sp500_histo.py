#!/usr/bin/env python3
import yfinance as yf
import os

# SP500 ticker
ticker = "^GSPC"
period = "1y"

# Retrieve paths
script_dir = os.path.dirname(os.path.realpath(__file__))            # .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow/extract_data_scripts
project_root = os.path.abspath(os.path.join(script_dir, ".."))      # .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow
with open(os.path.join(project_root, "results_path.txt"), "r") as f:
    data_root = f.read().strip()

# .../data_projet_BGD_airflow/raw_data/yahoo_finance/histo_data
output_dir = os.path.join(data_root, "raw_data", "yahoo_finance", "histo_data")
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "sp500_histo_close_prices.csv")

# if histo file already exists, we dont download data
if os.path.exists(output_file):
    print(f"File already exists : {output_file}")
    print("No need to retrieve data.")
    exit(0) 

# Download data from yahoo finance
sp500 = yf.download(ticker, period=period, interval="1d")

if not sp500.empty:
    sp500_close = sp500[['Close']]
    sp500_close.reset_index(inplace=True)
    sp500_close.columns = ['date', 'close']

    # Save data
    sp500_close.to_csv(output_file, index=False)
    print(f"Data saved in {output_file}")

else:
    print("Error : No data retrieved.")
