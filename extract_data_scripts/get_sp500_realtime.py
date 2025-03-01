#!/usr/bin/env python3
import yfinance as yf
import os
import datetime

# S&P 500
ticker = "^GSPC"

# get data each minute over 5 last days
sp500 = yf.Ticker(ticker)
latest_data_sp500 = sp500.history(period="5d", interval="1m")  # intraday data

if not latest_data_sp500.empty:
    # get last minute data
    latest_row = latest_data_sp500.iloc[-1]
    latest_timestamp = latest_data_sp500.index[-1]
    latest_price = latest_row['Close']

else:
    # if no last minute data, get last closing price
    latest_close_data = sp500.history(period="5d")  # 5 days histo
    latest_price = latest_close_data["Close"].iloc[-1]  # last close
    latest_timestamp = latest_close_data.index[-1]  # Timestamp

latest_timestamp = latest_timestamp.strftime("%Y-%m-%d")  

# Retrieve paths
script_dir = os.path.dirname(os.path.realpath(__file__))            # .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow/extract_data_scripts
project_root = os.path.abspath(os.path.join(script_dir, ".."))      # .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow
with open(os.path.join(project_root, "results_path.txt"), "r") as f:
    data_root = f.read().strip()

# .../data_projet_BGD_airflow/raw_data/yahoo_finance/realtime_data
output_dir = os.path.join(data_root, "raw_data", "yahoo_finance", "realtime_data")
os.makedirs(output_dir, exist_ok=True)

today_date = datetime.datetime.now().strftime("%Y_%m_%d")

# Name file
output_file = os.path.join(output_dir, f"{today_date}_sp500_realtime_price.csv")

# Save data
with open(output_file, "w") as f:
    f.write("timestamp,price\n")
    f.write(f"{latest_timestamp},{latest_price}\n")

print(f"Data saved in {output_file}")
