import os
import pandas as pd

# Retrieving paths
script_dir = os.path.dirname(os.path.realpath(__file__)) 
project_root = os.path.abspath(os.path.join(script_dir, ".."))
with open(os.path.join(project_root, "results_path.txt"), "r") as f:
    data_root = f.read().strip()
input_file = os.path.join(data_root, "usage_data", "cleaned_sp500_btc_usage_data.csv")
output_file = os.path.join(data_root, "usage_data", "cleaned_sp500_btc_usage_data_with_features.csv")

# Retrieve data
if not os.path.exists(input_file):
    print(f"Error : File '{input_file}' not found.")
    exit(1)
df = pd.read_csv(input_file)
if df.empty:
    print("csv file empty.")
    exit(1)

# Clean data
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(by='date')

# Calculate returns
df['btc_return'] = df['btc_price'].pct_change()
df['sp500_return'] = df['sp500_price'].pct_change()

# Calculate rolling correl on 30 days
window_size = 30
df['rolling_corr_btc_sp500_return'] = df['btc_return'].rolling(window=window_size, min_periods=15).corr(df['sp500_return'])

# Calculate base 100 prices
df['btc_base_100'] = 100 * (1 + df['btc_return']).cumprod()
df['sp500_base_100'] = 100 * (1 + df['sp500_return']).cumprod()
df['btc_base_100'].iloc[0] = 100
df['sp500_base_100'].iloc[0] = 100

# Calculate acceleration
df['btc_acceleration_abs'] = df['btc_return'].diff()
df['sp500_acceleration_abs'] = df['sp500_return'].diff()

# Export
df.to_csv(output_file, index=False)
print(f"Data saved in {output_file}")

