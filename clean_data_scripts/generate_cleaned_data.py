import pandas as pd
import os

# 📌Retrieve path
script_dir = os.path.dirname(os.path.realpath(__file__)) 
project_root = os.path.abspath(os.path.join(script_dir, ".."))  
with open(os.path.join(project_root, "results_path.txt"), "r") as f:
    data_root = f.read().strip()
input_file = os.path.join(data_root, "formatted_data", "joined_file", "joined_sp500_btc.csv")
output_dir = os.path.join(data_root, "usage_data")
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "cleaned_sp500_btc_usage_data.csv")

# Retrieve data
try:
    
    if not os.path.exists(input_file):
        print(f"Error : file '{input_file}' not found.")
        exit(1)
    df = pd.read_csv(input_file, encoding="utf-8")
    if df.empty:
        print("file is empty.")
        exit(1)

    # Clean data
    initial_rows = len(df)
    df_cleaned = df.dropna()
    cleaned_rows = len(df_cleaned)
    removed_rows = initial_rows - cleaned_rows

    if removed_rows == 0:
        print("No deleted lines.")
    else:
        print(f"✅ {removed_rows} deleted rows.")

    # Save
    df_cleaned.to_csv(output_file, index=False, encoding="utf-8")

    print(f"file saved in : {output_file}")

except Exception as e:
    print(f"Error : {e}")
    exit(1)
