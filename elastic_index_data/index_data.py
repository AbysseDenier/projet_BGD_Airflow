import os
import pandas as pd
from elasticsearch import Elasticsearch

# Retrieve path
script_dir = os.path.dirname(os.path.realpath(__file__))  
project_root = os.path.abspath(os.path.join(script_dir, ".."))  
with open(os.path.join(project_root, "results_path.txt"), "r") as f:
    data_root = f.read().strip()

input_file = os.path.join(data_root, "usage_data", "cleaned_sp500_btc_usage_data_with_features.csv")
if not os.path.exists(input_file):
    print(f"Error : '{input_file}' not found.")
    exit(1)

# Connection to Elasticsearch Cloud
CLOUD_ID = "https://45834191df1946ce864b32df95d67d1b.us-central1.gcp.cloud.es.io:443"
USERNAME = "elastic"
PASSWORD = "txqnun13a5hj6k59IVkk06iE" 

try:
    es = Elasticsearch(CLOUD_ID, basic_auth=(USERNAME, PASSWORD))
    print("Elasticsearch connection succeeded !")
except Exception as e:
    print(f"Error with connecting to Elasticsearch : {e}")
    exit(1)

# Load data
df = pd.read_csv(input_file)
if df.empty:
    print("Le fichier CSV est vide. Aucune donnée à indexer.")
    exit(1)

df = df.fillna(0)

# Create ES index
index_name = "sp500_btc_data"
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

es.indices.create(index=index_name)
print(f"Index '{index_name}' created.")

# Index data
for i, row in df.iterrows():
    doc = {
        "date": row["date"],
        "sp500_price": row["sp500_price"],
        "btc_price": row["btc_price"],
        "btc_market_cap": row["btc_market_cap"],
        "btc_volume": row["btc_volume"],
        "rolling_corr_btc_sp500_return": row["rolling_corr_btc_sp500_return"],
        "btc_return": row["btc_return"],
        "sp500_return": row["sp500_return"],
        "sp500_base_100": row["sp500_base_100"],
        "btc_base_100": row["btc_base_100"],
        "btc_acceleration_abs": row["btc_acceleration_abs"],
        "sp500_acceleration_abs": row["sp500_acceleration_abs"],
    }
    es.index(index=index_name, id=i, document=doc)

print("Data indexed in Elasticsearch Cloud !")