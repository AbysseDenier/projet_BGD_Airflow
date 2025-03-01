import os
import pandas as pd
from elasticsearch import Elasticsearch

# 📌 Récupérer le chemin du script et la racine du projet
script_dir = os.path.dirname(os.path.realpath(__file__))  # Dossier contenant ce script
project_root = os.path.abspath(os.path.join(script_dir, ".."))  # Racine du projet

# 📌 Lire le chemin des résultats depuis `results_path.txt`
with open(os.path.join(project_root, "results_path.txt"), "r") as f:
    data_root = f.read().strip()

# 📌 Définir le chemin du fichier CSV
input_file = os.path.join(data_root, "usage_data", "cleaned_sp500_btc_usage_data_with_features.csv")

# 📌 Vérifier si le fichier CSV existe
if not os.path.exists(input_file):
    print(f"❌ Erreur : Le fichier '{input_file}' est introuvable.")
    exit(1)

# 📌 Connexion à Elasticsearch Cloud
CLOUD_ID = "https://45834191df1946ce864b32df95d67d1b.us-central1.gcp.cloud.es.io:443"
USERNAME = "elastic"
PASSWORD = "txqnun13a5hj6k59IVkk06iE" 

try:
    es = Elasticsearch(CLOUD_ID, basic_auth=(USERNAME, PASSWORD))
    print("✅ Connexion à Elasticsearch réussie !")
except Exception as e:
    print(f"❌ Erreur de connexion à Elasticsearch : {e}")
    exit(1)

# 📌 Charger les données du fichier CSV
df = pd.read_csv(input_file)

# 📌 Vérifier si le fichier est vide
if df.empty:
    print("Le fichier CSV est vide. Aucune donnée à indexer.")
    exit(1)

# 📌 Remplacement des NaN avant l'indexation
df = df.fillna(0)

# 📌 Définir le nom de l'index
index_name = "sp500_btc_data"

# 📌 Supprimer l'index s'il existe déjà (optionnel)
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

# 📌 Créer un nouvel index
es.indices.create(index=index_name)
print(f"✅ Index '{index_name}' créé avec succès.")

# 📌 Indexer les données ligne par ligne
for i, row in df.iterrows():
    doc = {
        "date": row["date"],
        "sp500_price": row["sp500_price"],
        "btc_price": row["btc_price"],
        "btc_market_cap": row["btc_market_cap"],
        "btc_volume": row["btc_volume"],
        "rolling_corr_btc_sp500": row["rolling_corr_btc_sp500"],
        "btc_return": row["btc_return"],
        "sp500_return": row["sp500_return"],
        "btc_acceleration_abs": row["btc_acceleration_abs"],
        "sp500_acceleration_abs": row["sp500_acceleration_abs"],
        "btc_acceleration_rel": row["btc_acceleration_rel"],
        "sp500_acceleration_rel": row["sp500_acceleration_rel"]
    }
    es.index(index=index_name, id=i, document=doc)

print("✅ Données indexées avec succès dans Elasticsearch Cloud !")
