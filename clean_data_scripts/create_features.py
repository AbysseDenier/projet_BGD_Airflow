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
input_file = os.path.join(data_root, "usage_data", "cleaned_sp500_btc_usage_data.csv")
output_file = os.path.join(data_root, "usage_data", "cleaned_sp500_btc_usage_data_with_features.csv")

# 📌 Vérifier si le fichier CSV existe
if not os.path.exists(input_file):
    print(f"❌ Erreur : Le fichier '{input_file}' est introuvable.")
    exit(1)

# 📌 Charger les données du fichier CSV
df = pd.read_csv(input_file)

# 📌 Vérifier si le fichier est vide
if df.empty:
    print("Le fichier CSV est vide. Aucune donnée à traiter.")
    exit(1)

# 📌 Convertir la colonne date en format datetime
df['date'] = pd.to_datetime(df['date'])

# 📌 Trier par date
df = df.sort_values(by='date')

# 📌 Calculer la corrélation en rolling window
window_size = 30
df['rolling_corr_btc_sp500'] = df['btc_price'].rolling(window=window_size, min_periods=15).corr(df['sp500_price'])

# 📌 Calculer les rendements quotidiens
df['btc_return'] = df['btc_price'].pct_change()
df['sp500_return'] = df['sp500_price'].pct_change()

# 📌 Calculer l'accélération des prix (2ème dérivée)
df['btc_acceleration_abs'] = df['btc_return'].diff()
df['sp500_acceleration_abs'] = df['sp500_return'].diff()

# 📌 Calculer l'accélération relative des prix (2ème dérivée)
df['btc_acceleration_rel'] = df['btc_return'].pct_change()
df['sp500_acceleration_rel'] = df['sp500_return'].pct_change()

# Sauvegarder au format CSV
df.to_csv(output_file, index=False)
print(f"✅ Données transformées et enregistrées dans {output_file}")

