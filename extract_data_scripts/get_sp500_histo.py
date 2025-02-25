#!/usr/bin/env python3
import yfinance as yf
import os

# Définition du ticker S&P 500
ticker = "^GSPC"
period = "1y"

# Récupérer le chemin du script et la racine du projet
script_dir = os.path.dirname(os.path.realpath(__file__))            # .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow/extract_data_scripts
project_root = os.path.abspath(os.path.join(script_dir, ".."))      # .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow

# Lire le chemin des résultats depuis le fichier results_path.txt
with open(os.path.join(project_root, "results_path.txt"), "r") as f:
    data_root = f.read().strip()

# Définir le dossier des résultats
# .../data_projet_BGD_airflow/raw_data/yahoo_finance/histo_data
output_dir = os.path.join(data_root, "raw_data", "yahoo_finance", "histo_data")
os.makedirs(output_dir, exist_ok=True)

# Chemin du fichier de sortie
output_file = os.path.join(output_dir, "sp500_histo_close_prices.csv")

# ✅ Vérification : si le fichier existe déjà, on quitte sans exécuter la récupération
if os.path.exists(output_file):
    print(f"🚀 Le fichier des données historiques existe déjà : {output_file}")
    print("⏩ Skip : Pas besoin de récupérer les données.")
    exit(0)  # Sortie propre sans exécuter le téléchargement

# 📥 Téléchargement des données depuis Yahoo Finance
sp500 = yf.download(ticker, period=period, interval="1d")

if not sp500.empty:
    sp500_close = sp500[['Close']]
    sp500_close.reset_index(inplace=True)
    sp500_close.columns = ['date', 'close']

    # Enregistrement des données en CSV
    sp500_close.to_csv(output_file, index=False)
    print(f"✅ Données enregistrées dans {output_file}")

else:
    print("❌ Aucune donnée récupérée.")
