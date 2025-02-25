#!/usr/bin/env python3
import yfinance as yf
import os
import datetime

# Définition du ticker S&P 500
ticker = "^GSPC"

# Récupération des données minute par minute sur les 5 derniers jours
sp500 = yf.Ticker(ticker)
latest_data_sp500 = sp500.history(period="5d", interval="1m")  # Données intrajournalières

if not latest_data_sp500.empty:
    # Récupérer la dernière ligne de données minute disponible
    latest_row = latest_data_sp500.iloc[-1]
    latest_timestamp = latest_data_sp500.index[-1]
    latest_price = latest_row['Close']

else:
    # Si aucune donnée minute, récupérer le dernier prix de clôture avec son vrai timestamp
    latest_close_data = sp500.history(period="5d")  # Historique quotidien des 5 derniers jours
    latest_price = latest_close_data["Close"].iloc[-1]  # Dernière clôture disponible
    latest_timestamp = latest_close_data.index[-1]  # Timestamp exact de cette clôture

# ✅ Assurer que le timestamp est toujours au format YYYY-MM-DD
latest_timestamp = latest_timestamp.strftime("%Y-%m-%d")  # Format propre Y-M-D

# Récupérer le chemin du script et la racine du projet
script_dir = os.path.dirname(os.path.realpath(__file__))            # .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow/extract_data_scripts
project_root = os.path.abspath(os.path.join(script_dir, ".."))      # .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow

# Lire le chemin des résultats depuis le fichier results_path.txt
with open(os.path.join(project_root, "results_path.txt"), "r") as f:
    data_root = f.read().strip()

# Définir le dossier des résultats pour les données en temps réel
# .../data_projet_BGD_airflow/raw_data/yahoo_finance/realtime_data
output_dir = os.path.join(data_root, "raw_data", "yahoo_finance", "realtime_data")
os.makedirs(output_dir, exist_ok=True)

# ✅ Format de date Y-M-D pour le nom du fichier
today_date = datetime.datetime.now().strftime("%Y_%m_%d")

# Nom du fichier avec la date actuelle (toujours Y-M-D)
output_file = os.path.join(output_dir, f"{today_date}_sp500_realtime_price.csv")

# Enregistrement des données en CSV
with open(output_file, "w") as f:
    f.write("timestamp,price\n")
    f.write(f"{latest_timestamp},{latest_price}\n")

print(f"✅ Dernière donnée enregistrée dans {output_file}")
