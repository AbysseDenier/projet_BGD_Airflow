#!/usr/bin/env python3
import json
import pandas as pd
from datetime import datetime, timezone
import os
import glob

# 📌 Définition des chemins basés sur la structure du projet
script_dir = os.path.dirname(os.path.realpath(__file__))            # .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/extract_data_scripts
project_root = os.path.abspath(os.path.join(script_dir, ".."))      # .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow

# 📌 Lire le chemin des résultats depuis results_path.txt
with open(os.path.join(project_root, "results_path.txt"), "r") as f:
    data_root = f.read().strip()

# 📌 Définition des dossiers et fichiers
histo_file = os.path.join(data_root, "raw_data", "coin_gecko", "histo_data", "btc_histo_data.json")
realtime_data_dir = os.path.join(data_root, "raw_data", "coin_gecko", "realtime_data")
output_file = os.path.join(data_root, "formatted_data", "concatenated_files", "btc_concatenated.csv")


def convert_timestamp(ts):
    if isinstance(ts, str) and '-' in ts:
        return ts
    return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).strftime('%Y-%m-%d')

def load_historical_data(file_path):
    with open(file_path, "r") as f:
        histo_data = json.load(f)
    
    return pd.DataFrame({
        "date": [convert_timestamp(x[0]) for x in histo_data["prices"]],
        "price": [x[1] for x in histo_data["prices"]],
        "market_cap": [x[1] for x in histo_data["market_caps"]],
        "volume": [x[1] for x in histo_data["total_volumes"]]
    })

def load_realtime_data(file_path):
    with open(file_path, "r") as f:
        realtime_data = json.load(f)
    
    return pd.DataFrame([{
        "date": realtime_data["timestamp"],
        "price": realtime_data["data"]["bitcoin"]["eur"],
        "market_cap": realtime_data["data"]["bitcoin"]["eur_market_cap"],
        "volume": realtime_data["data"]["bitcoin"]["eur_24h_vol"]
    }])

# Vérifier l'existence du fichier historique
if not os.path.exists(histo_file):
    print(f"❌ Erreur : Fichier historique non trouvé : {histo_file}")
    exit(1)
btc_histo = load_historical_data(histo_file)

# Obtenir la liste des fichiers realtime triés par date
realtime_files = glob.glob(os.path.join(realtime_data_dir, "*_btc_realtime_data.json"))
if not realtime_files:
    print(f"❌ Erreur : Aucun fichier realtime trouvé dans : {realtime_data_dir}")
    exit(1)
realtime_files.sort()  # Tri par ordre alphabétique (qui correspond à l'ordre chronologique)

if os.path.exists(output_file):
    # Si le fichier existe, charger les données existantes
    existing_data = pd.read_csv(output_file)
    latest_date = existing_data['date'].max()
    
    # Filtrer les fichiers plus récents que la dernière date
    realtime_files = [f for f in realtime_files 
                     if datetime.strptime(os.path.basename(f).split('_btc_')[0].replace('_', '-'), '%Y-%m-%d').strftime('%Y-%m-%d') > latest_date]
    
    if realtime_files:
        # Charger et concaténer les nouvelles données
        new_data = pd.concat([load_realtime_data(f) for f in realtime_files])
        btc_final = pd.concat([existing_data, new_data])
    else:
        print("ℹ️ Aucune nouvelle donnée à traiter")
        exit(0)
else:
    # Si le fichier n'existe pas, concaténer toutes les données
    realtime_data = pd.concat([load_realtime_data(f) for f in realtime_files])
    btc_final = pd.concat([btc_histo, realtime_data])

# Supprimer les doublons éventuels et trier par timestamp
btc_final = btc_final.drop_duplicates(subset=['date']).sort_values('date')

# Sauvegarder au format CSV
btc_final.to_csv(output_file, index=False)
print(f"✅ Données transformées et enregistrées dans {output_file}")