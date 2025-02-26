#!/usr/bin/env python3
import os
import glob
import pandas as pd

# 📌 Définition des chemins basés sur la structure du projet
script_dir = os.path.dirname(os.path.realpath(__file__))            # .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/extract_data_scripts
project_root = os.path.abspath(os.path.join(script_dir, ".."))      # .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow

# 📌 Lire le chemin des résultats depuis results_path.txt
with open(os.path.join(project_root, "results_path.txt"), "r") as f:
    data_root = f.read().strip()

# 📌 Définition des dossiers et fichiers
histo_file = os.path.join(data_root, "raw_data", "yahoo_finance", "histo_data", "sp500_histo_close_prices.csv")
realtime_data_dir = os.path.join(data_root, "raw_data", "yahoo_finance", "realtime_data")
output_file = os.path.join(data_root, "formatted_data", "concatenated_files", "sp500_concatenated.csv")

# 📌 Fonction pour charger les données historiques du S&P 500
def load_historical_data(file_path):
    df = pd.read_csv(file_path)
    df = df.rename(columns={'close': 'price'})
    df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d')  # Format YYYY-MM-DD
    return df

# 📌 Fonction pour charger les fichiers temps réel du S&P 500
def load_realtime_data(file_path):
    df = pd.read_csv(file_path)
    df = df.rename(columns={'timestamp': 'date'})
    df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y-%m-%d')  # Format YYYY-MM-DD
    return df

# Vérifier l'existence du fichier historique
if not os.path.exists(histo_file):
    print(f"❌ Erreur : Fichier historique non trouvé : {histo_file}")
    exit(1)
sp500_histo = load_historical_data(histo_file)

# Obtenir la liste des fichiers realtime triés par date
realtime_files = glob.glob(os.path.join(realtime_data_dir, "*_sp500_realtime_price.csv"))
if not realtime_files:
    print(f"❌ Erreur : Aucun fichier realtime trouvé dans : {realtime_data_dir}")
    exit(1)
realtime_files.sort()  # Tri par ordre alphabétique (qui correspond à l'ordre chronologique)

if os.path.exists(output_file):
    # 📌 Charger les données existantes
    existing_data = pd.read_csv(output_file)
    latest_date = existing_data["date"].max()
    
    # 📌 Filtrer les fichiers plus récents que la dernière date enregistrée
    filtered_files = []
    for f in realtime_files:
        temp_df = load_realtime_data(f)
        if temp_df["date"].max() > latest_date:
            filtered_files.append(f)
    
    realtime_files = filtered_files
    
    if realtime_files:
        # 📌 Charger et concaténer les nouvelles données
        new_data = pd.concat([load_realtime_data(f) for f in realtime_files])
        sp500_final = pd.concat([existing_data, new_data])
    else:
        print("ℹ️ Aucune nouvelle donnée à ajouter.")
        exit(0)
else:
    # 📌 Si le fichier de sortie n'existe pas, concaténer toutes les données
    realtime_data = pd.concat([load_realtime_data(f) for f in realtime_files])
    sp500_final = pd.concat([sp500_histo, realtime_data])

# 📌 Supprimer les doublons et trier par timestamp
sp500_final = sp500_final.drop_duplicates(subset=["date"]).sort_values("date")

# 📌 Sauvegarder au format CSV
sp500_final.to_csv(output_file, index=False)
print(f"✅ Données transformées et enregistrées dans {output_file}")
