import pandas as pd
import os

# 📌 Récupérer le chemin du script et la racine du projet
script_dir = os.path.dirname(os.path.realpath(__file__))  # Dossier contenant ce script
project_root = os.path.abspath(os.path.join(script_dir, ".."))  # Racine du projet

# 📌 Lire le chemin des résultats depuis results_path.txt
with open(os.path.join(project_root, "results_path.txt"), "r") as f:
    data_root = f.read().strip()

# 📌 Définir les chemins des fichiers
input_file = os.path.join(data_root, "formatted_data", "joined_file", "joined_sp500_btc.csv")
output_dir = os.path.join(data_root, "usage_data")
os.makedirs(output_dir, exist_ok=True)  # Créer le dossier s'il n'existe pas
output_file = os.path.join(output_dir, "cleaned_sp500_btc_usage_data.csv")

try:
    # Vérifier si le fichier source existe
    if not os.path.exists(input_file):
        print(f"❌ Erreur : Le fichier source '{input_file}' n'existe pas.")
        exit(1)

    # Charger le fichier CSV
    df = pd.read_csv(input_file, encoding="utf-8")

    # Vérifier si le fichier est vide
    if df.empty:
        print("⚠️ Le fichier est vide, rien à nettoyer.")
        exit(1)

    # Nombre de lignes avant nettoyage
    initial_rows = len(df)

    # Supprimer les lignes contenant au moins une valeur vide
    df_cleaned = df.dropna()

    # Nombre de lignes après nettoyage
    cleaned_rows = len(df_cleaned)
    removed_rows = initial_rows - cleaned_rows

    # Vérifier si des lignes ont été supprimées
    if removed_rows == 0:
        print("✅ Aucune ligne à supprimer, le fichier était déjà propre.")
    else:
        print(f"✅ {removed_rows} lignes supprimées.")

    # Enregistrer le fichier nettoyé
    df_cleaned.to_csv(output_file, index=False, encoding="utf-8")

    print(f"📂 Fichier nettoyé enregistré sous : {output_file}")

except Exception as e:
    print(f"❌ Une erreur s'est produite : {e}")
    exit(1)
