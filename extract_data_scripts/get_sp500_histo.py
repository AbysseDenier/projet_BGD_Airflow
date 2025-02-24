#!/usr/bin/env python3
import yfinance as yf
import os

ticker = "^GSPC"  # S&P 500
period = "1y"

sp500 = yf.download(ticker, period=period, interval="1d")

if not sp500.empty:

    sp500_close = sp500[['Close']]
    sp500_close.reset_index(inplace=True)
    sp500_close.columns = ['date', 'close']

    script_dir = os.path.dirname(os.path.realpath(__file__))            # .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow/extract_data_scripts
    project_root = os.path.abspath(os.path.join(script_dir, ".."))      # .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow


    # Lire le chemin des résultats depuis le fichier results_path.txt
    with open(os.path.join(project_root, "results_path.txt"), "r") as f:
        data_root = f.read().strip()

    # Définir le dossier des résultats
    # .../data_projet_BGD_airflow/raw_data/yahoo_finance/data_histo
    output_dir = os.path.join(data_root, "raw_data", "yahoo_finance", "data_histo")
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, "sp500_histo_close_prices.csv")
    sp500_close.to_csv(output_file, index=False)
    print(f"✅ Données enregistrées dans {output_file}")

else:
    print("Aucune donnée récupérée.")
