import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Définition des arguments par défaut pour le DAG
default_args = {
    "owner": "etienneeskinazi",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 📌 Récupération automatique des chemins :
# Chemin du dossier contenant ce DAG --> renvoie : .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/dags
dag_folder = os.path.dirname(os.path.realpath(__file__))

# Chemin vers le dossier des scripts Bash --> renvoie : .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/extract_data_scripts
scripts_path = os.path.abspath(os.path.join(dag_folder, "..", "extract_data_scripts"))

# Chemin complet du script Bash --> renvoie : .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/extract_data_scripts/get_btc_realtime.sh
bash_script_path = os.path.join(scripts_path, "get_btc_realtime.sh")

# Racine du projet --> renvoie : .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow
project_root = os.path.abspath(os.path.join(dag_folder, ".."))

# 📌 Définition du DAG Airflow
with DAG(
    dag_id="dag3_btc_realtime",   # Nom du DAG
    default_args=default_args,  
    schedule_interval=None,     #
    catchup=False,             # Pas de rattrapage si une exécution est manquée
    max_active_runs=1          # Une seule instance active à la fois pour éviter les conflits
) as dag:

    # 📌 Définition de la tâche BashOperator pour récupérer les données BTC en temps réel
    fetch_btc_realtime = BashOperator(
        task_id="get_btc_realtime",  
        bash_command=f"bash {bash_script_path} {project_root}",  # 📌 Exécution du script avec le chemin absolu
    )

    fetch_btc_realtime
