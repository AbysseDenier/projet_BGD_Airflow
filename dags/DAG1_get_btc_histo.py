import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "etienneeskinazi",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/dags
dag_folder = os.path.dirname(os.path.realpath(__file__))

# .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/extract_data_scripts
scripts_path = os.path.abspath(os.path.join(dag_folder, "..", "extract_data_scripts"))

# .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/extract_data_scripts/get_btc_histo.sh
bash_script_path = os.path.join(scripts_path, "get_btc_histo.sh")

# .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow
project_root = os.path.abspath(os.path.join(dag_folder, ".."))

# Définition du DAG Airflow
with DAG(
    dag_id="dag1_btc_histo",    
    default_args=default_args,  
    schedule_interval=None,     # Exécution uniquement manuelle (pas automatique)
    catchup=False,              # Pas de rattrapage si une exécution est manquée
    max_active_runs=1           # Une seule instance active à la fois pour éviter les conflits
) as dag:

    # Définition de la tâche BashOperator pour télécharger les données historiques BTC
    fetch_btc_histo = BashOperator(
        task_id="get_btc_histo",

        # bash .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/extract_data_scripts/get_btc_histo.sh ./ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow
        bash_command=f"bash {bash_script_path} {project_root}",
    )

    fetch_btc_histo
