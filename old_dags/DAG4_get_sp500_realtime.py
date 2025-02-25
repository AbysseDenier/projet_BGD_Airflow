import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Arguments par défaut pour le DAG
default_args = {
    "owner": "etienneeskinazi",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Renvoie : .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow/dags
dag_folder = os.path.dirname(os.path.realpath(__file__))

# Renvoie : .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow/extract_data_scripts
scripts_path = os.path.abspath(os.path.join(dag_folder, "..", "extract_data_scripts"))
bash_script_path = os.path.join(scripts_path, "launch_get_sp500_realtime.sh")

# Renvoie : .../ESKINAZI_Etienne_RAMZI_Naji_projet_BGD_airflow
project_root = os.path.abspath(os.path.join(dag_folder, ".."))

# Définition du DAG pour récupérer les données du S&P 500 en temps réel
with DAG(
    dag_id="dag4_sp500_realtime",  # Identifiant unique du DAG
    default_args=default_args,  
    schedule_interval=None,  # Exécuter toutes les 5 minutes
    catchup=False,  
    max_active_runs=1  
) as dag:

    # Tâche Bash pour récupérer les données en temps réel du S&P 500
    fetch_sp500_realtime_task = BashOperator(
        task_id="get_sp500_realtime",
        bash_command=f"bash {bash_script_path} {project_root}",
    )

    fetch_sp500_realtime_task
