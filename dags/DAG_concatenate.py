import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 📌 Configuration des paramètres par défaut
default_args = {
    "owner": "etienneeskinazi",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 📂 Récupération des chemins
dag_folder = os.path.dirname(os.path.realpath(__file__))  
scripts_path = os.path.abspath(os.path.join(dag_folder, "..", "concatenate_data_scripts"))
project_root = os.path.abspath(os.path.join(dag_folder, ".."))

# 📌 Chemins des scripts Bash
btc_transform_script = os.path.join(scripts_path, "launch_concatenate_btc_data.sh")
sp500_transform_script = os.path.join(scripts_path, "launch_concatenate_sp500_data.sh")

# 📌 Définition du DAG
with DAG(
    dag_id="dag2_concatenate",
    default_args=default_args,
    schedule_interval=None,  # Changer si besoin pour une exécution automatique
    catchup=False,
    max_active_runs=1,
) as dag:

    # 📌 TASK 1 - Concaténation des données BTC
    transform_btc_task = BashOperator(
        task_id="concatenate_btc_data",
        bash_command=f"bash {btc_transform_script} {project_root}",
    )

    # 📌 TASK 2 - Concaténation des données S&P 500
    transform_sp500_task = BashOperator(
        task_id="concatenate_sp500_data",
        bash_command=f"bash {sp500_transform_script} {project_root}",
    )
