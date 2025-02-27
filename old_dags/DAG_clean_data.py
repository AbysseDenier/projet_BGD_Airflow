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
clean_scripts_path = os.path.abspath(os.path.join(dag_folder, "..", "clean_data_scripts"))  # Ajuste le dossier si nécessaire
project_root = os.path.abspath(os.path.join(dag_folder, ".."))

# 📌 Chemin du script Bash de nettoyage
clean_data_script = os.path.join(clean_scripts_path, "launch_generate_cleaned_data.sh")

# 📌 Définition du DAG
with DAG(
    dag_id="dag_clean_data_pipeline",
    default_args=default_args,
    schedule_interval=None,  # Exécution manuelle ou planifiée via Airflow UI
    catchup=False,
    max_active_runs=1,
) as dag:

    # 📌 TASK DE NETTOYAGE DES DONNÉES
    task_clean_data = BashOperator(
        task_id="clean_joined_sp500_btc_data",
        bash_command=f"bash {clean_data_script} {project_root}",
    )

    task_clean_data  # Tâche autonome

