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
dag_folder = os.path.dirname(os.path.realpath(__file__))  # Chemin du DAG
project_root = os.path.abspath(os.path.join(dag_folder, ".."))  # Racine du projet

# 📌 Chemin du script Bash d'indexation Elasticsearch
index_data_script = os.path.join(project_root, "elastic_index_data", "launch_index_data.sh")

# 📌 Définition du DAG
with DAG(
    dag_id="dag_index_elasticsearch",
    default_args=default_args,
    schedule_interval=None,  # Exécution manuelle ou planifiée via l'UI Airflow
    catchup=False,
    max_active_runs=1,
) as dag:

    # 📌 TASK INDEXATION Elasticsearch
    task_index_elasticsearch = BashOperator(
        task_id="index_data_in_elasticsearch",
        bash_command=f"bash {index_data_script} {project_root}",
    )

    # 🔗 Définition du workflow : Lancer l'indexation
    task_index_elasticsearch
