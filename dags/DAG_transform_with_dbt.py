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
project_root = os.path.abspath(os.path.join(dag_folder, ".."))

# 📌 Chemin du script Bash de mise à jour
update_script = os.path.join(project_root, "dbt_transform_data_scripts", "update_dbt_joined_file.sh")

# 📌 Définition du DAG
with DAG(
    dag_id="dag_update_dbt_pipeline",
    default_args=default_args,
    schedule_interval=None,  # Exécution manuelle ou planifiée via l’UI Airflow
    catchup=False,
    max_active_runs=1,
) as dag:

    # 📌 Tâche pour exécuter le script Bash
    run_update_script = BashOperator(
        task_id="run_update_dbt",
        bash_command=f"bash {update_script} {project_root}",
    )

    run_update_script
