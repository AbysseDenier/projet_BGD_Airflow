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
extract_scripts_path = os.path.abspath(os.path.join(dag_folder, "..", "extract_data_scripts"))
concat_scripts_path = os.path.abspath(os.path.join(dag_folder, "..", "concatenate_data_scripts"))
dbt_scripts_path = os.path.abspath(os.path.join(dag_folder, "..", "dbt_transform_data_scripts"))
clean_scripts_path = os.path.abspath(os.path.join(dag_folder, "..", "clean_data_scripts"))
index_scripts_path = os.path.abspath(os.path.join(dag_folder, "..", "elastic_index_data"))
project_root = os.path.abspath(os.path.join(dag_folder, ".."))

# 📌 Chemins des scripts d'extraction
btc_histo_script = os.path.join(extract_scripts_path, "get_btc_histo.sh")
sp500_histo_script = os.path.join(extract_scripts_path, "launch_get_sp500_histo.sh")
btc_realtime_script = os.path.join(extract_scripts_path, "get_btc_realtime.sh")
sp500_realtime_script = os.path.join(extract_scripts_path, "launch_get_sp500_realtime.sh")

# 📌 Chemins des scripts de concaténation
btc_transform_script = os.path.join(concat_scripts_path, "launch_concatenate_btc_data.sh")
sp500_transform_script = os.path.join(concat_scripts_path, "launch_concatenate_sp500_data.sh")

# 📌 Chemin du script Bash DBT
update_dbt_script = os.path.join(dbt_scripts_path, "update_dbt_joined_file.sh")

# 📌 Chemin du script de cleaning
clean_data_script = os.path.join(clean_scripts_path, "launch_generate_cleaned_data.sh")

# 📌 Chemin du script d'indexation Elasticsearch
index_data_script = os.path.join(index_scripts_path, "launch_index_data.sh")

# 📌 Définition du DAG
with DAG(
    dag_id="dag_full_pipeline",
    default_args=default_args,
    schedule_interval=None,  # Exécution manuelle ou planifiée via l’UI Airflow
    catchup=False,
    max_active_runs=1,
) as dag:

    # 📌 TASKS EXTRACTION - BTC
    task1_btc_histo = BashOperator(
        task_id="get_btc_histo",
        bash_command=f"bash {btc_histo_script} {project_root}",
    )

    task2_btc_realtime = BashOperator(
        task_id="get_btc_realtime",
        bash_command=f"bash {btc_realtime_script} {project_root}",
    )

    # 📌 TASKS EXTRACTION - S&P 500
    task3_sp500_histo = BashOperator(
        task_id="get_sp500_histo",
        bash_command=f"bash {sp500_histo_script} {project_root}",
    )

    task4_sp500_realtime = BashOperator(
        task_id="get_sp500_realtime",
        bash_command=f"bash {sp500_realtime_script} {project_root}",
    )

    # 📌 TASKS CONCATÉNATION
    task5_concat_btc = BashOperator(
        task_id="concatenate_btc_data",
        bash_command=f"bash {btc_transform_script} {project_root}",
    )

    task6_concat_sp500 = BashOperator(
        task_id="concatenate_sp500_data",
        bash_command=f"bash {sp500_transform_script} {project_root}",
    )

    # 📌 TASK DBT - Jointure des fichiers
    task7_run_dbt = BashOperator(
        task_id="dbt_join_sp500_btc_data",
        bash_command=f"bash {update_dbt_script} {project_root}",
    )

    # 📌 TASK CLEANING
    task8_clean_data = BashOperator(
        task_id="clean_sp500_btc_data",
        bash_command=f"bash {clean_data_script} {project_root}",
    )

    # 📌 TASK INDEXATION Elasticsearch
    task9_index_data = BashOperator(
        task_id="index_data_in_elasticsearch",
        bash_command=f"bash {index_data_script} {project_root}",
    )

    # 🔗 Définition du workflow :
    # Chaîne BTC : historique -> realtime -> concaténation
    task1_btc_histo >> task2_btc_realtime >> task5_concat_btc
    
    # Chaîne S&P 500 : historique -> realtime -> concaténation
    task3_sp500_histo >> task4_sp500_realtime >> task6_concat_sp500

    # Exécution de DBT après concaténation réussie
    [task5_concat_btc, task6_concat_sp500] >> task7_run_dbt

    # Exécution du cleaning après transformation réussie
    task7_run_dbt >> task8_clean_data

    # Exécution de l'indexation Elasticsearch après le nettoyage des données
    task8_clean_data >> task9_index_data
