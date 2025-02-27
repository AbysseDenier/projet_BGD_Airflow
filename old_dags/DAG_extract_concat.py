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
project_root = os.path.abspath(os.path.join(dag_folder, ".."))

# 📌 Chemins des scripts d'extraction
btc_histo_script = os.path.join(extract_scripts_path, "get_btc_histo.sh")
sp500_histo_script = os.path.join(extract_scripts_path, "launch_get_sp500_histo.sh")
btc_realtime_script = os.path.join(extract_scripts_path, "get_btc_realtime.sh")
sp500_realtime_script = os.path.join(extract_scripts_path, "launch_get_sp500_realtime.sh")

# 📌 Chemins des scripts de concaténation
btc_transform_script = os.path.join(concat_scripts_path, "launch_concatenate_btc_data.sh")
sp500_transform_script = os.path.join(concat_scripts_path, "launch_concatenate_sp500_data.sh")

# 📌 Définition du DAG
with DAG(
    dag_id="dag_extract_concat_pipeline",
    default_args=default_args,
    schedule_interval=None,
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

    # 🔗 Définition du workflow :
    # Chaîne BTC : historique -> realtime -> concaténation
    task1_btc_histo >> task2_btc_realtime >> task5_concat_btc
    
    # Chaîne S&P 500 : historique -> realtime -> concaténation
    task3_sp500_histo >> task4_sp500_realtime >> task6_concat_sp500