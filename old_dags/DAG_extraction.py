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

# 🕓 Programmation du DAG : Tous les jours à 16h (Paris Time)
#schedule_time = "15:00"  # UTC+0 (Paris est UTC+1 en hiver, UTC+2 en été)
#schedule_interval = f"0 {schedule_time.split(':')[0]} * * *"

# 📂 Récupération des chemins
dag_folder = os.path.dirname(os.path.realpath(__file__))  
scripts_path = os.path.abspath(os.path.join(dag_folder, "..", "extract_data_scripts"))
project_root = os.path.abspath(os.path.join(dag_folder, ".."))

# 📌 Chemins des scripts Bash
btc_histo_script = os.path.join(scripts_path, "get_btc_histo.sh")
sp500_histo_script = os.path.join(scripts_path, "launch_get_sp500_histo.sh")
btc_realtime_script = os.path.join(scripts_path, "get_btc_realtime.sh")
sp500_realtime_script = os.path.join(scripts_path, "launch_get_sp500_realtime.sh")

# 📌 Définition du DAG
with DAG(
    dag_id="dag1_extraction",
    default_args=default_args,
    schedule_interval=None,             #schedule_interval
    catchup=False,
    max_active_runs=1,
) as dag:

    # 📌 TASK 1 - Récupération des données historiques BTC
    task1_btc_histo = BashOperator(
        task_id="get_btc_histo",
        bash_command=f"bash {btc_histo_script} {project_root}",
    )

    # 📌 TASK 2 - Récupération des données historiques S&P 500
    task2_sp500_histo = BashOperator(
        task_id="get_sp500_histo",
        bash_command=f"bash {sp500_histo_script} {project_root}",
    )

    # 📌 TASK 3 - Récupération des données en temps réel BTC (dépend de Task 1)
    task3_btc_realtime = BashOperator(
        task_id="get_btc_realtime",
        bash_command=f"bash {btc_realtime_script} {project_root}",
    )

    # 📌 TASK 4 - Récupération des données en temps réel S&P 500 (dépend de Task 2)
    task4_sp500_realtime = BashOperator(
        task_id="get_sp500_realtime",
        bash_command=f"bash {sp500_realtime_script} {project_root}",
    )

    # 🔗 Définition du workflow :
    task1_btc_histo >> task3_btc_realtime
    task2_sp500_histo >> task4_sp500_realtime
