# Projet Big Data Airflow - Etienne Eskinazi et Ramzi Naji

Ce dépôt contient le projet de récupération de données financières et de la livraison de KPIs à partir de ces données via Airflow.


## Installation et démarrage du projet

### 1. Cloner le dépôt
```bash
git clone [url_de_ton_depot_git]
cd ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow

### 2. Créer un environnement virtuel Python
python -m venv airflow-venv
source airflow-venv/bin/activate

### 3. Installer les librairies nécessaires
pip install -r requirements.txt

### 4. Modifier results_path.txt
results_path.txt doit pointer vers le dossier où stocker les résultats

### 5. Configurer Airflow
Configurer Airflow en modifiant la variable dags_folder dans airflow.cfg de sorte à pointer vers le dossier contenant les dags à exécuter
dags_folder = .../ESKINAZI_Etienne_NAJI_Ramzi_projet_BGD_airflow/dags

Initialiser Airflow (si première fois): airflow db init

### 6. Lancer Airflow Standalone
airflow scheduler
airflow webserver (dans un autre terminal)
Accéder à Airflow via http://localhost:8080 (si 8080 est défini comme Port du server)

### 7. Exécution des dags
dag1_btc_histo pour récupérer les données crypto (Bitcoin).
dag2_sp500_histo pour récupérer les données de l'indice S&P 500.

