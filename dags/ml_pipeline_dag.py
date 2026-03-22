from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import subprocess
import sys

# Allow imports from project
sys.path.append('/opt/airflow')

# -----------------------------
# DEFAULT ARGS
# -----------------------------
default_args = {
    "owner": "retail_team",
    "retries": 1,
}

# -----------------------------
# DAG
# -----------------------------
dag = DAG(
    dag_id="retail_platform_pipeline",
    default_args=default_args,
    description="End-to-End Retail Data Platform pipeline",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# -----------------------------
# TASK FUNCTIONS
# -----------------------------
def init_db():
    import psycopg2

    conn = psycopg2.connect(
        host="postgres",
        database="retail_data_platform",
        user="postgres",
        password="salemessid15122003"
    )
    cursor = conn.cursor()

    with open('/opt/airflow/data/init_db.sql', 'r') as f:
        cursor.execute(f.read())

    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Database schema initialized")

def run_data_generator():
    logging.info("Running Smart Data Generator...")
    script_path = "/opt/airflow/data_generator/data_generator.py"
    
    result = subprocess.run(
        ["python3", script_path],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        logging.error(result.stderr)
        raise Exception("Data generator failed")

    logging.info(result.stdout)


def train_sales():
    logging.info("Training sales model...")
    from ml.experiments.sales_forecasting import train_sales_model
    train_sales_model()


def train_fraud():
    logging.info("Training fraud model...")
    from ml.experiments.fraud_detection import train_fraud_model
    train_fraud_model()


def train_kmeans():
    logging.info("Training segmentation model...")
    from ml.experiments.customer_segmentation import train_kmeans
    train_kmeans()


def train_recommender():
    logging.info("Training recommender model...")
    from ml.experiments.recommendation_system import train_recommender
    train_recommender()

# -----------------------------
# TASKS
# -----------------------------
t0 = PythonOperator(
    task_id="init_db",
    python_callable=init_db,
    dag=dag,
)

t1 = PythonOperator(task_id="generate_data", python_callable=run_data_generator, dag=dag)
t2 = PythonOperator(task_id="train_sales", python_callable=train_sales, dag=dag)
t3 = PythonOperator(task_id="train_fraud", python_callable=train_fraud, dag=dag)
t4 = PythonOperator(task_id="train_kmeans", python_callable=train_kmeans, dag=dag)
t5 = PythonOperator(task_id="train_recommender", python_callable=train_recommender, dag=dag)

# -----------------------------
# DEPENDENCIES
# -----------------------------
_= t0 >> t1 >> [t2, t3, t4, t5]
