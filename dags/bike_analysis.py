from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "spark-sncf"
file_path = "/usr/local/spark/resources/data/airflow.cfg"

###############################################
# DAG Definition
###############################################

#now = datetime.now()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 4, 5),
    "email": ["jahd.jabre@hetic.net"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}
dag = DAG(
        dag_id="bike_analysis", 
        description="This DAG runs a simple Pyspark sncf csv",
        default_args=default_args,
        schedule_interval='*/2 * * * *'
    )

script_python_path = "/usr/local/spark/app/notebook_for_dags_bike_analysis.py" #==> Modifier le chemin du scripts
bikeanalysisscript = BashOperator(
    task_id='script_bike_analysis',
    bash_command=f'python {script_python_path}',
    dag=dag)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
start >> bikeanalysisscript >> end