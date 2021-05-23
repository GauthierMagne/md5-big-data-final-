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


def count_nb_file(local_path, list_word_researched):
    print(f"Local path ==> {local_path}")
    print(f"list_word_researched ==> {list_word_researched}")
    entries = os.listdir(local_path)
    #if len(list_word_researched)>1:
    for word_researched in list_word_researched:
        nb_file = find_word(entries, word_researched)
        if word_researched == ".csv":
            print(f"there is {nb_file} csv")
        else:
            print(f"there is {nb_file} containing {word_researched}")

def find_word(list_word_to_analyse, extension):
    nb_word=0
    if len(list_word_to_analyse) > 0:
        for file_analysed in list_word_to_analyse:
            if extension in file_analysed:
                nb_word+=1
        return nb_word
    else:
        print("Nothing in List ")
        

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
        dag_id="datalake_feed", 
        description="This DAG runs a simple Pyspark sncf csv",
        default_args=default_args, 
        schedule_interval=None
    )

word_to_search=[".csv","citibike","trip"] #==>  File to search
csv_local_path="/usr/local/spark/resources/data/" 
script_python_path = "/usr/local/spark/app/notebook_for_dags_datalake_feed.py" #==> path for the python script

python_task = PythonOperator(
    task_id=f"datalekkefeeed",
    python_callable=count_nb_file,
    op_kwargs={"local_path": csv_local_path, "list_word_researched": word_to_search},
    dag=dag
)
script_feed = BashOperator(
    task_id='testairflow',
    bash_command=f'python {script_python_path}',
    dag=dag)


end = DummyOperator(task_id="end", dag=dag)
python_task >> script_feed >> end