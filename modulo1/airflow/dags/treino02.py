#Dag schedulada para dados do titanic

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
  'owner': 'detonih',
  'depends_on_past': False,
  'start_date': datetime(2021, 3, 14),
  'email': ['detonihenrique@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retries_delay': timedelta(minutes=1)
}

dag = DAG(
  "treino-02",
  description="Extrai dados do Titanic e calcula a idade media dos passageiros",
  default_args=default_args,
  schedule_interval='*/2 * * * *'
)

get_data = BashOperator(
  task_id='get-data',
  bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o /opt/airflow/csv/train.csv',
  dag=dag
)

def calculate_mean_age():
  df = pd.read_csv('/opt/airflow/csv/train.csv')
  med = df.Age.mean()
  return med

def print_age(**context):
  value = context['task_instance'].xcom_pull(task_ids='calcula-idade-media')
  print(f"A idade média do Titanic era {value} anos.")

task_idade_media = PythonOperator(
  task_id='calcula-idade-media',
  python_callable=calculate_mean_age,
  dag=dag
)

task_print_idade = PythonOperator(
  task_id='mostra-idade',
  python_callable=print_age,
  provide_context=True,
  dag=dag
)

get_data >> task_idade_media >> task_print_idade