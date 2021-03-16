#Dag schedulada para dados do titanic

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random

default_args = {
  'owner': 'detonih',
  'depends_on_past': False,
  'start_date': datetime(2021, 3, 16, 10, 50),
  'email': ['detonihenrique@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retries_delay': timedelta(minutes=1)
}

dag = DAG(
  "treino-03",
  description="Pega dados do Titanic e calcula idade média para homens ou mulheres",
  default_args=default_args,
  schedule_interval='*/2 * * * *'
)

get_data = BashOperator(
  task_id='get-data',
  bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o /opt/airflow/csv/train.csv',
  dag=dag
)

def sorteia_h_m():
  
  return random.choice(['male', 'female'])

escolhe_h_m = PythonOperator(
  task_id='escolhe-h-m',
  python_callable=sorteia_h_m,
  dag=dag
)

def MouF(**context):
  value = context['task_instance'].xcom_pull(task_ids='escolhe-h-m')
  if value == 'male':
    return 'branch_homem'
  if value == 'female':
    return 'branch_mulher'

male_female = BranchPythonOperator(
  task_id='condicional',
  python_callable=MouF,
  provide_context=True,
  dag=dag
)

def mean_homem():
  df = pd.read_csv('/opt/airflow/csv/train.csv')
  df = df.loc[df.Sex == 'male']
  print(f"Média de idade dos homens no Titanic: {df.Age.mean()}")

branch_homem = PythonOperator(
  task_id='branch_homem',
  python_callable=mean_homem,
  dag=dag
)

def mean_mulher():
  df = pd.read_csv('/opt/airflow/csv/train.csv')
  df = df.loc[df.Sex == 'female']
  print(f"Média de idade das mulheres no Titanic: {df.Age.mean()}")

branch_mulher = PythonOperator(
  task_id='branch_mulher',
  python_callable=mean_mulher,
  dag=dag
)

def print_age(**context):
  value = context['task_instance'].xcom_pull(task_ids='calcula-idade-media')
  print(f"A idade média do Titanic era {value} anos.")

get_data >> escolhe_h_m >> male_female >> [branch_homem, branch_mulher]
