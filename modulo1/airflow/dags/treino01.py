#Primeira DAG com AirFlow

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

#Argumentos default
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

#Ps: para configurar o "email_on_failure", envio de emails em caso de uma dag falhar, é necessário configurar o SMTP no airflow.cfg e setar email_on_failure para True

#Definir o fluxo da DAG

dag = DAG(
  "treino-01",
  description="Básico de Bash Operators e PythonOperators",
  default_args=default_args,
  schedule_interval=timedelta(minutes=2)
)

# Adicionando tarefas (tasks)

hello_bash = BashOperator(
  task_id="Hello_Bash",
  bash_command='echo "Hello AirFlow from bash"',
  dag=dag
)

def say_hello():
  print("Hello AirFlow from Python")

hello_python = PythonOperator(
  task_id="Hello_Python",
  python_callable=say_hello,
  dag=dag
)

# Construindo encademaneto de tasks

hello_bash >> hello_python