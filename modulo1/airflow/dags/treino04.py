from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import zipfile

# Constantes
DATA_PATH = '/opt/airflow/csv/microdados_enade_2019/2019/3.DADOS/'
ARQUIVO = DATA_PATH + 'microdados_enade_2019.txt'

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
  "treino-04",
  description="Paralelismos",
  default_args=default_args,
  schedule_interval='*/10 * * * *'
)

start_preprocessiog = BashOperator(
  task_id='start_processing',
  bash_command='echo "Start Preprocessing! Vai!',
  dag=dag
)

get_data = BashOperator(
  task_id='get-data',
  bash_command='curl https://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o /opt/airflow/csv/microdados_enade_2019.zip',
  dag=dag
)

def unzip_file():
  with zipfile.ZipFile('/opt/airflow/csv/microdados_enade_2019.zip') as zipped:
    zipped.extractall('/opt/airflow/csv')

unzip_data= PythonOperator(
  task_id='unzip_data',
  python_callable=unzip_file,
  dag=dag
)

def aplica_filtros():
  cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE', 'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
  enade = read_csv(ARQUIVO, sep=';', decimal=',', usecols=cols)
  enade = enade.loc[
    (enade.NU_IDADE > 20) &
    (enade.NU_IDADE < 40) &
    (enade.NT_GER > 0) 
  ]
  enade.to_csv(DATA_PATH + 'enade_filtrado.csv', index=False)

task_aplica_filtro = PythonOperator(
  task_id='aplica_filtro',
  python_callable=aplica_filtros,
  dag=dag
)