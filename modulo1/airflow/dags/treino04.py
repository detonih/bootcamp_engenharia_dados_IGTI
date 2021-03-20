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
  'start_date': datetime(2021, 3, 20, 21, 40),
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
  schedule_interval='*/20 * * * *'
)

start_preprocessiog = BashOperator(
  task_id='start_processing',
  bash_command='echo "Start Preprocessing! Vai!"',
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
  cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE',
   'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
  enade = pd.read_csv(ARQUIVO, sep=';', decimal=',', usecols=cols)
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

# Idade centralizada na média
# Idade centralizada ao quadrado

#Geralmente quando paralelizamos tasks criamos DFs separados de colunas unicas e depois fazemos o join
# ** dois colchetes (ex: idade[['idadecent']]) serve para criar um DF de unica coluna
def constroi_idade_centralizada():
  idade = pd.read_csv(DATA_PATH + 'enade_filtrado.csv', usecols=['NU_IDADE'])
  idade['idade_cent'] = idade.NU_IDADE - idade.NU_IDADE.mean()
  idade[['idade_cent']].to_csv(DATA_PATH + 'idade_cent.csv', index=False)

def constroi_idade_cent_quad():
  idadecent = pd.read_csv(DATA_PATH + "idade_cent.csv")
  idadecent['idade2'] = idadecent.idade_cent ** 2
  idadecent[['idade2']].to_csv(DATA_PATH + 'idade_quadrado.csv', index=False)

task_idade_cent = PythonOperator(
  task_id='constroi_idade_centralizada',
  python_callable=constroi_idade_centralizada,
  dag=dag
)

task_idade_quad = PythonOperator(
  task_id='constroi_idade_ao_quadrado',
  python_callable=constroi_idade_cent_quad,
  dag=dag
)

def constroi_est_civil():
  filtro = pd.read_csv(DATA_PATH + 'enade_filtrado.csv', usecols=['QE_I01'])
  filtro['estado_civil'] = filtro.QE_I01.replace({
    'A': 'Solteiro',
    'B': 'Casado',
    'C': 'Separado',
    'D': 'Viúvo',
    'E': 'Outro'
  })

  filtro[['estado_civil']].to_csv(DATA_PATH + 'estado_civil.csv', index=False)

task_est_civil = PythonOperator(
  task_id='constroi_est_civil',
  python_callable=constroi_est_civil,
  dag=dag
)

def constroi_cor():
  filtro = pd.read_csv(DATA_PATH + 'enade_filtrado.csv', usecols=['QE_I02'])
  filtro['cor'] = filtro.QE_I02.replace({
    "A": "Branca",
    "B": "Preta",
    "C": "Amarela",
    "D": "Parda",
    "E": "Indígena",
    "F": "",
    " ": ""
  })
  filtro[['cor']].to_csv(DATA_PATH + 'cor.csv', index=False)

task_cor = PythonOperator(
  task_id='constroi_cor_da_pele',
  python_callable=constroi_cor,
  dag=dag
)

#Task de JOIN

def join_data():
  filtro = pd.read_csv(DATA_PATH + 'enade_filtrado.csv')
  idade_cent = pd.read_csv(DATA_PATH + 'idade_cent.csv')
  idade_ao_quadrado = pd.read_csv(DATA_PATH + 'idade_quadrado.csv')
  estado_civil = pd.read_csv(DATA_PATH + 'estado_civil.csv')
  cor = pd.read_csv(DATA_PATH + 'cor.csv')

  final = pd.concat([
    filtro, idade_cent, idade_ao_quadrado, estado_civil, cor
  ],
  axis=1
  )
  final.to_csv(DATA_PATH + 'enade_tradado.csv', index=False)
  print(final)

task_join = PythonOperator(
  task_id='join_data',
  python_callable=join_data,
  dag=dag
)

start_preprocessiog >> get_data >> unzip_data >> task_aplica_filtro

task_aplica_filtro >> [task_idade_cent, task_est_civil, task_cor]

# Define que task_idade_quad tem que vir necessariamente apos task_idade_cent
task_idade_quad.set_upstream(task_idade_cent)

task_join.set_upstream([
  task_est_civil, task_cor, task_idade_quad
])