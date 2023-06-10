from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
import pendulum

import os
from os.path import join
import pandas as pd



def rename_folder(name):
    os.chdir(r"/opt/airflow/dags")
    os.rename("semana", name)

def extrai_dados(data_interval_end):
    city = 'Boston'
    key = 'MZRAB623SMDD8NVXVZDCMALPR'

    URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
               f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')
    
    dados = pd.read_csv(URL)

    file_path = f'/opt/airflow/dags/semana={data_interval_end}/'

    dados.to_csv(file_path + 'dados_brutos.csv')
    dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
    dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')

with DAG(
    "dados_climaticos",
    start_date=pendulum.datetime(2023, 5, 29, tz="UTC"),
    schedule_interval='0 0 * * 1', # executar toda segunda feira
) as dag:

    tarefa_1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = '/opt/airflow/dags/test.sh ',
        dag=dag
    )
    tarefa_2 = PythonOperator(
        task_id = 'nomeia_pasta',
        python_callable = rename_folder,
        op_kwargs={'name':'semana={{data_interval_end.strftime("%Y-%m-%d")}}'},
        dag=dag
    )
    tarefa_3 = PythonOperator(
        task_id = 'extrai_dados',
        python_callable = extrai_dados,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    tarefa_1 >> tarefa_2 >> tarefa_3