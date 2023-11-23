from airflow.models import Variable
from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
import inflection
import requests
import pyodbc
import pymssql
import json
import re
from des import DesKey
import base64
import numpy as np
 
# A conexao com o banco de dados SSMS
def connect_database():
    server = 'CPT-SRVSQLDW01\DWMINERVA'
    database = 'dw_trusted'
    username = ''
    password = ''
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    return cnxn
 
 
# O inicio da DEF, isto é, dos scripts abaixo
def powerbi_dataflow_datasources():
    cnxn = connect_database()
    cursor = cnxn.cursor()
    table_name = 'data_engineer.tab_pbi_logs_admin_dataflows_datasources'
    # TRUNCATE antes de inserir novos registros
    cursor.execute(f"TRUNCATE TABLE {table_name}")
 
 
# A parte que faz a carga no SQL Server a partir do DF Pandas
    if len(new_list) == 0:
        print("oops! This is return")
        continue
    else:  
        df = pd.DataFrame(new_list)
        #df = df.astype(object).where(pd.notnull(df), 'null')
        df = df.rename(columns={col: inflection.underscore(col) for col in df.columns})
        df = df = df.rename(columns={"dataflow_object_id": "dataflow_id", "server": "server_path", "database": "database_path"})
        df = df.replace(['', ' ', None, np.nan], 'null')
        df['row_ingestion_timestamp'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Remover os últimos 3 caracteres (microssegundos)
        columns = ', '.join(df.columns)
        params = ', '.join(['?' for _ in range(len(df.columns))])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({params})"
        values = [tuple(str(item) if str(item) != '<NA>' else None for item in row) for row in df.values]
        cursor.executemany(sql, values)
        cnxn.commit()                
 
 
 
# A estrutura da DAG
 
default_args = {
     'owner': 'dataside',
     'start_date': datetime(2023, 4, 11),
     'retries': 1,
     'retry_delay': timedelta(minutes=5)
}
 
with DAG(
     'log_powerbi_admin_dataflows_datasources',
     default_args=default_args,
     description='Dag para resgatar os eventos.',
     schedule_interval="0 6,12 * * *",
     catchup=False,
     dagrun_timeout=timedelta(minutes=30),
     ) as dag:
     log_powerbi_admin_dataflows_datasources = PythonOperator(
         task_id='powerbi_admin_dataflows_datasources',
         python_callable=powerbi_dataflow_datasources
     )
     log_powerbi_admin_dataflows_datasources