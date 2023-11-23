#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 14:03:07 2023

@author: rodrigo
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from pandas.io.json import json_normalize

#DocString:
"""
    roda os jobs runs para pegar os dag_id e os dag_run_id
    
"""

df_dag = pd.read_csv("/home/rodrigo/Documentos/dags_inventary.csv")

# Configurar a URL da API do Airflow e o ID da DAG
api_url = "http://172.25.2.107:8100/api/v1"
#dag_id = "dbcomfri_tab_modo_conservacao"

# Obter a data atual
data_atual = datetime.utcnow()

# Subtrair um dia
data_menos_um_dia = data_atual - timedelta(days=1)

# Formatar a data no formato desejado
execution_date_gte = data_menos_um_dia.strftime('%Y-%m-%dT%H:%M:%SZ')
#execution_date_gte = '2023-11-08T00:00:00Z'
print(execution_date_gte)



# Configurar a URL da API do Airflow e o ID da DAG
api_url = "http://172.25.2.107:8100/api/v1"
#dag_id = "Job_Abate_Biliar"

# Configurar as credenciais
username = "cst_dside_rodrigo"
password = "R"


df_final = pd.DataFrame()
for index, row in df_dag.iterrows():
    dag_id = row['dagId']
    
    response = requests.get(
        f"{api_url}/dags/{dag_id}/dagRuns?execution_date_gte={execution_date_gte}",
        auth=(username, password)
    )
    
    if response.status_code == 200:
        data = response.json()
        # Extraia o dicionário `dag_runs` do dicionário `data`
        dag_runs = data['dag_runs']
    
        # Crie um DataFrame Pandas a partir do dicionário `dag_runs` usando o método `json_normalize()`
        df = pd.json_normalize(dag_runs)
        df_final = df_final.append(df)
        #print(dag_id)
    
        
    else:
        raise ValueError(f"Erro ao fazer a requisição: {response.status_code}")

df_final.loc[:, ['dag_id', 'dag_run_id','data_interval_end','data_interval_start','end_date','last_scheduling_decision','start_date','state']]
df_final.to_csv('/home/rodrigo/Documentos/dag_runs.csv')