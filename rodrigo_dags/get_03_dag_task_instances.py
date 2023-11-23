#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 10 21:07:06 2023

@author: rodrigo
"""

import requests
from pandas import json_normalize
import pandas as pd

'''
DocStrng:
    Não retorna DAG desativadas
    Retorna data de Start e status da task

'''

# Configurar as credenciais
username = "cst_dside_rodrigo"
password = "R"

# Configurar a URL da API do Airflow e o ID da DAG
api_url = "http://172.25.2.107:8100/api/v1"
dag_id = "dbcomfri_tab_modo_conservacao"
dag_run_id = "scheduled__2023-11-10T01:32:32.352042+00:00"


df_dag = pd.read_csv("/home/rodrigo/Documentos/dag_runs.csv")
#df_dag = df_dag[df_dag['dag_id'] == 'dbcomfri_tab_modo_conservacao']


df_final = pd.DataFrame()

for index, row in df_dag.iterrows():
    dag_id = row['dag_id']
    dag_run_id = row['dag_run_id']

    response = requests.get(
        f"{api_url}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
        auth=(username, password)
    )
    
    if response.status_code == 200:
        data = response.json()
        task_runs = data['task_instances']
        
        df = pd.json_normalize(task_runs)
        df_final = df_final.append(df)

df_final.to_csv('/home/rodrigo/Documentos/dag_task_instance.csv')

    
    
    