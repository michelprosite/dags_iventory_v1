#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 10:17:25 2023

@author: rodrigo
"""

import requests
import pandas as pd

'''
DocString:
gera a lista com informações sobre DAGs como nome da DAG, se esta ativa ou inativa, 
caminho da arquivo, entre outras

'''




'''
# Configurar a URL da API do 
api_url = "http://172.25.2.107:8100/api/v1"

# Configurar as credenciais
username = "cst_dside_rodrigo"
password = "ROmin1@2023"

# Exemplo de chamada para listar DAGs
response = requests.get(f"{api_url}/dags", auth=(username, password))

# Verificar se a solicitação foi bem-sucedida (código 200)
if response.status_code == 200:
    # Exibir a resposta em formato JSON
    print(response.json())
    df = pd.read_json(response.text)
else:
    # Se a solicitação falhou, exibir o código de status
    print(f"A solicitação falhou com código de status: {response.status_code}")
    print(response.text)
'''


# Configurar a URL da API do 
api_url = "http://172.25.2.107:8100/api/v1"

# Configurar as credenciais
username = "cst_dside_rodrigo"
password = "ROmin1@2023"

# Inicializar uma lista para armazenar os DataFrames de cada página
dfs = []

# Parâmetros para controle de paginação
page_size = 100  # Número de registros por página
page_number = 1  # Página inicial

while True:
    # Exemplo de chamada para listar DAGs com parâmetros de paginação
    response = requests.get(
        f"{api_url}/dags",
        params={"limit": page_size, "offset": (page_number - 1) * page_size},
        auth=(username, password)
    )

    # Verificar se a solicitação foi bem-sucedida (código 200)
    if response.status_code == 200:
        # Ler os dados JSON da resposta e converter para DataFrame
        
        df = pd.read_json(response.text)

        # Adicionar o DataFrame atual à lista
        dfs.append(df)

        print('go >>>>')

        # Verificar se há mais páginas
        if len(df) < page_size:
            break
            print('>>>>')
        else:
            # Incrementar o número da página
            page_number += 1
            print('>>>> +1')
    else:
        # Se a solicitação falhou, exibir o código de status
        print(f"A solicitação falhou com código de status: {response.status_code}")
        print(response.text)
        break

# Concatenar todos os DataFrames em um único DataFrame
final_df = pd.concat(dfs, ignore_index=True)
#final_df['key1'] = [x['dag_id'] for x in final_df['dags']]
final_df['dagId'] = [x['dag_id'] for x in final_df['dags']]
final_df['nextDagrunStart'] = [x['next_dagrun_data_interval_start'] for x in final_df['dags']]
final_df['isActive'] = [x['is_active'] for x in final_df['dags']]
final_df['isPaused'] = [x['is_paused'] for x in final_df['dags']]

#is_active': True, 'is_paused': False
final_df.to_csv('/home/michel/Documentos/airflow_docker/airflow-docker/doc_test/dags_inventary.csv')



# Exibir o DataFrame final
print(final_df)

