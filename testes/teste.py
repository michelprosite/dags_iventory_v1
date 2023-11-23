import os
import re
import pandas as pd

# Função para encontrar as ocorrências nos arquivos .log
def encontrar_ocorrencias(caminho):
    ocorrencias = {
        'AIRFLOW_CTX_DAG_OWNER': [],
        'AIRFLOW_CTX_DAG_ID': [],
        'AIRFLOW_CTX_TASK_ID': [],
        'AIRFLOW_CTX_EXECUTION_DATE': [],
        'AIRFLOW_CTX_TRY_NUMBER': [],
        'AIRFLOW_CTX_DAG_RUN_ID': []
    }
    
    # Lista todos os arquivos na pasta "novo"
    for arquivo in os.listdir(caminho):
        if arquivo.endswith(".log"):
            with open(os.path.join(caminho, arquivo), 'r') as file:
                linhas = file.readlines()
                # Procura por padrões nas linhas do arquivo
                for linha in linhas:
                    for chave, valor in ocorrencias.items():
                        padrao = f'{chave}=(.*)'
                        match = re.search(padrao, linha)
                        if match:
                            valor.append(match.group(1))
    
    return ocorrencias

# Caminho para a pasta "novo"
caminho_pasta = '/home/michel/Documentos/airflow_docker/dags_iventory_v1/testes/novo'

# Encontrar as ocorrências nos arquivos .log
ocorrencias = encontrar_ocorrencias(caminho_pasta)

# Criar DataFrame com as ocorrências encontradas
df = pd.DataFrame(ocorrencias, columns=[
    'AIRFLOW_CTX_DAG_OWNER',
    'AIRFLOW_CTX_DAG_ID',
    'AIRFLOW_CTX_TASK_ID',
    'AIRFLOW_CTX_EXECUTION_DATE',
    'AIRFLOW_CTX_TRY_NUMBER',
    'AIRFLOW_CTX_DAG_RUN_ID'
])

# Exibir o DataFrame
#print(df)
df.to_csv('/home/michel/Documentos/airflow_docker/dags_iventory_v1/testes/novo/def.csv')
