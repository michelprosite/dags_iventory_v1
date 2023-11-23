from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import re
import glob

# Definição dos argumentos da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 23),
    'retries': 1
}

# Fução para extrais e concatenar os dados de acordo com as pastas existentes no log
def concatenar_logs(caminho_logs, caminho_saida):
    for root, dirs, files in os.walk(caminho_logs):
        logs = []
        for file in files:
            if file.endswith(".log"):
                caminho_arquivo = os.path.join(root, file)
                with open(caminho_arquivo, 'r') as f:
                    log = f.read()
                    logs.append(log)
        
        # Concatenar todos os logs encontrados no diretório
        log_concatenado = '\n'.join(logs)
        
        # Verificar se há logs no diretório
        if logs:
            nome_arquivo = os.path.basename(root) + '.log'
            caminho_saida_arquivo = os.path.join(caminho_saida, nome_arquivo)
            with open(caminho_saida_arquivo, 'a') as output_file:
                output_file.write(log_concatenado)

# Paths
caminho_raiz_logs = '/opt/airflow/logs/' # Caminho onde se encontra os logs
caminho_saida_logs = '/opt/airflow/data/raw' # Caminho de destido dos logs coletados (DW, DL, Folder, Bucket, etc)
concatenar_logs(caminho_raiz_logs, caminho_saida_logs)


def process_logs(files_path, output_file):
    # Inicializa uma lista vazia para armazenar os dados processados
    data = []

    # Itera sobre os arquivos na pasta especificada
    for file_name in glob.glob(files_path):
        with open(file_name, 'r') as file:
            lines = file.readlines()
            for line in lines:
                # Utiliza expressões regulares para extrair os dados
                match = re.search(r'\[(.*?)\].*?\{(.*?)\}.*?INFO - (.*)', line)
                if match:
                    occurrence = match.group(1)
                    task = match.group(2)
                    info = match.group(3)

                    # Adiciona os dados limpos à lista
                    data.append([occurrence, task, info])

    # Cria o DataFrame com os dados coletados
    df = pd.DataFrame(data, columns=['ocorrencia', 'task', 'info'])
    df = df.drop_duplicates()

    # Salva o DataFrame em um arquivo CSV
    df.to_csv(output_file, index=False)

# Chama a função passando o caminho dos arquivos de log e o arquivo de saída desejado
process_logs('/opt/airflow/data/raw/*.log', '/opt/airflow/data/trusted/df_dags_inventary.csv')


###*******************************************************************

# Definição da DAG
dag = DAG('extrair_logs_airflow', default_args=default_args, schedule_interval=None)

# Definição das tarefas na DAG
extrair_logs_dag_con = PythonOperator(
    task_id='extrair_logs_dag_conexao',
    python_callable=concatenar_logs,
    op_args=[caminho_raiz_logs, caminho_saida_logs],
    dag=dag
)

process_logs_dag_con = PythonOperator(
    task_id='process_logs_dag_conexao',
    python_callable=process_logs,
    op_args=['/opt/airflow/data/raw/*.log', '/opt/airflow/data/trusted/output.csv'],
    dag=dag
)

# Define a ordem de execução das tarefas na DAG
extrair_logs_dag_con >> process_logs_dag_con
