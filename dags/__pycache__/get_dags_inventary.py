from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

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
caminho_saida_logs = '/opt/airflow/data' # Caminho de destido dos logs coletados (DW, DL, Folder, Bucket, etc)
concatenar_logs(caminho_raiz_logs, caminho_saida_logs)

# Definição dos argumentos da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 23),
    'retries': 1
}

# Definição da DAG
dag = DAG('extrair_logs_airflow', default_args=default_args, schedule_interval=None)

# Definição das tarefas na DAG
extrair_logs_dag_con = PythonOperator(
    task_id='extrair_logs_dag_conexao',
    python_callable=concatenar_logs,  # Corrigindo a chamada da função
    op_args=[caminho_raiz_logs, caminho_saida_logs],  # Passando os argumentos para a função
    dag=dag
)

# Definição da ordem de execução das tarefas na DAG
extrair_logs_dag_con
