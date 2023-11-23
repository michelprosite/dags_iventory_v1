import pandas as pd
import re
import glob

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

    # Salva o DataFrame em um arquivo CSV
    df.to_csv(output_file, index=False)

# Chama a função passando o caminho dos arquivos de log e o arquivo de saída desejado
process_logs('/home/michel/Documentos/airflow_docker/dags_iventory_v1/data/*.log', '/home/michel/Documentos/airflow_docker/dags_iventory_v1/testes/novo/df_dags_inventary.csv')
