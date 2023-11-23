import os

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
            with open(caminho_saida_arquivo, 'w') as output_file:
                output_file.write(log_concatenado)

# Exemplo de uso
caminho_raiz_logs = '/home/michel/Documentos/airflow_docker/dags_iventory_v1/logs/'  # Substitua pelo caminho correto
caminho_saida_logs = '/home/michel/Documentos/airflow_docker/dags_iventory_v1/testes/novo/'  # Substitua pelo caminho correto
concatenar_logs(caminho_raiz_logs, caminho_saida_logs)
