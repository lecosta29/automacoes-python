import paramiko
import os
import pickle
import time
import logging
from datetime import datetime, timedelta
from telegram import Bot
import requests

# Configurações de conexão
host = ""
port = 22
username = ""
password = ""
remote_path = '/tim_files/Recebidos'
local_dict_file = "sftp_files.pkl"  # Arquivo para armazenar o dicionário local

# Configuração do logger
logging.basicConfig(filename='sftp_error.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s: %(message)s')

# Função para registrar erros no arquivo de log
def log_error(error_msg):
    logging.error(error_msg)

# Função para notificar novos arquivos .gz
def notify_new_files(new_files):
    if new_files:
        print("Novos arquivos .gz detectados:")
        for file in new_files:
            print(file)
    else:
        print("Nenhum novo arquivo .gz detectado.")

# Diretório de backup
local_backup_dir = r'\\10.10.220.4\Backup\TIM'

# Variável para controlar se é a primeira verificação ou não
primeira_verificacao = True

# Inicializa o dicionário local
local_file_dict = {}

# Função para enviar mensagem para o canal do Telegram
def enviar_mensagem_telegram(mensagem):
    token = ''  # Substitua pelo token do seu bot
    chat_id = ''  # Substitua pelo chat_id do seu canal

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {
        'chat_id': chat_id,
        'text': mensagem
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        print("Mensagem enviada para o canal do Telegram com sucesso!")
    else:
        print("Erro ao enviar mensagem para o canal do Telegram")

# Função para criar pastas com base na data
def create_date_folder(base_dir, file_date):
    folder_name = file_date.strftime("%Y-%m-%d")
    folder_path = os.path.join(base_dir, folder_name)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    return folder_path

# Loop de monitoramento contínuo
while True:
    try:
        # Cria uma conexão SFTP
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Lista os arquivos no caminho remoto
        remote_files = sftp.listdir(remote_path)

        if primeira_verificacao:
            # Atualiza o dicionário local com os nomes dos arquivos existentes
            for file in remote_files:
                local_file_dict[file] = True

            # Salva o dicionário inicial
            with open(local_dict_file, 'wb') as f:
                pickle.dump(local_file_dict, f)

            print("Dicionário inicial criado. Aguardando próxima verificação.")
            time.sleep(600)  # Aguarda 10 minutos antes da próxima verificação
            primeira_verificacao = False
            continue

        # Filtra os arquivos novos .gz
        new_gz_files = []
        for file in remote_files:
            if file.endswith('.gz') and file not in local_file_dict:
                new_gz_files.append(file)
                local_file_dict[file] = True

        # Salva o dicionário atualizado
        with open(local_dict_file, 'wb') as f:
            pickle.dump(local_file_dict, f)

        # Notifica sobre os novos arquivos .gz
        notify_new_files(new_gz_files)

        # Faz o download dos novos arquivos para pastas baseadas nas datas
        moved_files = []  # Lista para armazenar os nomes dos arquivos movidos

        # Atualiza o título e cabeçalho fora do loop
        titulo = "RPA TIM - ALERTA, " + str(datetime.now())
        cabecalho = "Os seguintes arquivos foram movidos:\n\n"

        # Faz o download dos novos arquivos para pastas baseadas nas datas
        for new_file in new_gz_files:
            remote_file_path = remote_path + '/' + new_file
            local_file_path = os.path.join(local_backup_dir, new_file)

            # Obtém a data de modificação do arquivo no servidor
            remote_file_mtime = sftp.stat(remote_file_path).st_mtime
            file_modification_date = datetime.fromtimestamp(remote_file_mtime).date()

            if file_modification_date == datetime.now().date():
                # Arquivo com data de modificação de hoje
                date_folder = create_date_folder(local_backup_dir, file_modification_date)
                local_file_path = os.path.join(date_folder, new_file)
            elif file_modification_date == (datetime.now() - timedelta(days=1)).date():
                # Arquivo com data de modificação de ontem
                yesterday_folder = create_date_folder(local_backup_dir, file_modification_date)
                local_file_path = os.path.join(yesterday_folder, new_file)
            else:
                # Arquivo com data diferente
                custom_date_folder = create_date_folder(local_backup_dir, file_modification_date)
                local_file_path = os.path.join(custom_date_folder, new_file)

            sftp.get(remote_file_path, local_file_path)
            moved_files.append(local_file_path)  # Adiciona o nome do arquivo à lista de arquivos movidos

            # Acrescenta o nome do arquivo ao corpo da mensagem
            corpo_mensagem = f"{os.path.basename(local_file_path)} - Diretório: {os.path.dirname(local_file_path)}\n"
            mensagem_telegram = titulo + "\n" + cabecalho + corpo_mensagem
            enviar_mensagem_telegram(mensagem_telegram)

    except Exception as e:
        error_msg = f"Erro na conexão SFTP: {str(e)}"
        log_error(error_msg)
        print(error_msg)

        # Aguardar um tempo antes de tentar novamente (por exemplo, 30 segundos)
        time.sleep(30)
        continue

    # Aguarda por um intervalo antes de verificar novamente (em segundos)
    time.sleep(600)  # 10 minutos

# Fecha a sessão SFTP e a conexão de transporte após o término do loop
sftp.close()
transport.close()
