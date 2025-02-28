import paramiko
import time
import os
import zipfile
import shutil
from telegram import Bot
import requests
import logging
import datetime

# Função para conectar ao servidor SFTP
def connect_to_sftp(host, port, username, password):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, port=port, username=username, password=password)
    return client.open_sftp()

# Função para obter a pasta mais recente no servidor SFTP com base na data de modificação
def get_latest_folder(sftp, remote_path):
    folders = [f for f in sftp.listdir_attr(remote_path) if f.st_mode // 2 ** 12 == 0o4]
    if not folders:
        return None

    current_date = datetime.datetime.now()
    latest_folder = None
    latest_modification_time = None

    for folder in folders:
        folder_name = folder.filename
        mod_time = datetime.datetime.fromtimestamp(folder.st_mtime)

        # Verifique se a pasta foi modificada no mês atual
        if mod_time.year == current_date.year and mod_time.month == current_date.month:
            if latest_modification_time is None or mod_time > latest_modification_time:
                latest_folder = folder_name
                latest_modification_time = mod_time

    return latest_folder

# Função para listar arquivos em uma pasta no servidor SFTP
def list_files(sftp, remote_path):
    return sftp.listdir_attr(remote_path)

# Função para baixar um arquivo do servidor SFTP
def download_file(sftp, remote_path, local_path):
    sftp.get(remote_path, local_path)

# Função para extrair um arquivo ZIP
def extract_file(file_path, extract_path):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

# Função para processar e mover arquivos
def process_and_move_files(local_download_path):
    files_to_delete = []  # Lista para armazenar arquivos que não serão movidos
    for filename in os.listdir(local_download_path):
        if filename.endswith('.txt'):
            file_path = os.path.join(local_download_path, filename)
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
                destination_folder = None

                if "|PC1|" in content:
                    destination_folder = r'\\10.10.220.4\batimento\Enel GO PC1\Pendentes'
                elif "|PC2|" in content:
                    destination_folder = r'\\10.10.220.4\batimento\Enel GO PC2\Pendentes'
                elif "|PC3|" in content:
                    destination_folder = r'\\10.10.220.4\batimento\Enel GO PC3\Pendentes'
                elif "|PC4|" in content:
                    destination_folder = r'\\10.10.220.4\batimento\Enel GO PC4\Pendentes'
                elif "|PC5|" in content:
                    destination_folder = r'\\10.10.220.4\batimento\Enel GO PC5\Pendentes'
                elif "|PC6|" in content:
                    destination_folder = r'\\10.10.220.4\batimento\Enel GO PC6\Pendentes'

                if destination_folder:
                    new_file_path = os.path.join(destination_folder, filename)
                    os.makedirs(destination_folder, exist_ok=True)
                    shutil.copy(file_path, new_file_path)  # Usar shutil.copy para copiar
                    print(f"Copied file {filename} to {destination_folder}")
                    files_to_delete.append(file_path)  # Adicionar arquivo à lista para excluir

                    # Envia uma notificação com informações do arquivo

                    mensagem_telegram = f"Arquivo '{os.path.basename(filename)}' foi movido para o diretório '{destination_folder}'.\n"
                    enviar_mensagem_telegram(mensagem_telegram)

                    # Esperar por 5 segundos
                    time.sleep(5)

                    # Tentar mover o arquivo original após esperar
                    try:
                        shutil.move(file_path, os.path.join(local_download_path, 'temp', filename))
                        print(f"Moved original file {filename} to temporary folder")
                    except Exception as e:
                        print(f"An error occurred while moving original file: {str(e)}")

                    # Excluir o arquivo original após esperar
                    try:
                        os.remove(file_path)
                        print(f"Removed original file {filename}")
                    except Exception as e:
                        print(f"An error occurred while removing original file: {str(e)}")
                else:
                    files_to_delete.append(file_path)  # Adicionar arquivos à lista para excluir

    # Excluir os arquivos que não foram movidos para as respectivas pastas
    for file_path in files_to_delete:
        try:
            os.remove(file_path)
            print(f"Removed non-moved file {file_path}")
        except Exception as e:
            print(f"An error occurred while removing non-moved file: {str(e)}")

    # Excluir os arquivos .zip
    for filename in os.listdir(local_download_path):
        if filename.endswith('.zip'):
            zip_file_path = os.path.join(local_download_path, filename)
            try:
                os.remove(zip_file_path)
                print(f"Removed zip file {zip_file_path}")
            except Exception as e:
                print(f"An error occurred while removing zip file: {str(e)}")

def is_valid_zip(file_path):
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            return zip_ref.testzip() is None
    except zipfile.BadZipFile:
        return False

def monitor_folder(sftp, folder_path, local_download_path, processed_files):
    is_first_check = True  # Flag to differentiate the first check

    while True:
        print("Checking for changes...")
        files = list_files(sftp, folder_path)

        for file_info in files:
            filename = file_info.filename
            mod_time = file_info.st_mtime

            if filename not in processed_files:
                if is_first_check:
                    processed_files.add(filename)
                else:
                    processed_files.add(filename)
                    print(f"New file detected: {filename}")
                    remote_file_path = f"{folder_path}/{filename}"
                    local_file_path = os.path.join(local_download_path, filename)
                    download_file(sftp, remote_file_path, local_file_path)
                    print(f"File downloaded to: {local_file_path}")

                    # Verificar se o arquivo é um arquivo ZIP válido
                    if is_valid_zip(local_file_path):
                        # Diretório onde o arquivo ZIP será extraído
                        extract_folder = os.path.join(local_download_path, "extracted")
                        os.makedirs(extract_folder, exist_ok=True)

                        # Extrair o arquivo baixado
                        extract_file(local_file_path, extract_folder)

                        # Processar e mover os arquivos após a extração
                        process_and_move_files(extract_folder)

                        # Após a extração e movimentação, remover o arquivo ZIP
                        try:
                            os.remove(local_file_path)
                            print(f"Removed zip file {local_file_path}")
                        except Exception as e:
                            print(f"An error occurred while removing zip file: {str(e)}")

        is_first_check = False  # After the first check, set the flag to False
        time.sleep(60)  # Wait for 60 seconds before checking again

# Função para enviar mensagem para o Telegram
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

if __name__ == "__main__":
    host = ''
    port = 2222
    username = ''
    password = ''
    base_folder = r'/enelgo/Remessa/2024'  # Update this with the base folder path
    local_download_path = r'\\10.10.220.4\batimento\RPA_VALIDACAO\GO'  # Update this with the local download path

    # Configurar o sistema de log
    logging.basicConfig(filename='connection_log.log', level=logging.INFO)

    max_attempts = 20  # Número máximo de tentativas
    attempts = 0

    while attempts < max_attempts:
        try:
            sftp = connect_to_sftp(host, port, username, password)
            logging.info("Connected to SFTP")
            print("Connected to SFTP")

            # Obtenha a pasta mais recente com base na data de modificação
            folder_path = os.path.join(base_folder, get_latest_folder(sftp, base_folder)).replace('\\', '/')
            logging.info(f"Monitoring folder: {folder_path}")
            print(f"Monitoring folder: {folder_path}")

            processed_files = set()
            monitor_folder(sftp, folder_path, local_download_path, processed_files)
            break  # Se a conexão e o monitoramento forem bem-sucedidos, saia do loop
        except Exception as e:
            error_message = f"An error occurred on attempt {attempts + 1}: {str(e)}"
            logging.error(error_message)
            print(error_message)
            attempts += 1
            if attempts < max_attempts:
                retry_message = "Retrying in 60 seconds..."
                logging.info(retry_message)
                print(retry_message)
                time.sleep(60)  # Espera 60 segundos antes de tentar novamente
            else:
                max_attempts_message = "Max attempts reached. Exiting."
                logging.error(max_attempts_message)
                print(max_attempts_message)
                break
        finally:
            if 'sftp' in locals():
                sftp.close()  # Certifique-se de fechar a conexão se estiver aberta antes de sair do loop
