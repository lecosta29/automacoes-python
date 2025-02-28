import paramiko
import os
import pickle
import time
import logging
import shutil
from datetime import datetime
from telegram import Bot
import requests

# Configurações de conexão
host = ''
port = 22
username = ''
password = ''
remote_path = '/upload'
local_dict_file = "sftp_logGAB.pkl"  # Arquivo para armazenar o dicionário local

# Configuração do logger
logging.basicConfig(filename='sftp_logGAB.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s: %(message)s')

# Função para registrar erros no arquivo de log
def log_error(error_msg):
    logging.error(error_msg)

# Função para notificar novos arquivos .txt
def notify_new_files(new_files):
    if new_files:
        print("Novos arquivos .TXT detectados:")
        for file in new_files:
            print(file)
    else:
        print("Nenhum novo arquivo .TXT detectado.")

# Diretório de backup
local_backup_dir = r''

# Inicializa o dicionário local para rastrear arquivos
if os.path.exists(local_dict_file):
    with open(local_dict_file, 'rb') as f:
        local_file_dict = pickle.load(f)
else:
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

# Função para determinar e criar a pasta baseada na data de modificação do arquivo
def get_month_dir(base_dir, file_mod_time):
    # Dicionário para mapear os meses aos seus nomes
    month_names = {
        '01': '1. JANEIRO', '02': '2. FEVEREIRO', '03': '3. MARÇO',
        '04': '4. ABRIL', '05': '5. MAIO', '06': '6. JUNHO',
        '07': '7. JULHO', '08': '8. AGOSTO', '09': '9. SETEMBRO',
        '10': '10. OUTUBRO', '11': '11. NOVEMBRO', '12': '12. DEZEMBRO'
    }
    # Obtem o nome do mês a partir do dicionário usando como chave o mês da data de modificação do arquivo
    month_key = file_mod_time.strftime("%m")
    month_name = month_names[month_key]

    # Lista todos os subdiretórios no diretório base
    existing_dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    # Verifica se existe algum diretório cujo nome corresponde ao mês do arquivo
    for dir_name in existing_dirs:
        dir_path = os.path.join(base_dir, dir_name)
        # Obtém a data de modificação do diretório
        dir_mod_time = datetime.fromtimestamp(os.path.getmtime(dir_path))
        # Compara se o mês e o ano da data de modificação do diretório correspondem ao mês e ano do arquivo
        if dir_mod_time.strftime("%m.%Y") == file_mod_time.strftime("%m.%Y"):
            print(f"Usando pasta existente: {dir_path}")
            return dir_path
    # Se não encontrar uma pasta existente, cria uma nova
    new_dir_path = os.path.join(base_dir, month_name)
    os.makedirs(new_dir_path)
    print(f"Pasta criada: {new_dir_path}")
    # Atualiza a data de modificação da pasta para refletir o mês correto
    os.utime(new_dir_path, (file_mod_time.timestamp(), file_mod_time.timestamp()))
    return new_dir_path

def copy_file_based_on_mod_date(local_file_path, base_dir):
    file_mod_time = datetime.fromtimestamp(os.path.getmtime(local_file_path))
    destination_dir = get_month_dir(base_dir, file_mod_time)
    destination_path = os.path.join(destination_dir, os.path.basename(local_file_path))
    shutil.copy(local_file_path, destination_path)
    print(f"Arquivo {os.path.basename(local_file_path)} copiado para {destination_path}")
    # Envia mensagem ao Telegram após mover o arquivo
    mensagem_telegram = f"Nome do Arquivo: {os.path.basename(local_file_path)} \n Movido para o Diretório do Drive:  {destination_dir}"
    enviar_mensagem_telegram(mensagem_telegram)

def move_file_based_on_prefix(local_file_path):
    filename = os.path.basename(local_file_path)
    if filename.startswith('BT'):
        destination_dir = r''
    elif filename.startswith('B'):
        destination_dir = r''
    elif filename.startswith('C'):
        destination_dir = r''
    else:
        return  # Se não corresponder a nenhuma regra, não mover o arquivo
    destination_path = os.path.join(destination_dir, filename)
    shutil.move(local_file_path, destination_path)
    print(f"Arquivo {filename} movido para {destination_dir}")
    # Copia o arquivo baseado na data de modificação para o shared drive
    copy_file_based_on_mod_date(destination_path, r'')
    # Envia mensagem ao Telegram após mover o arquivo
    mensagem_telegram = f"Nome do Arquivo: {filename} \n Movido para o Diretório de Rede:  {destination_dir}"
    enviar_mensagem_telegram(mensagem_telegram)

# Loop de monitoramento contínuo
while True:
    try:
        # Cria uma conexão SFTP
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Lista os arquivos no caminho remoto
        remote_files = sftp.listdir(remote_path)
        new_txt_files = []

        # Verifica quais arquivos são realmente novos comparando com o dicionário local
        for file in remote_files:
            if file.lower().endswith('.txt') and file not in local_file_dict:
                new_txt_files.append(file)
                local_file_dict[file] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Marca o momento do download

        # Salva o dicionário atualizado para garantir que o estado dos arquivos é mantido
        with open(local_dict_file, 'wb') as f:
            pickle.dump(local_file_dict, f)

        # Notifica sobre os novos arquivos
        notify_new_files(new_txt_files)

        for new_file in new_txt_files:
            remote_file_path = remote_path + '/' + new_file
            local_file_path = os.path.join(local_backup_dir, new_file)
            # Download do arquivo
            sftp.get(remote_file_path, local_file_path)
            # Move o arquivo com base no prefixo do nome e copia para o drive compartilhado
            move_file_based_on_prefix(local_file_path)

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
