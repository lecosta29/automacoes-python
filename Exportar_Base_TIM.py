import paramiko
import datetime
import os
import shutil
from telegram import Bot

# Token do seu bot no Telegram
telegram_bot_token = ''

# Chat ID do seu canal ou grupo no Telegram (onde você deseja enviar as notificações)
telegram_chat_id = ''

# Defina as informações de conexão SFTP
sftp_host = ''
sftp_port = 22
sftp_username = ''
sftp_password = ''
sftp_remote_directory = '/exportacao/Consulta'

# Diretório de backup local
local_backup_dir = r''

# Diretórios de destino
destination_dir_base = r''
destination_dir_fone = r''

# Crie uma instância do cliente SSH
ssh_client = paramiko.SSHClient()

# Carregue as chaves do sistema (opcional)
# ssh_client.load_system_host_keys()

# Aceite automaticamente chaves de host desconhecidas (não recomendado para produção)
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# Crie uma instância do bot do Telegram
bot = Bot(token=telegram_bot_token)

async def main():
    try:
        # Conecte-se ao servidor SFTP
        ssh_client.connect(sftp_host, sftp_port, sftp_username, sftp_password)

        # Crie uma instância do cliente SFTP
        sftp_client = ssh_client.open_sftp()

        # Lista de arquivos no diretório remoto
        files = sftp_client.listdir_attr(sftp_remote_directory)

        # Data atual
        current_date = datetime.datetime.now().date()
        current_date_str = current_date.strftime("%d%m%Y")

        # Faça algo com os arquivos cuja data de modificação é a data atual
        print(f"Arquivos no diretório '{sftp_remote_directory}' modificados hoje:")
        for file in files:
            file_name = file.filename
            file_mtime = datetime.datetime.fromtimestamp(file.st_mtime).date()

            # Verifique se a data de modificação é a data atual
            if file_mtime == current_date:
                print(f"{file_name} - Última modificação: {file_mtime}")
                # Caminho de origem no SFTP
                source_path = f'{sftp_remote_directory}/{file_name}'

                # Caminho de destino no diretório de backup local
                destination_path = os.path.join(local_backup_dir, file_name)

                # Baixe o arquivo do SFTP para o diretório de backup local
                sftp_client.get(source_path, destination_path)

                # Verifique se o nome do arquivo contém "_2" e mova-o para o diretório apropriado
                if "_2" in file_name:
                    destination_dir = destination_dir_fone
                else:
                    destination_dir = destination_dir_base

                # Renomeie o arquivo adicionando a data atual ao final do nome
                new_file_name = f'{os.path.splitext(file_name)[0]}_{current_date_str}{os.path.splitext(file_name)[1]}'
                new_file_path = os.path.join(destination_dir, new_file_name)
                shutil.move(destination_path, new_file_path)
                print(f"{file_name} - Movido e renomeado para: {new_file_name}")

                # Envie a notificação para o Telegram
                message = f'Arquivo "{new_file_name}" movido para {destination_dir} em {datetime.datetime.now()}'
                await bot.send_message(chat_id=telegram_chat_id, text=message)

        # Feche a conexão SFTP
        sftp_client.close()

    finally:
        # Feche a conexão SSH
        ssh_client.close()

# Executar a função principal assíncrona
import asyncio
asyncio.run(main())
