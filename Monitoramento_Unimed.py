import asyncio
from ftplib import FTP
import os.path
import time
from datetime import datetime
import aiohttp

# Dados de conexão
host = ''
username = ''
password = ''

# Dados do Telegram
token = ''
chat_id = ''

# Função para listar arquivos em um diretório
def listar_arquivos(ftp_connection, directory):
    ftp_connection.cwd(directory)
    return ftp_connection.nlst()

# Função para enviar mensagem para o Telegram
async def enviar_mensagem_telegram(session, token, chat_id, mensagem):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {'chat_id': chat_id, 'text': mensagem}
    async with session.post(url, data=params, ssl=False) as response:
        if response.status != 200:
            print(f"Erro ao enviar mensagem para o Telegram: {response.status}")
            print(await response.text())
            registrar_erro(f"Erro ao enviar mensagem para o Telegram: {response.status}")
        else:
            print("Mensagem enviada ao Telegram com sucesso.")

# Função para conectar ao servidor FTP
def conectar_ftp(host, username, password):
    ftp = FTP(host)
    ftp.login(username, password)
    return ftp

# Função para registrar erros em um arquivo de log
def registrar_erro(mensagem):
    data_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("erros.log", "a") as arquivo:
        arquivo.write(f"{data_hora}: {mensagem}\n")

# Função para monitorar os diretórios no servidor FTP
async def monitorar_ftp():
    global primeira_verificacao
    tentativas = 0
    while tentativas < 20:
        try:
            ftp = conectar_ftp(host, username, password)
            print("Conexão FTP estabelecida.")
            return ftp
        except Exception as e:
            mensagem_erro = f"Falha ao conectar ao FTP: {e}"
            print(mensagem_erro)
            registrar_erro(mensagem_erro)
            tentativas += 1
            print(f"Tentando novamente em 10 minutos... (Tentativa {tentativas}/20)")
            await asyncio.sleep(600)  # 10 minutos
    mensagem_erro = "Número máximo de tentativas de conexão excedido. Encerrando programa."
    print(mensagem_erro)
    registrar_erro(mensagem_erro)
    return None

# Diretórios para monitoramento
diretorios = ['', '']

# Dicionário para armazenar os nomes dos arquivos
arquivos_existentes = {}

# Flag para indicar a primeira verificação
primeira_verificacao = True

# Loop infinito para monitoramento contínuo
async def main():
    global primeira_verificacao
    while True:
        async with aiohttp.ClientSession() as session:
            ftp = await monitorar_ftp()
            if ftp is not None:
                try:
                    print("Realizando monitoramento...")
                    # Verificando se existem arquivos novos
                    for diretorio in diretorios:
                        arquivos = listar_arquivos(ftp, diretorio)
                        for arquivo in arquivos:
                            if primeira_verificacao:
                                arquivos_existentes[arquivo] = os.path.join(host, diretorio, arquivo)
                            elif arquivo not in arquivos_existentes:
                                arquivos_existentes[arquivo] = os.path.join(host, diretorio, arquivo)
                                # Envia mensagem para o Telegram
                                mensagem = f"Novo arquivo encontrado: {arquivo} no diretório {diretorio}. Data e hora: {datetime.now()}"
                                await enviar_mensagem_telegram(session, token, chat_id, mensagem)
                    # Se for a primeira verificação, marque como False para as verificações subsequentes
                    if primeira_verificacao:
                        primeira_verificacao = False
                        print("Dicionário de arquivos existentes criado.")
                except Exception as e:
                    mensagem_erro = f"Erro durante o monitoramento: {e}"
                    print(mensagem_erro)
                    registrar_erro(mensagem_erro)
                finally:
                    ftp.quit()  # Fechando a conexão FTP
            # Aguardando 30 segundos antes da próxima verificação
            await asyncio.sleep(30)

# Iniciar o loop de eventos assíncrono
asyncio.run(main())
