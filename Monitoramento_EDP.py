import paramiko
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# Dados de conexão
host = ''
port = 22
username = ''
password = ''

# Dados do servidor de e-mail
smtp_server = 'smtp.gmail.com'
smtp_port = 587
smtp_username = ''
smtp_password = ''

# Dados do e-mail
sender_email = ''
receiver_emails = ['']
email_subject = 'Novo arquivo adicionado no SFTP'
email_body_template = 'Um novo arquivo foi adicionado:\n\nNome do arquivo: {}\nDiretório: {}\nData: {}'

# Diretórios que serão monitorados
diretorios = [
    '',
    '',
    '',
    ''
]

# Dicionário para guardar o estado dos arquivos em cada diretório
estado_anterior = {diretorio: set() for diretorio in diretorios}

def listar_arquivos(sftp, diretorio):
    arquivos = sftp.listdir(diretorio)
    return set(arquivos)

def comparar_arquivos(diretorio, arquivos_atuais):
    arquivos_anteriores = estado_anterior[diretorio]
    novos_arquivos = arquivos_atuais - arquivos_anteriores
    return novos_arquivos, arquivos_atuais

def atualizar_estado(diretorio, arquivos_atuais):
    estado_anterior[diretorio] = arquivos_atuais

def enviar_email(arquivo, diretorio, data, titulo_email):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(receiver_emails)  # Junta os destinatários separados por vírgula
    msg['Subject'] = titulo_email

    email_body_template = 'Um novo arquivo foi adicionado:\n\nNome do arquivo: {}\nDiretório: {}\nData: {}'
    email_body = email_body_template.format(arquivo, diretorio, data)
    msg.attach(MIMEText(email_body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(sender_email, receiver_emails, msg.as_string())
        print("E-mail enviado com sucesso!")
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")
    finally:
        server.quit()

# Variável para indicar se a primeira verificação foi realizada
primeira_verificacao = True

def obter_titulo_email(diretorio):
    if "EDP SP/Exclusao" in diretorio:
        return "Novo arquivo adicionado no SFTP - EDP SP"
    elif "EDP SP/Remessa" in diretorio:
        return "Novo arquivo adicionado no SFTP - EDP SP"
    elif "EDP ES/Exclusao" in diretorio:
        return "Novo arquivo adicionado no SFTP - EDP ES"
    elif "EDP ES/Remessa" in diretorio:
        return "Novo arquivo adicionado no SFTP - EDP ES"
    else:
        return "Novo arquivo adicionado no SFTP"

def conectar_sftp():
    tentativas_maximas = 15
    tentativa_atual = 0

    while tentativa_atual < tentativas_maximas:
        try:
            transport = paramiko.Transport((host, port))
            transport.connect(username=username, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            return sftp, transport
        except paramiko.ssh_exception.NoValidConnectionsError as e:
            print(f"Erro de conexão: {e}")
        except Exception as e:
            print(f"Erro inesperado: {e}")

        tentativa_atual += 1
        print(f"Tentativa de reconexão #{tentativa_atual}. Aguardando 10 segundos...")
        time.sleep(60)

    # Se todas as tentativas falharem, levante uma exceção para indicar o problema
    raise Exception("Falha ao conectar ao SFTP após várias tentativas.")

def monitorar_diretorios():
    global primeira_verificacao  # Usar a variável global

    # Movendo a criação do objeto SFTP para fora do loop
    sftp = None
    transport = None

    try:
        sftp, transport = conectar_sftp()

        # Adicionando um print para indicar a conexão bem-sucedida
        print(f"Conexão SFTP estabelecida com sucesso para {host}")

        while True:
            for diretorio in diretorios:
                arquivos_atuais = listar_arquivos(sftp, diretorio)

                # Primeira verificação, apenas atualiza o estado e pula para o próximo diretório
                if 'primeira_verificacao' not in globals() or primeira_verificacao:
                    atualizar_estado(diretorio, arquivos_atuais)
                    continue

                novos_arquivos, arquivos_atuais = comparar_arquivos(diretorio, arquivos_atuais)

                if novos_arquivos:
                    data_atual = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
                    titulo_email = obter_titulo_email(diretorio)
                    for arquivo in novos_arquivos:
                        enviar_email(arquivo, diretorio, data_atual, titulo_email)
                        # Adicionando um print indicando que um novo arquivo foi encontrado
                        print(f"Novo arquivo encontrado: {arquivo} em {diretorio}")

                atualizar_estado(diretorio, arquivos_atuais)

            # Adicionando um print para indicar uma nova verificação a cada 60 segundos
            print(f"Nova verificação às {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(60)  # Intervalo entre verificações (em segundos)

            # Indicar que a primeira verificação foi concluída
            primeira_verificacao = False

    except Exception as e:
        print(f"Erro durante o monitoramento: {e}")

    finally:
        if transport:
            transport.close()

if __name__ == "__main__":
    monitorar_diretorios()
