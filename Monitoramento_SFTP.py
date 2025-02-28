import paramiko
import schedule
import asyncio
import time
from datetime import datetime
from telegram import Bot

# Função assíncrona para enviar mensagem para o canal do Telegram
async def enviar_mensagem_telegram(token, chat_id, mensagem):
    bot = Bot(token=token)
    await bot.send_message(chat_id=chat_id, text=mensagem)

# Função para verificar a conectividade do SFTP
def verificar_conectividade_sftp(host, porta, usuario, senha):
    try:
        cliente_sftp = paramiko.SSHClient()
        cliente_sftp.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        cliente_sftp.connect(hostname=host, port=porta, username=usuario, password=senha)
        cliente_sftp.close()
        return True, None
    except Exception as e:
        print(f"Erro ao conectar ao SFTP {host}: {e}")
        return False, str(e)

# Função para criar a mensagem do checklist
def criar_mensagem_checklist_sftp(sftps, sucesso, erros=None):
    # Monta o título com a data e hora do monitoramento
    titulo = f"↘️ Checklist SFTP {datetime.now().strftime('%d/%m/%y %H:%M')}:\n\n"
    
    # Monta a lista de verificação para cada SFTP
    lista_verificacao = ""
    for sftp in sftps:
        status = "✅" if sucesso else "❌"
        lista_verificacao += f"{status} SFTP {sftp['nome']}\n"
    
    mensagem = titulo + lista_verificacao
    
    if not sucesso and erros:
        mensagem += "\n\nErros de Conexão:\n"
        for erro in erros:
            mensagem += erro + "\n"
    
    return mensagem

# Função de monitoramento dos SFTPs
def monitorar_sftps():
    print("Iniciando monitoramento...")
    sftps_para_monitorar = [
        {"nome": "", "host": "", "porta": 22, "usuario": "", "senha": ""},
        {"nome": "", "host": "", "porta": 22, "usuario": "", "senha": ""},
        {"nome": "", "host": "", "porta": 22, "usuario": "", "senha": "m"},
        {"nome": "", "host": "", "porta": 22, "usuario": "", "senha": ""},
        {"nome": "", "host": "", "porta": 2222, "usuario": "", "senha": ""},
    ]

    # Lista para armazenar os erros de conexão
    erros = []

    # Verifica a conectividade para cada SFTP
    for sftp in sftps_para_monitorar:
        conectado, erro = verificar_conectividade_sftp(sftp["host"], sftp["porta"], sftp["usuario"], sftp["senha"])
        sftp["conectado"] = conectado
        if not conectado:
            erros.append(f"Erro ao conectar ao SFTP {sftp['nome']}: {erro}")

    # Criar mensagem do checklist
    mensagem_sucesso = criar_mensagem_checklist_sftp(sftps_para_monitorar, sucesso=True)
    mensagem_falha = criar_mensagem_checklist_sftp(sftps_para_monitorar, sucesso=False)

    # Enviar mensagem para o Telegram apenas se houver erros de conexão
    if erros:
        asyncio.run(enviar_mensagem_telegram(token="6605381922:AAFoUknSUF8fGHJWHIhKFmGjnDGXGByhuGE", chat_id="-1001928552630", mensagem=mensagem_falha))
    else:
        asyncio.run(enviar_mensagem_telegram(token="6605381922:AAFoUknSUF8fGHJWHIhKFmGjnDGXGByhuGE", chat_id="-1001928552630", mensagem=mensagem_sucesso))

    print("Monitoramento concluído.")

    # Se houve falha de conexão, reagendar o monitoramento para verificar a cada 10 minutos
    if erros:
        print("Reagendando monitoramento para verificar a cada 10 minutos...")
        schedule.clear()  # Limpar agendamentos anteriores
        schedule.every(10).minutes.do(monitorar_sftps)
    else:
        print("Reagendando monitoramento para verificar a cada hora...")
        schedule.clear()  # Limpar agendamentos anteriores
        schedule.every().hour.do(monitorar_sftps)

# Chamar a função de monitoramento uma vez para verificar a conectividade inicial
monitorar_sftps()

while True:
    schedule.run_pending()
    time.sleep(1)
