import pyodbc
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# ==========================
# FUNÇÃO PARA LER ARQUIVO .PROPERTIES SEM SECTIONS
# ==========================

def load_properties(filepath):
    props = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                props[key.strip()] = value.strip()
    return props


# ==========================
# LER CONFIGURAÇÕES SSISDB
# ==========================

ROOT_DIR = os.environ.get('ROOT_DIR', r'C:\Projects\ETL\bi-hospitalar-etl')
config_path = os.path.join(ROOT_DIR, 'config', 'config.properties')
props = load_properties(config_path)

SSIS_CONFIG = {
    "server": props["dw.hostname"],
    "database": props["dw.database"],
    "username": props["dw.username"],
    "password": props["dw.password"],
    "port": props["dw.port"],
    "driver": "{ODBC Driver 17 for SQL Server}"
}


# =================================
# FUNÇÃO PARA OBTER ERROS DO SSISDB
# =================================

def get_latest_ssis_error():
    try:
        conn_str = (
            f"DRIVER={SSIS_CONFIG['driver']};"
            f"SERVER={SSIS_CONFIG['server']},{SSIS_CONFIG['port']};"
            f"DATABASE={SSIS_CONFIG['database']};"
            f"UID={SSIS_CONFIG['username']};"
            f"PWD={SSIS_CONFIG['password']}"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        query = """
        WITH ErrosCUBO AS (
            SELECT 
                EM.event_message_id,
                EM.message_time AS DataHora,
                EM.message AS MensagemErro,
                E.execution_id AS Execution_Id,
                E.package_name AS Package_Name,
                E.project_name AS Project_Name,
                ROW_NUMBER() OVER (
                    PARTITION BY E.execution_id, EM.message 
                    ORDER BY EM.message_time DESC
                ) AS rn
            FROM 
                SSISDB.catalog.event_messages AS EM
            JOIN 
                SSISDB.catalog.executions AS E 
                ON EM.operation_id = E.execution_id
            WHERE 
                EM.event_name = 'OnError'
        )
        SELECT TOP 1
            Execution_Id,
            DataHora,
            MensagemErro,
            Package_Name,
            Project_Name
        FROM 
            ErrosCUBO
        WHERE 
            rn = 1
        ORDER BY 
            Execution_Id DESC
        """

        cursor.execute(query)
        return cursor.fetchall()

    except Exception as e:
        print(f"Erro ao buscar dados do SSISDB: {str(e)}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()

# ============================================
# FUNÇÃO PARA FORMATAR O TEXTO DO EMAIL (SSIS)
# ============================================

def format_ssis_errors(errors):
    lines = ["[ERROS CUBO] \n"]
    for error in errors:
        lines.append(f"Execution ID : {error.Execution_Id}")
        lines.append(f"Data/Hora     : {error.DataHora}")
        lines.append(f"Projeto       : {error.Project_Name}")
        lines.append(f"Pacote        : {error.Package_Name}")
        lines.append(f"Mensagem Erro : {error.MensagemErro}\n")
    return '\n'.join(lines)


# ====================
# ENVIO DE EMAIL
# ====================

def send_email(subject, body, to_email):
    sender_email = props['mail.addr.sender']
    sender_password = props['mail.server.password']
    smtp_server = props['mail.server']
    smtp_port = int(props['mail.server.port'])

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = to_email
    msg['Subject'] = subject

    full_body = f"""Foram encontrados erros nas execuções do CUBO.

----------------------------
Detalhes dos Erros:
----------------------------

{body}

Este é um e-mail automático. Por favor verifique.
"""
    msg.attach(MIMEText(full_body, 'plain'))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, to_email, msg.as_string())
        print("Email enviado com sucesso.")
    except Exception as e:
        print(f"Erro ao enviar email: {e}")

# ================
# FUNÇÃO PRINCIPAL
# ================

def main():
    errors = get_latest_ssis_error()

    if errors:
        email_body = format_ssis_errors(errors)
        send_email(
            subject=f"Erros Detectados na Execução do CUBO ",
            body=email_body,
            to_email=props['mail.recipient']
        )
    else:
        print("Nenhum erro encontrado nas últimas execuções.")


# ========
# EXECUÇÃO
# ========

if __name__ == "__main__":
    main()
