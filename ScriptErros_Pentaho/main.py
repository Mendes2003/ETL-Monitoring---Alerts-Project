
import re
import smtplib
import os
import pyodbc
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ====================================================
# LER CONFIGURAÇÕES MANUALMENTE DO FICHEIRO PROPERTIES
# ====================================================

def load_properties(filepath):
    props = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                props[key.strip()] = value.strip()
    return props

ROOT_DIR = os.environ.get('ROOT_DIR', r'D:\Projects\cho-bi-hospitalar-etl')
config_path = os.path.join(ROOT_DIR, 'config', 'config.properties')
props = load_properties(config_path)


# ==========================
# LER CONFIGURAÇÕES DW E SA
# =========================

# Configurações para ligação à DW
DW_CONFIG = {
    "server": props["dw.hostname"],
    "database": props["dw.database"],
    "username": props["dw.username"],
    "password": props["dw.password"],
    "port": props["dw.port"],
    "driver": "{ODBC Driver 17 for SQL Server}"
}

# Configurações para ligação à SA
SA_CONFIG = {
    "server": props["sa.hostname"],
    "database": props["sa.database"],
    "username": props["sa.username"],
    "password": props["sa.password"],
    "port": props["sa.port"],
    "driver": "{ODBC Driver 17 for SQL Server}"
}

# ==============================
# FUNÇÃO PARA A LIMPEZA DE ERROS
# ==============================

def clean_pentaho_error(error_content):
    unwanted_patterns = [
        r'^\s*at\s.*',
        r'^\s*\.\.\. \d+ more\b.*',
        r'^\s*Caused by:\s.*',
        r'\bException:\s*$',
        r'^\s*$',
        r'.*org\.pentaho\..*',
    ]

    seen = set()
    cleaned_lines = []

    for line in error_content.split('\n'):
        original_line = line
        line = line.strip()

        if any(re.search(pattern, original_line) for pattern in unwanted_patterns):
            continue

        if not line:
            continue

        if line not in seen:
            seen.add(line)
            cleaned_lines.append(line)

    return '\n'.join(cleaned_lines)

# ===================================
# FUNÇÃO QUE VAI BUSCAR ÚLTIMO JOBID
# ===================================

def get_latest_jobid_dw():
    query = "SELECT MAX(jobid) FROM etl_job"
    conn_str = (
        f"DRIVER={DW_CONFIG['driver']};"
        f"SERVER={DW_CONFIG['server']},{DW_CONFIG['port']};"
        f"DATABASE={DW_CONFIG['database']};"
        f"UID={DW_CONFIG['username']};"
        f"PWD={DW_CONFIG['password']};"
    )
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result else None

# ======================================
# FUNÇÕES QUE VÃO BUSCAR ERROS POR JOBID
# ======================================

def get_dw_errors(jobid):
    query = "SELECT jobid, Transformacao, Descricao FROM etl_erros_dw WHERE jobid = ?"
    conn_str = (
        f"DRIVER={DW_CONFIG['driver']};"
        f"SERVER={DW_CONFIG['server']},{DW_CONFIG['port']};"
        f"DATABASE={DW_CONFIG['database']};"
        f"UID={DW_CONFIG['username']};"
        f"PWD={DW_CONFIG['password']};"
    )
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(query, jobid)
        return cursor.fetchall()

def get_sa_errors(jobid):
    query = "SELECT jobid, Transformacao, Campo, Descricao FROM etl_erros_staging WHERE jobid = ?"
    conn_str = (
        f"DRIVER={SA_CONFIG['driver']};"
        f"SERVER={SA_CONFIG['server']},{SA_CONFIG['port']};"
        f"DATABASE={SA_CONFIG['database']};"
        f"UID={SA_CONFIG['username']};"
        f"PWD={SA_CONFIG['password']};"
    )
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(query, jobid)
        return cursor.fetchall()

# =====================================
# FUNÇÃO PARA FORMATAR O TEXTO DO EMAIL
# =====================================

def format_errors(dw_errors, sa_errors):
    lines = []

    if dw_errors:
        lines.append("[ERROS DW] (tabela etl_erros_dw)\n")
        for row in dw_errors:
            cleaned_desc = clean_pentaho_error(row.Descricao or '')
            lines.append(f"JobID: {row.jobid}")
            lines.append(f"Transformação: {row.Transformacao}")
            lines.append(f"Descrição: {cleaned_desc}\n")

    if sa_errors:
        lines.append("[ERROS SA] (tabela elt_erros_staging)\n")
        for row in sa_errors:
            cleaned_desc = clean_pentaho_error(row.Descricao or '')
            lines.append(f"JobID: {row.jobid}")
            lines.append(f"Transformação: {row.Transformacao}")
            lines.append(f"Campo: {row.Campo}")
            lines.append(f"Descrição: {cleaned_desc}\n")

    return '\n'.join(lines)

# =======================
# GUARDAR EM FICHEIRO TXT
# =======================

def save_to_txt(content):
    output_path = os.path.join(ROOT_DIR, 'Relatorio_Erros.txt')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)

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

    full_body = f"""Foram encontrados erros nas execuções dos jobs.

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
    try:
        latest_jobid = get_latest_jobid_dw()
        if not latest_jobid:
            print("Não foi possível obter o último jobid.")
            return

        dw_errors = get_dw_errors(latest_jobid)
        sa_errors = get_sa_errors(latest_jobid)

        if not dw_errors and not sa_errors:
            print(f"Nenhum erro encontrado para o jobid {latest_jobid}.")
            return

        full_report = format_errors(dw_errors, sa_errors)

        output_path = os.path.join(ROOT_DIR, 'Relatorio_Erros.txt')
        save_to_txt(full_report)

        send_email(
            subject=f"Erros Detetados na Execução do PENTAHO",
            body=full_report,
            to_email=props['mail.addr.destination']
        )

        # Eliminar o ficheiro após envio
        try:
            os.remove(output_path)
            print("Ficheiro eliminado com sucesso.")
        except Exception as e:
            print(f"Erro ao eliminar o ficheiro: {e}")

    except Exception as e:
        print(f"Erro crítico no processo: {e}")

# ========
# EXECUÇÃO
# ========

if __name__ == "__main__":
    main()
