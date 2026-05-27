import os
import json
import time
import pandas as pd
import gspread
import mysql.connector
from datetime import datetime
from sshtunnel import SSHTunnelForwarder
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()

def get_env_int(name, default):
    value = os.environ.get(name, "")
    return int(value) if value.strip().isdigit() else default

ssh_host = os.environ.get("SSH_HOST")
ssh_port = get_env_int("SSH_PORT", 22)
ssh_user = os.environ.get("SSH_USER")
ssh_password = os.environ.get("SSH_PASSWORD")

mysql_host = os.environ.get("DB_HOST")
mysql_user = os.environ.get("DB_USER")
mysql_password = os.environ.get("DB_PASS")
mysql_db = os.environ.get("DB_NAME")
mysql_port = get_env_int("DB_PORT", 3306)

SHEET_ID = os.environ.get("SHEET_ID")
ABA_VENDA = os.environ.get("ABA_VENDA")

ARQUIVO_CONTROLE = os.path.abspath("controle_incremental.json")

MODO_TESTE = True  
DATA_INICIO_TESTE = "20260101"
DATA_FIM_TESTE = "20260131"

def inicializar_controle():
    if not os.path.exists(ARQUIVO_CONTROLE):
        controle = {
            "pxa": {"date": "20260101", "time": 0, "nfno": 0},
            "xalog2": {"date": "20260101", "time": 0, "nfno": 0}
        }
        with open(ARQUIVO_CONTROLE, "w") as f:
            json.dump(controle, f, indent=4)

def ler_controle():
    with open(ARQUIVO_CONTROLE, "r") as f:
        return json.load(f)

def salvar_controle(origem, date, time, nfno):
    controle = ler_controle()
    controle[origem] = {"date": date, "time": int(time), "nfno": int(nfno)}
    with open(ARQUIVO_CONTROLE, "w") as f:
        json.dump(controle, f, indent=4)

def conectar_banco():
    server = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_password=ssh_password,
        remote_bind_address=(mysql_host, mysql_port)
    )
    server.start()
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=server.local_bind_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db,
        connect_timeout=30
    )
    return conn, server

def conectar_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credenciais_google.json", scope)
    return gspread.authorize(creds)


def buscar_dados_pxa(controle=None):
    if MODO_TESTE:
        query = """
            SELECT
                m.time AS HORA,
                m.nfno AS NF,
                CAST(m.date AS CHAR) AS DATA,
                CONCAT(m.custno, '-', custp.name) AS CLIENTE,
                CONCAT(m.nfno, '/', m.nfse) AS 'NOTA/SERIE',
                TRIM(f.prdno) AS CODIGO,
                prd.name AS PRODUTO,
                f.grade AS GRADE,
                type.name AS 'TIPO PRODUTO',
                f.qtty AS 'QUANTIDADE',
                xaprd2.precoUnitario AS 'VALOR TOTAL'
            FROM pxaprd f
            JOIN pxa m ON f.xano = m.xano
            JOIN xaprd2 ON f.xano = xaprd2.xano AND f.prdno = xaprd2.prdno AND f.storeno = xaprd2.storeno
            JOIN custp ON m.custno = custp.no
            JOIN prd ON f.prdno = prd.no
            JOIN type ON prd.typeno = type.no
            WHERE m.storeno = 70
              AND m.date BETWEEN %s AND %s
            ORDER BY m.date, m.time, m.nfno
        """
        params = (DATA_INICIO_TESTE, DATA_FIM_TESTE)
    else:
        data_busca, hora_busca, nota_busca = controle["pxa"]["date"], controle["pxa"]["time"], controle["pxa"]["nfno"]
        query = """
            SELECT
                m.time AS HORA,
                m.nfno AS NF,
                CAST(m.date AS CHAR) AS DATA,
                CONCAT(m.custno, '-', custp.name) AS CLIENTE,
                CONCAT(m.nfno, '/', m.nfse) AS 'NOTA/SERIE',
                TRIM(f.prdno) AS CODIGO,
                prd.name AS PRODUTO,
                f.grade AS GRADE,
                type.name AS 'TIPO PRODUTO',
                f.qtty AS 'QUANTIDADE',
                xaprd2.precoUnitario AS 'VALOR TOTAL'
            FROM pxaprd f
            JOIN pxa m ON f.xano = m.xano
            JOIN xaprd2 ON f.xano = xaprd2.xano AND f.prdno = xaprd2.prdno AND f.storeno = xaprd2.storeno
            JOIN custp ON m.custno = custp.no
            JOIN prd ON f.prdno = prd.no
            JOIN type ON prd.typeno = type.no
            WHERE m.storeno = 70
              AND ((m.date > %s) OR (m.date = %s AND m.time > %s) OR (m.date = %s AND m.time = %s AND m.nfno > %s))
            ORDER BY m.date, m.time, m.nfno
        """
        params = (data_busca, data_busca, hora_busca, data_busca, hora_busca, nota_busca)
    return query, params

def buscar_dados_xalog2(controle=None):
    if MODO_TESTE:
        query = """
            SELECT
                x.time AS HORA,
                inv.nfNfno AS NF,
                CAST(x.date AS CHAR) AS DATA,
                CONCAT(x.custno, '-', custp.name) AS CLIENTE,
                CONCAT(MID(x.doc,1,LOCATE('/',x.doc)-1),'/',MID(x.doc,LOCATE('/',x.doc)+1,2)) AS 'NOTA/SERIE',
                TRIM(x.prdno) AS CODIGO,
                prd.name AS PRODUTO,
                x.grade AS GRADE,
                type.name AS 'TIPO PRODUTO',
                x.qtty AS 'QUANTIDADE',
                -1 * (((x.qtty / 1000) * (x.price / 100)) - ABS(x.discount / 100)) AS 'VALOR TOTAL'
            FROM xalog2 x
            JOIN custp ON x.custno = custp.no
            JOIN prd ON x.prdno = prd.no
            JOIN type ON prd.typeno = type.no
            JOIN inv ON x.storeno = inv.storeno AND x.xano = inv.auxLong1
            WHERE x.storeno = 70
              AND x.qtty < 0
              AND x.date BETWEEN %s AND %s
            ORDER BY x.date, x.time, inv.nfNfno
        """
        params = (DATA_INICIO_TESTE, DATA_FIM_TESTE)
    else:
        data_busca, hora_busca, nota_busca = controle["xalog2"]["date"], controle["xalog2"]["time"], controle["xalog2"]["nfno"]
        query = """
            SELECT
                x.time AS HORA,
                inv.nfNfno AS NF,
                CAST(x.date AS CHAR) AS DATA,
                CONCAT(x.custno, '-', custp.name) AS CLIENTE,
                CONCAT(MID(x.doc,1,LOCATE('/',x.doc)-1),'/',MID(x.doc,LOCATE('/',x.doc)+1,2)) AS 'NOTA/SERIE',
                TRIM(x.prdno) AS CODIGO,
                prd.name AS PRODUTO,
                x.grade AS GRADE,
                type.name AS 'TIPO PRODUTO',
                x.qtty AS 'QUANTIDADE',
                -1 * (((x.qtty / 1000) * (x.price / 100)) - ABS(x.discount / 100)) AS 'VALOR TOTAL'
            FROM xalog2 x
            JOIN custp ON x.custno = custp.no
            JOIN prd ON x.prdno = prd.no
            JOIN type ON prd.typeno = type.no
            JOIN inv ON x.storeno = inv.storeno AND x.xano = inv.auxLong1
            WHERE x.storeno = 70
              AND x.qtty < 0
              AND ((x.date > %s) OR (x.date = %s AND x.time > %s) OR (x.date = %s AND x.time = %s AND inv.nfNfno > %s))
            ORDER BY x.date, x.time, inv.nfNfno
        """
        params = (data_busca, data_busca, hora_busca, data_busca, hora_busca, nota_busca)
    return query, params


def processar_e_salvar(df, origem, sheet):
    if df.empty:
        print(f"Nenhum dado novo de {origem}.")
        return

    df['QUANTIDADE'] = (df['QUANTIDADE'] / 1000).round(3)
    if origem == "pxa":
        df['VALOR TOTAL'] = (df['VALOR TOTAL'] / 100).round(2)
    elif origem == "xalog2":
        df['VALOR TOTAL'] = df['VALOR TOTAL'].round(2)  

    df['VALOR UNITÁRIO'] = (df['VALOR TOTAL'] / df['QUANTIDADE'].abs()).round(2)

    df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')

    ultima_linha = df.iloc[-1]
    
    colunas_ordenadas = [
        'HORA', 'NF', 'DATA', 'CLIENTE', 'NOTA/SERIE', 'CODIGO', 
        'PRODUTO', 'GRADE', 'TIPO PRODUTO', 'QUANTIDADE', 'VALOR UNITÁRIO', 'VALOR TOTAL'
    ]
    df_upload = df[colunas_ordenadas].copy()
    
    df_upload['HORA'] = pd.to_timedelta(df_upload['HORA'], unit='s').astype(str).str.split().str[-1]
    df_upload['DATA'] = df_upload['DATA'].dt.strftime('%Y-%m-%d')
    df_upload = df_upload.fillna('')

    valores_existentes = [row for row in sheet.get_all_values() if any(cell.strip() for cell in row)]
    
    if not valores_existentes:
        print(f"Planilha identificada como vazia. Inserindo cabeçalhos para {origem}...")
        sheet.append_row(df_upload.columns.tolist())
        time.sleep(1)

    dados = df_upload.values.tolist()
    sheet.append_rows(dados, value_input_option="USER_ENTERED")

    if not MODO_TESTE:
        nova_data = ultima_linha['DATA'].strftime('%Y%m%d')
        salvar_controle(origem, nova_data, ultima_linha['HORA'], ultima_linha['NF'])

    print(f"{len(df)} linhas de {origem} enviadas com sucesso.")


def main():
    inicializar_controle()
    controle = ler_controle()

    conn, server = conectar_banco()
    client = conectar_sheets()
    sheet = client.open_by_key(SHEET_ID).worksheet(ABA_VENDA)

    try:
        query, params = buscar_dados_pxa(controle)
        df_pxa = pd.read_sql(query, conn, params=params)
        processar_e_salvar(df_pxa, "pxa", sheet)

        time.sleep(1.5)

        query, params = buscar_dados_xalog2(controle)
        df_xalog2 = pd.read_sql(query, conn, params=params)
        processar_e_salvar(df_xalog2, "xalog2", sheet)

    finally:
        conn.close()
        server.stop()

if __name__ == "__main__":
    main()