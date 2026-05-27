import os
import traceback
import pandas as pd
import mysql.connector
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

load_dotenv()

def get_env_int(name, default):
    """Lê variável de ambiente e garante retorno de número inteiro."""
    try:
        val = os.environ.get(name)
        if val:
            return int(str(val).strip())
        return default
    except (ValueError, TypeError):
        return default

# Configurações de conexão (SSH e Banco de Dados)
SSH_HOST = os.environ.get("SSH_HOST")
SSH_PORT = get_env_int("SSH_PORT", 22)
SSH_USER = os.environ.get("SSH_USER")
SSH_PASSWORD = os.environ.get("SSH_PASSWORD")

DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
DB_PORT = get_env_int("DB_PORT", 3306)

# Configurações do Google Sheets
SHEET_ID = os.getenv('SHEET_ID')
ABA_ESTOQUE = os.getenv('ABA_ESTOQUE')
GOOGLE_JSON_PATH = os.getenv('GOOGLE_JSON_PATH')

def get_query_data():
    try:
        with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_password=SSH_PASSWORD,
            remote_bind_address=(DB_HOST, DB_PORT)
        ) as tunnel:
            
            print(f"Conexão SSH estabelecida na porta local: {tunnel.local_bind_port}")

            conn = mysql.connector.connect(
                host='127.0.0.1',
                port=tunnel.local_bind_port,
                user=DB_USER,
                password=DB_PASS,
                database=DB_NAME
            )

            query = """
            SELECT
                storeno AS LOJA,
                sano AS AREA,
                trim(prdno) AS CODIGO,
                prd.name AS PRODUTO,
                LEFT(stksa.grade, 5) AS GRADE,
                RIGHT(stksa.grade, 3) AS PACOTE,
                qtty_varejo / 1000 AS 'ESTOQUE ATUAL',
                type.name AS 'TIPO DO PRODUTO'
            FROM stksa
            JOIN prd ON prd.no = stksa.prdno
            JOIN type ON prd.typeno = type.no
            WHERE storeno = 70
              AND sano = 1
              AND qtty_varejo <> 0;
            """

            df = pd.read_sql(query, conn)
            
            if 'CODIGO' in df.columns:
                df['CODIGO'] = pd.to_numeric(df['CODIGO'], errors='coerce').fillna(0).astype(int)
            # ----------------------------------------

            conn.close()
            return df
            
    except Exception as e:
        print(f"Erro ao processar SQL: {e}")
        traceback.print_exc()
        return None

def update_google_sheets(df):
    try:
        if not GOOGLE_JSON_PATH or not os.path.exists(GOOGLE_JSON_PATH):
            raise FileNotFoundError(f"Arquivo de credenciais do Google não encontrado: {GOOGLE_JSON_PATH}")

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_JSON_PATH, scope)
        client = gspread.authorize(creds)
        
        planilha = client.open_by_key(SHEET_ID)
        aba = planilha.worksheet(ABA_ESTOQUE)
        
        # Limpa a aba antes de inserir os novos dados de estoque
        aba.clear()
        
        # Preenche valores nulos com texto vazio
        df_limpo = df.fillna("")
        
        # Converte o DataFrame para o formato de lista que o gspread aceita
        dados = [df_limpo.columns.values.tolist()] + df_limpo.values.tolist()
        
        # Atualiza a partir da célula A1
        aba.update('A1', dados)
        
        print(f"Planilha de estoque atualizada com sucesso em {datetime.now().strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"Erro Google Sheets: {type(e).__name__}: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    df_resultado = get_query_data()
    if df_resultado is not None and not df_resultado.empty:
        update_google_sheets(df_resultado)
    else:
        print("Fim da execução: Nenhum dado de estoque encontrado para processar.")