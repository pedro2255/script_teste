import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

try:
    print('Conectando ao banco de dados...')
    conn = psycopg2.connect(host=DB_HOST,
                            port=DB_PORT, 
                            dbname=DB_NAME, 
                            user=DB_USER, 
                            password=DB_PASSWORD)
    print('Conexão bem sucedida!')

    cur = conn.cursor()

    print('Executando query...')
    query = "SELECT * FROM acordos_integrar_sap;"
    df = pd.read_sql_query(query, conn)
    
    media_fatura_nova_acordada = df['valor_original_fatura_nova_acordada'].mean()
    soma_fatura_nova_acordada = df['valor_original_fatura_nova_acordada'].sum()

    print(f'Média da fatura nova acordada: {media_fatura_nova_acordada}')
    print(f'Soma da fatura nova acordada: {soma_fatura_nova_acordada}')


except psycopg2.Error as e:
    print(f'Erro ao conectar ao banco de dados: {e}')

finally:
    if cur:
        cur.close()
    if conn:
        conn.close()    
        