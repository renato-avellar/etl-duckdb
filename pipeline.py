import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from duckdb import DuckDBPyRelation
from pandas import DataFrame

load_dotenv()

def baixar_arquivos_google_driver(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)
    
def listar_arquivos_csv(diretorio):
    arquivos = os.listdir(diretorio)
    arquvos_csv = []
    for arquivo in arquivos:
        if arquivo.endswith('.csv'):
            caminho_completo = os.path.join(diretorio,arquivo)
            arquvos_csv.append(caminho_completo)
    return arquvos_csv

def ler_arquivo_csv(caminho_do_arquivo):
    return duckdb.read_csv(caminho_do_arquivo)
    
def transformar(df: DuckDBPyRelation) -> DataFrame:
    return duckdb.sql("SELECT *, quantidade * valor AS total_vendas from df").df()
    
def salvar_no_postgres(df_duck,tabela):
    url= os.getenv("DATABASE_URL")
    print(url)
    engine = create_engine(url)
    df_duck.to_sql(tabela, engine, if_exists='append', index=False)




if __name__ == "__main__":
    url_pasta = 'https://drive.google.com/drive/folders/19flL9P8UV9aSu4iQtM6Ymv-77VtFcECP'
    diretorio_local = './data'
    lista_de_arquivos = listar_arquivos_csv(diretorio_local)
    
    for arquivo in lista_de_arquivos:
        df_duck_db = ler_arquivo_csv(arquivo)
        df = transformar(df_duck_db)
        salvar_no_postgres(df, "vendas_calculado")
    
    
 