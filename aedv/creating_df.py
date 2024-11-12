""" Módulo de Agrupamento de empresa e volume
"""

import pandas as pd
import matplotlib.pyplot as plt
import os
from utils import *

# Caminho para a pasta que contém os arquivos CSV
folder_path = "data/df_meses_2017_mandioqueiro"

# Inicializar uma lista para armazenar os dataframes mensais
dfs = []

# Iterar sobre os arquivos de janeiro a dezembro
for month in range(1, 13):
    # Gerar o nome do arquivo com base no mês (ex: df_01.csv, df_02.csv, ...)
    file_path = os.path.join(folder_path, f"df_{month:02d}.csv")
    
    # Ler o arquivo CSV e adicioná-lo à lista de dataframes
    df = pd.read_csv(file_path, dtype={'Registro': str, 'CPF_CNPJ_Rem': str, 'CPF_CNPJ_Des': str})
    
    # Converter a coluna 'datetimeDtEmissao' para datetime
    df['datetimeDtEmissao'] = pd.to_datetime(df['datetimeDtEmissao'], errors='coerce')
    
    # Adicionar o dataframe lido à lista
    dfs.append(df)

# Combinar todos os dataframes mensais em um único dataframe
df_2017 = pd.concat(dfs, ignore_index=True)

def agrupa_e_arruma(df: pd.DataFrame, eh_entrada: bool) -> pd.DataFrame:
    """
    Agrupa e organiza os dados de transações de madeira por data e empresa.

    Esta função agrupa o DataFrame `df` por empresa e data, somando o volume diário de transações.
    Dependendo do valor de `eh_entrada`, o agrupamento considera a empresa de origem ou destino.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame contendo o histórico de transações de madeira, com algumas colunas do tipo:
        - 'CPF_CNPJ_Rem' : str
            CPF/CNPJ da empresa de origem.
        - 'CPF_CNPJ_Des' : str
            CPF/CNPJ da empresa de destino.
        - 'datetimeDtEmissao' : datetime-like
            Data da emissão da transação.
        - 'Volume' : float
            Volume de madeira transacionado.
        
    eh_entrada : bool
        Se `True`, agrupa os dados considerando a empresa de destino (`CPF_CNPJ_Rem`). 
        Se `False`, considera a empresa de origem (`CPF_CNPJ_Des`).

    Returns
    -------
    pandas.DataFrame
        DataFrame com as transações agrupadas por empresa e data, com as seguintes colunas:
        - 'Empresa' : str
            CPF/CNPJ da empresa envolvida na transação (origem ou destino, conforme `eh_entrada`).
        - 'Data' : datetime64
            Data da transação, convertida para o formato `datetime`.
        - 'Volume_Saida' : float
            Volume total transacionado para cada dia e empresa.

    Notes
    -----
    - A coluna 'Data' no DataFrame de entrada deve ser compatível com o tipo `datetime` para uso de `.dt.date`.
    - A coluna de `Volume_Saida` é preenchida com o volume total diário para cada empresa.
    - O DataFrame `df` original não é modificado; a função retorna uma nova estrutura de dados.
    """
    # Agrupando os meses das empresas de destino e origem para separar em df_entrada e df_saida
    if eh_entrada:
        df = df_2017.groupby(['CPF_CNPJ_Rem', df_2017['datetimeDtEmissao'].dt.date])['Volume'].sum().reset_index()
    else:
        df = df_2017.groupby(['CPF_CNPJ_Des', df_2017['datetimeDtEmissao'].dt.date])['Volume'].sum().reset_index()
    
    # Settando as colunas e ajustando para datetime
    df.columns = ['Empresa', 'Data', 'Volume_Saida']
    df['Data'] = pd.to_datetime(df['Data'])

    return df
   
# Agrupar por empresa de origem e somar o volume de madeira que saiu de cada empresa ao longo do ano
df_saida = agrupa_e_arruma(df_2017, eh_entrada=True)
df_entrada = agrupa_e_arruma(df_2017, eh_entrada=False)

if __name__ == "__main__":
    nice_print("Coluna de data")
    print(df_entrada["Data"])