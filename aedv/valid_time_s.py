""" Módulo de criação de séries temporais com uma seleção de empresas desejada a partir dos datasets iniciais
02

Transformação de df_saida/entrada não ordenado para séries temporais válidas de duas maneiras, dado que a segunda só é atingida após fazer a transformação na primeira.

1ª: cada 365 linhas são os dias do ano de uma empresa, as proximas 365 sao de outra empresa
2ª: pivoteamento tal que tem 365 linhas, os dias, e cada linha é uma empresa
Pode-se alterar as empresas selecionadas para visualização nos dfs no final do código, bem como o date_range dos dfs
"""

# Ignorando warnings
import warnings
warnings.filterwarnings("ignore")

import time
from tqdm import tqdm
import reading_data
import matplotlib.pyplot as plt
from creating_df_2017 import df_entrada, df_saida
import pandas as pd
from utils import *
import pathlib 

start = time.time()

hist_cambios_path = reading_data.historial_cambios_me_epp_solo

def ajusta_df_time_s(empresas_lista: list , entrada_ou_saida: str, full_date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Ajusta o DataFrame `df_entrada` ou `df_saida` para representar séries temporais diárias de transações de madeira
    para cada empresa especificada.

    Esta função organiza os dados de entrada ou saída de madeira de cada empresa em linhas diárias no ano de 2017, preenchendo dias sem transações com zero.

    Parameters
    ----------
    entrada_ou_saida : str
        Especifica o tipo de transação, 'entrada' para entrada de madeira ou 'saida' para saída de madeira.

    Returns
    -------
    pandas.DataFrame
        Um DataFrame contendo as séries temporais diárias de volume de madeira para cada empresa, com as colunas:
        - 'Data' : datetime64
            Datas de 2017, incluindo dias sem transações, preenchidos com zero.
        - 'Empresa' : str
            Identificação da empresa associada a cada série temporal.
        - 'Volume_Entrada' ou 'Volume_Saida' : float
            Volume de madeira (entrada ou saída) agregado mensalmente e distribuído em intervalos diários.

    Notes
    -----
    - O DataFrame retornado incluirá zero para todos os dias em que não houve transação.
    - As colunas de volume variam dependendo do tipo especificado em `entrada_ou_saida`.
    - `full_date_range` deve ser definido previamente, cobrindo todas as datas de 2017.
    - `emp_plotadas`, `df_entrada` e `df_saida` também devem estar disponíveis no escopo da função.
    """

    # Lista dos dfs temporarios mensais para concatenar apenas no final
    dfs = []

    # Criando um df filtrado para a iteração nele
    if entrada_ou_saida == 'entrada':
        df_filtrado = df_entrada[df_entrada['Empresa'].isin(empresas_lista)].copy()
        df_filtrado.columns = ['Empresa', 'Data', 'Volume_Entrada']
    elif entrada_ou_saida == 'saida':
        df_filtrado = df_saida[df_saida['Empresa'].isin(empresas_lista)].copy()
        df_filtrado.columns = ['Empresa', 'Data', 'Volume_Saida']
    else:
        raise ValueError("entrada_ou_saida deve ser 'entrada' ou 'saida'")
    
    # Verificação se existem empresas encontradas
    if len(df_filtrado) == 0:
        print(f"Aviso: Nenhuma empresa encontrada no DataFrame de {entrada_ou_saida}.")
        print(f"Primeiras 10 empresas na lista: {empresas_lista[:10]}")
        print(f"Primeiras 10 empresas no df_{entrada_ou_saida}: {df_saida['Empresa'].head(10).tolist() if entrada_ou_saida == 'saida' else df_entrada['Empresa'].head(10).tolist()}")
        return pd.DataFrame()  # Retorna DataFrame vazio

    # Converter pra datetime pra usar no groupby (verificando se já não é datetime)
    if not pd.api.types.is_datetime64_any_dtype(df_filtrado['Data']):
        df_filtrado['Data'] = pd.to_datetime(df_filtrado['Data'])

    # Contando as empresas encontradas vs. solicitadas
    empresas_encontradas = df_filtrado['Empresa'].nunique()
    print(f"Empresas solicitadas: {len(empresas_lista)}, Empresas encontradas: {empresas_encontradas}")
    # Imprime algumas amostras para verificação
    print(f"Amostra de dados filtrados para {entrada_ou_saida}:")
    print(df_filtrado.head())

    # Itera emps selecionadas para ajuste de df de series temporais
    for empresa, df_empresa in tqdm(df_filtrado.groupby("Empresa"), desc=f"Processando empresas ({entrada_ou_saida})"):

        # Verificar e remover duplicatas de Data antes de usar como índice
        # Se houver duplicatas, somamos os volumes para cada data
        if df_empresa.duplicated('Data').any():
            print(f"Aviso: Encontradas datas duplicadas para empresa {empresa}. Somando volumes.")
            if entrada_ou_saida == 'entrada':
                df_empresa = df_empresa.groupby('Data')['Volume_Entrada'].sum().reset_index()
            else:
                df_empresa = df_empresa.groupby('Data')['Volume_Saida'].sum().reset_index()

        # De acordo com a indexação de data, incluo o valor 0 para dias sem transação em todo o 2017 e depois 
        # reseto o indice para o normal
        df_empresa = df_empresa.set_index('Data').reindex(full_date_range, fill_value=0).reset_index()
        
        # Rechamando a coluna index de Data porque isso se alterou ali em cima
        df_empresa = df_empresa.rename(columns={'index': 'Data'})

        # Colocando de volta a empresa como elemento da coluna 'Empresa'
        df_empresa["Empresa"] = empresa
        
        # Adicionando df temporario a lista de dfs temporarios
        dfs.append(df_empresa)           

    # Se não encontrou nenhuma empresa, retorna DataFrame vazio
    if not dfs:
        print(f"Nenhum dado processado para {entrada_ou_saida}.")
        return pd.DataFrame()
    
    # Transformando dfs temporarios em df grande definitivo
    df_time_s = pd.concat(dfs, axis=0, ignore_index=True) 
    return df_time_s

def segundo_ajuste_df_time_s(df_time_s_passed:pd.DataFrame, entrada_ou_saida: bool, full_date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """Realiza um segundo ajuste no DataFrame de séries temporais, transformando-o em um formato pivoteado, onde as datas se tornam colunas.

    Esta função reorganiza os dados de entrada ou saída de madeira, estruturando o DataFrame de modo que cada linha represente uma empresa e cada coluna (exceto a primeira) corresponda a um dia do ano de 2017.

    Parameters
    ----------
    entrada_ou_saida : bool
        Define se o ajuste será feito para o DataFrame de entrada de madeira (True) ou para o de saída de madeira (False).
    full_date_range : pd.DatetimeIndex
        Índice de datas cobrindo todo o ano de 2017, utilizado para garantir consistência no pivoteamento.

    Returns
    -------
    pandas.DataFrame
        Um DataFrame onde:
        - O índice representa as empresas associadas às transações.
        - As colunas representam os dias do ano no formato datetime.
        - Os valores indicam o volume diário de madeira movimentado por cada empresa.
    """

    # Verificar se o DataFrame está vazio
    if df_time_s_passed.empty:
        print(f"DataFrame vazio passado para segundo_ajuste_df_time_s. Retornando DataFrame vazio.")
        return pd.DataFrame()
    
    # Verificar as colunas antes do pivoteamento
    expected_cols = ['Empresa', 'Data']
    expected_vals = ['Volume_Entrada'] if entrada_ou_saida else ['Volume_Saida']
    for col in expected_cols + expected_vals:
        if col not in df_time_s_passed.columns:
            print(f"Erro: Coluna {col} não encontrada no DataFrame. Colunas disponíveis: {df_time_s_passed.columns.tolist()}")
            return pd.DataFrame()

    # Itera sobre as emps selecionadas para colocar seus dados no df
    if entrada_ou_saida == True: # Caso o df seja o de volume de entrada
        df_segundo_ajuste = df_time_s_passed.pivot(index='Empresa', columns='Data', values='Volume_Entrada')
    else: # Caso o df seja o de volume de saída
        df_segundo_ajuste = df_time_s_passed.pivot(index='Empresa', columns='Data', values='Volume_Saida')

    return df_segundo_ajuste

# Gerar um range completo de datas para 2017 pra usar na função
full_date_range = pd.date_range(start='2017-01-01', end='2017-12-31')

# Selecionar empresas por volume de saída/entrada ao longo do ano
df_hist_cambios = pd.read_csv(hist_cambios_path, low_memory=False)


# Converter CPF/CNPJ para string e garantir o preenchimento com zeros à esquerda
empresas_cambio = df_hist_cambios["CPF_CNPJ_Rem"].astype(str).str.zfill(14).unique().tolist()
print(f"Total de empresas únicas no df_hist_cambios: {len(empresas_cambio)}")
print(f"Primeiras 5 empresas: {empresas_cambio[:5]}")

# Verificar se há valores inválidos ou suspeitos nos CNPJs
def verificar_cnpj(cnpj_str):
    # Verificar se tem comprimento esperado
    if len(cnpj_str) != 14:
        return False
    # Verificar se é numérico
    if not cnpj_str.isdigit():
        return False
    return True

empresas_validas = [emp for emp in empresas_cambio if verificar_cnpj(emp)]
empresas_invalidas = [emp for emp in empresas_cambio if not verificar_cnpj(emp)]

if empresas_invalidas:
    print(f"Aviso: Encontrados {len(empresas_invalidas)} CNPJs com formato inválido:")
    print(empresas_invalidas[:5], "..." if len(empresas_invalidas) > 5 else "")

# Usar apenas CNPJs válidos
empresas_cambio = empresas_validas

# Primeiro, vamos verificar se essas empresas existem nos dataframes de entrada/saída
empresas_em_entrada = set(df_entrada['Empresa'].astype(str).str.zfill(14))
empresas_em_saida = set(df_saida['Empresa'].astype(str).str.zfill(14))
empresas_cambio_set = set(empresas_cambio)

print(f"Total de empresas em df_entrada: {len(empresas_em_entrada)}")
print(f"Total de empresas em df_saida: {len(empresas_em_saida)}")
print(f"Empresas de cambio em df_entrada: {len(empresas_cambio_set.intersection(empresas_em_entrada))}")
print(f"Empresas de cambio em df_saida: {len(empresas_cambio_set.intersection(empresas_em_saida))}")


# CORREÇÃO 1: Garantir que estamos trabalhando com o mesmo formato de CPF/CNPJ
# Convertendo todos para string com preenchimento de zeros
df_entrada['Empresa'] = df_entrada['Empresa'].astype(str).str.zfill(14)
df_saida['Empresa'] = df_saida['Empresa'].astype(str).str.zfill(14)


# PRA EMPRESAS DE CAMBIO
# Cria os df's de series temporais das empresas de cambio
df_time_s_entrada_cambios = ajusta_df_time_s(empresas_cambio, 'entrada', full_date_range)
df_time_s_saida_cambios = ajusta_df_time_s(empresas_cambio, 'saida', full_date_range)
# Cria os df's pivoteados (ajustados pela segunda vez)
segundo_df_time_s_entrada_cambios = segundo_ajuste_df_time_s(df_time_s_entrada_cambios, True, full_date_range)
segundo_df_time_s_saida_cambios = segundo_ajuste_df_time_s(df_time_s_saida_cambios, False, full_date_range)


# CORREÇÃO 2: Para todas as empresas, usar a lista completa de empresas de cada dataframe
todas_empresas_entrada = df_entrada['Empresa'].unique().tolist()
todas_empresas_saida = df_saida['Empresa'].unique().tolist()
print(f"Total de empresas únicas em df_entrada: {len(todas_empresas_entrada)}")
print(f"Total de empresas únicas em df_saida: {len(todas_empresas_saida)}")


# PRA TODAS AS EMPRESAS
# Cria os df's de series temporais de todas as empresas
df_time_s_entrada = ajusta_df_time_s(todas_empresas_entrada, 'entrada', full_date_range)
df_time_s_saida = ajusta_df_time_s(todas_empresas_saida, 'saida', full_date_range)
# Cria os df's pivoteados (ajustados pela segunda vez)
segundo_df_time_s_entrada = segundo_ajuste_df_time_s(df_time_s_entrada, True, full_date_range)
segundo_df_time_s_saida = segundo_ajuste_df_time_s(df_time_s_saida, False, full_date_range)

# Transformando resultados em .csv
df_time_s_entrada.to_csv(reading_data.DATA_DIR / "df_time_s_entrada.csv")
df_time_s_saida.to_csv(reading_data.DATA_DIR / "df_time_s_saida.csv")
segundo_df_time_s_entrada.to_csv(reading_data.DATA_DIR / "segundo_df_time_s_entrada.csv")
segundo_df_time_s_saida.to_csv(reading_data.DATA_DIR / "segundo_df_time_s_saida.csv")


end = time.time()

if __name__ == "__main__":
    print(f"Tempo de execução: {end - start:.2f} segundos")
    
    # Verificar resultados das empresas de cambio
    print("\n===== Resultados para Empresas de Cambio =====")
    if not df_time_s_entrada_cambios.empty:
        print('df_time_s_entrada_cambios', '\n', df_time_s_entrada_cambios.head(5))
        print(f"Total de linhas: {len(df_time_s_entrada_cambios)}")
        print(f"Empresas únicas: {df_time_s_entrada_cambios['Empresa'].nunique()}")
        # Verifica se existem volumes diferentes de zero
        print(f"Volumes > 0: {(df_time_s_entrada_cambios['Volume_Entrada'] > 0).sum()}")
    else:
        print("df_time_s_entrada_cambios está vazio")
    
    print("\n" + "*"*30)
    
    if not df_time_s_saida_cambios.empty:
        print('df_time_s_saida_cambios', '\n', df_time_s_saida_cambios.head(5))
        print(f"Total de linhas: {len(df_time_s_saida_cambios)}")
        print(f"Empresas únicas: {df_time_s_saida_cambios['Empresa'].nunique()}")
        # Verifica se existem volumes diferentes de zero
        print(f"Volumes > 0: {(df_time_s_saida_cambios['Volume_Saida'] > 0).sum()}")
    else:
        print("df_time_s_saida_cambios está vazio")
    
    print("\n===== Resultados para Todas as Empresas =====")
    if not df_time_s_entrada.empty:
        print('df_time_s_entrada', '\n', df_time_s_entrada.head(5))
        print(f"Total de linhas: {len(df_time_s_entrada)}")
        print(f"Empresas únicas: {df_time_s_entrada['Empresa'].nunique()}")
        # Verifica se existem volumes diferentes de zero
        print(f"Volumes > 0: {(df_time_s_entrada['Volume_Entrada'] > 0).sum()}")
    else:
        print("df_time_s_entrada está vazio")
    
    print("\n" + "*"*30)
    
    if not df_time_s_saida.empty:
        print('df_time_s_saida', '\n', df_time_s_saida.head(5))
        print(f"Total de linhas: {len(df_time_s_saida)}")
        print(f"Empresas únicas: {df_time_s_saida['Empresa'].nunique()}")
        # Verifica se existem volumes diferentes de zero
        print(f"Volumes > 0: {(df_time_s_saida['Volume_Saida'] > 0).sum()}")
    else:
        print("df_time_s_saida está vazio")
    
    print("\n===== Resultados para DataFrames Pivoteados =====")
    if not segundo_df_time_s_entrada_cambios.empty:
        print('segundo_df_time_s_entrada_cambios', '\n', segundo_df_time_s_entrada_cambios.head(5))
        print(f"Shape: {segundo_df_time_s_entrada_cambios.shape}")
        # Verifica se existem volumes diferentes de zero
        print(f"Células > 0: {(segundo_df_time_s_entrada_cambios > 0).sum().sum()}")
    else:
        print("segundo_df_time_s_entrada_cambios está vazio")
    
    print("\n" + "*"*30)

    if not segundo_df_time_s_saida_cambios.empty:
        print('segundo_df_time_s_saida_cambios', '\n', segundo_df_time_s_saida_cambios.head(5))
        print(f"Shape: {segundo_df_time_s_saida_cambios.shape}")
        # Verifica se existem volumes diferentes de zero
        print(f"Células > 0: {(segundo_df_time_s_saida_cambios > 0).sum().sum()}")
    else:
        print("segundo_df_time_s_saida_cambios está vazio")
    
    print("\n" + "*"*30)

    if not segundo_df_time_s_entrada.empty:
        print('segundo_df_time_s_entrada', '\n', segundo_df_time_s_entrada.head(5))
        print(f"Shape: {segundo_df_time_s_entrada.shape}")
        # Verifica se existem volumes diferentes de zero
        print(f"Células > 0: {(segundo_df_time_s_entrada > 0).sum().sum()}")
    else:
        print("segundo_df_time_s_entrada está vazio")

    print("\n" + "*"*30)
    
    if not segundo_df_time_s_saida.empty:
        print('segundo_df_time_s_saida', '\n', segundo_df_time_s_saida.head(5))
        print(f"Shape: {segundo_df_time_s_saida.shape}")
        # Verifica se existem volumes diferentes de zero
        print(f"Células > 0: {(segundo_df_time_s_saida > 0).sum().sum()}")
    else:
        print("segundo_df_time_s_saida está vazio")
    