""" Módulo de criação de séries temporais de empresas suspeitas
02

Transformação de df_saida/entrada não ordenado para séries temporais válidas
O formato do df é tal que: cada 365 linhas são os dias 
Pode-se alterar as empresas selecionadas para visualização nos dfs no final do código, bem como o date_range dos dfs
"""

# Ignorando warnings
import warnings
warnings.filterwarnings("ignore")

import reading_data
import matplotlib.pyplot as plt
from creating_df import *
from utils import *
import pathlib 

hist_cambios_path = reading_data.DATA_DIR / 'historial_cambios_me_epp_solo.csv'

def ajusta_df_time_s(entrada_ou_saida: str, full_date_range: pd.DatetimeIndex) -> pd.DataFrame:
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

    # Itera emps selecionadas para ajuste de df de series temporais
    for index, empresa in enumerate(emp_plotadas):
        empresa = str(empresa)
        # Inicializando os df_temp_mensal de acordo com a especificação
        if entrada_ou_saida == 'entrada':
            df_temp_mensal = df_entrada[df_entrada['Empresa'] == empresa]
            df_temp_mensal.columns = ['Empresa', 'Data', 'Volume_Entrada']
        elif entrada_ou_saida == 'saida':
            df_temp_mensal = df_saida[df_saida['Empresa'] == empresa]
            df_temp_mensal.columns = ['Empresa', 'Data', 'Volume_Saida']

        # Converter pra datetime pra usar no groupby
        df_temp_mensal['Data'] = pd.to_datetime(df_temp_mensal['Data'])

        # De acordo com a indexação de data, incluo o valor 0 para dias sem transação em todo o 2017 e depois 
        # reseto o indice para o normal
        df_temp_mensal = df_temp_mensal.set_index('Data').reindex(full_date_range, fill_value=0).reset_index()
        
        # Rechamando a coluna index de Data porque isso se alterou ali em cima
        df_temp_mensal = df_temp_mensal.rename(columns={'index': 'Data'})

        # Colocando de volta a empresa como elemento da coluna 'Empresa'
        df_temp_mensal["Empresa"] = empresa
        
        # Criar o df de series temporais que sera usado, contendo todas as series temps de todas as empresas
        if index == 0:
            df_time_s = df_temp_mensal
        else:
            df_time_s = pd.concat([df_time_s, df_temp_mensal], axis=0, ignore_index=True)               

    return df_time_s

def segundo_ajuste_df_time_s(entrada_ou_saida: bool, full_date_range: pd.DatetimeIndex) -> pd.DataFrame:
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

    # Itera sobre as emps selecionadas para colocar seus dados no df
    if entrada_ou_saida == True: # Caso o df seja o de volume de entrada
        df_segundo_ajuste = df_time_s_entrada.pivot(index='Empresa', columns='Data', values='Volume_Entrada')
    else: # Caso o df seja o de volume de saída
        df_segundo_ajuste = df_time_s_saida.pivot(index='Empresa', columns='Data', values='Volume_Saida')

    return df_segundo_ajuste

# Gerar um range completo de datas para 2017 pra usar na função
full_date_range = pd.date_range(start='2017-01-01', end='2017-12-31')

# Selecionar empresas por volume de saída/entrada ao longo do ano
df_hist_cambios = pd.read_csv(hist_cambios_path, low_memory=False)
emp_plotadas = df_hist_cambios["CPF_CNPJ_Rem"]

# Cria os df's de series temporais
df_time_s_entrada = ajusta_df_time_s('entrada', full_date_range)
df_time_s_saida = ajusta_df_time_s('saida', full_date_range)

# Cria os df's pivoteados (ajustados pela segunda vez)
segundo_df_time_s_entrada = segundo_ajuste_df_time_s(entrada_ou_saida=True, full_date_range=full_date_range)
segundo_df_time_s_saida = segundo_ajuste_df_time_s(entrada_ou_saida=False, full_date_range=full_date_range)

if __name__ == "__main__":
    # Configurar para mostrar 367 linhas
    with pd.option_context('display.max_rows', 100): # Averiguando se as datas estao certas mesmo
        print('df time s saida','\n', df_time_s_saida.head(385), '\n')
    df_time_s_entrada.info()
    print('df time s saida', '\n', df_time_s_saida.head(14), '\n')
    df_time_s_saida.info()
    print('\n', 'segundo df time saida', segundo_df_time_s_saida.head(14), '\n')
    segundo_df_time_s_saida.info()
    print('\n', 'segundo df time entrada', segundo_df_time_s_entrada.head(14), '\n')
    segundo_df_time_s_entrada.info()