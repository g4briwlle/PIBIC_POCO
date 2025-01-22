""" Módulo de criação de séries temporais de empresas suspeitas

Para alterar as empresas plota
"""

# Ignorando warnings
import warnings
warnings.filterwarnings("ignore")

import matplotlib.pyplot as plt
from creating_df import *
from utils import *

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

        # Somando as transações de volume
        if entrada_ou_saida == 'entrada':
            df_temp_mensal = df_temp_mensal.groupby(df_temp_mensal['Data'].dt.to_period('M'))['Volume_Entrada'].sum().reset_index()
        elif entrada_ou_saida == 'saida':
            df_temp_mensal = df_temp_mensal.groupby(df_temp_mensal['Data'].dt.to_period('M'))['Volume_Saida'].sum().reset_index()
        
        # Recolocando como serie temporal de dia a dia
        df_temp_mensal['Data'] = df_temp_mensal['Data'].dt.to_timestamp()

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

# Gerar um range completo de datas para 2017 pra usar na função
full_date_range = pd.date_range(start='2017-01-01', end='2017-12-31')

# Selecionar empresas por volume de saída/entrada ao longo do ano
df_hist_cambios = pd.read_csv(r'data\historial_cambios_me_epp_solo.csv', low_memory=False)
emp_plotadas = df_hist_cambios["CPF_CNPJ_Rem"]

# Cria os df's de series temporais
df_time_s_entrada = ajusta_df_time_s('entrada', full_date_range)
df_time_s_saida = ajusta_df_time_s('saida', full_date_range)

if __name__ == "__main__":

    nice_print(" Dfs de entrada e de saida ")
    # Configurar para mostrar 367 linhas
    with pd.option_context('display.max_rows', 367): # Averiguando se as datas estao certas mesmo
        nice_print(df_time_s_entrada.head(367))
    df_time_s_entrada.info()
    nice_print(df_time_s_saida.head(14))
    df_time_s_saida.info()

    # plt.title('Volume de Madeira que Saiu das Empresas selecionadas (2017)')
    # plt.xlabel('Data')
    # plt.ylabel('Volume de Saída (m³)')
    # plt.legend(loc='upper left', bbox_to_anchor=(1,1), title="Empresas")
    # plt.xticks(rotation=45)
    # plt.tight_layout()
    # plt.show()

    # # Plotando as séries temporais de entrada ao longo do tempo para as 10 maiores empresas
    # plt.figure(figsize=(12, 6))

    # plt.title('Volume de Madeira que Entrou nas Empresas selecionadas(2017)')
    # plt.xlabel('Data')
    # plt.ylabel('Volume de Entrada (m³)')
    # plt.legend(loc='upper left', bbox_to_anchor=(1,1), title="Empresas")
    # plt.xticks(rotation=45)
    # plt.tight_layout()
    # plt.show()

    # for emp in emp_plotadas:
    #     df_temp =  df_saida[df_saida['Empresa']==emp]
    #     if not df_temp.empty:
    #         print(emp)        # aqui eu vi que eles nao tem interseção
    # print(df_saida[df_saida['Empresa']==emp_plotadas[1]])

