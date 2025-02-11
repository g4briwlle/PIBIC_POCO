""" Módulo do cálculo da série temporal 'média'
04 

"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.seasonal import seasonal_decompose
from valid_time_s import ajusta_df_time_s

import numpy as np
from tslearn.barycenters import dtw_barycenter_averaging
from valid_time_s import df_time_s_entrada, df_time_s_saida

from utils import *

# Define datas como o mesmo formato: YYYY-MM-DD
def arruma_data(df):
    df["Data"] = df["Data"].dt.strftime(r'%Y-%m-%d')
    df['Data'] = pd.to_datetime(df['Data'])
    return df

# Nomes de todas as empresas
lista_empresas = list(df_time_s_entrada["Empresa"].unique())

df_mensal = arruma_data(df_time_s_entrada) # Agora o df_time_s_entrada ja vem com esse formato

# Agrupe por empresa e mês, somando os valores de entrada para cada mês
df_mensal = df_time_s_entrada.groupby([df_time_s_entrada['Data'].dt.to_period('M'), 'Empresa'])['Volume_Entrada'].sum().reset_index()

# Converta a coluna `Data` de volta para datetime para usar no pivoteamento
df_mensal['Data'] = df_mensal['Data'].astype(str)

# Pivoteando dataframe.
df_pivot = df_mensal.pivot(index='Empresa', columns='Data', values='Volume_Entrada')

# Colocando apenas os valore em uma matriz
entradas_matriz = df_pivot.values

print(df_mensal.head(1000))
print(df_pivot)

# Fazendo barycenter averaging
barycenter = dtw_barycenter_averaging(entradas_matriz)

# Plotando
plt.plot(barycenter, label="Barycenter Averaging")
plt.legend()
plt.title("DBA Barycenter Averaging de Séries Temporais")
plt.show()


nice_print(f"Olhando empresa: {20825514000193}")
print("oi")
print(df_time_s_entrada[df_time_s_entrada["Empresa"] == "20825514000193"]["Volume_Entrada"])