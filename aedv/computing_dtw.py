""" Módulo do cálculo do fast DTW 
03

"""

import time
from tqdm import tqdm
from fastdtw import fastdtw
from scipy.spatial.distance import euclidean
from tslearn.metrics import cdist_dtw
from sklearn.cluster import DBSCAN
import pandas as pd
import numpy as np
import reading_data

tempo_inicio_total = time.time()
print(f"Iniciando execução do módulo de cálculo do fast DTW às {time.strftime('%H:%M:%S')}")

# Usando o df de emps que trocaram de nome ao longo dos 11 anos (55 emps)
# df_hist_cambios = pd.read_csv(reading_data.historial_cambios_me_epp_solo)
# emps_suspicious = df_hist_cambios[["CPF_CNPJ_Rem"]].rename(columns = {"CPF_CNPJ_Rem": "Empresa"})
    

# LOADING DF'S TO TESTS: 100, 300, 500, 1000 samples first

# Getting whole data, 4500 emps
df_time_s_entrada = pd.read_csv(reading_data.df_time_s_entrada)
df_time_s_entrada["Empresa"] = df_time_s_entrada["Empresa"].astype(str)

segundo_df_time_s_entrada = pd.read_csv(reading_data.segundo_df_time_s_entrada)
segundo_df_time_s_entrada["Empresa"] = segundo_df_time_s_entrada["Empresa"].astype(str)

# 100 samples
df_100_samples = segundo_df_time_s_entrada[:100]

# 300
df_300_samples = segundo_df_time_s_entrada[:300]

# 500
df_500_samples = segundo_df_time_s_entrada[:500]

df_800_samples = segundo_df_time_s_entrada[:800]

df_1000_samples= segundo_df_time_s_entrada[:1000]

df_1500_samples= segundo_df_time_s_entrada[:1500]


def calculate_dtw(time_series_passed: pd.DataFrame):
    """Calcula a matriz de distância DTW entre séries temporais.
    
    Parameters
    ----------
    time_series_passed : pd.DataFrame
        DataFrame com séries temporais onde cada linha é uma série
        
    Returns
    -------
    numpy.ndarray
        Matriz de distância DTW entre todas as séries
    """

    print(f"Iniciando cálculo da matriz de distância DTW para {time_series_passed.shape[0]} séries temporais...")
    tempo_inicio = time.time()

    # converting df to array to use in cdist_dtw
    series_array = time_series_passed.values.astype(float)
    # informando o usuário sobre o progresso
    print(f"Convertendo dados para formato adequado: {time.time() - tempo_inicio:.2f} segundos")
    
    # calculating cross similarity matrix com barra de progresso
    print("Calculando matriz de distância DTW (pode levar algum tempo)...")
    tempo_calculo = time.time()
    distance_matrix = cdist_dtw(series_array, )
    tempo_fim = time.time()
    
    print(f"Matriz de distância calculada em {tempo_fim - tempo_calculo:.2f} segundos")
    print(f"Tempo total da função calculate_dtw: {tempo_fim - tempo_inicio:.2f} segundos")
    
    # Informações sobre a matriz gerada
    print(f"Dimensões da matriz de distância: {distance_matrix.shape}")
    print(f"Média das distâncias: {np.mean(distance_matrix):.2f}")
    print(f"Distância mínima (excluindo diagonais): {np.min(distance_matrix + np.eye(len(distance_matrix)) * np.max(distance_matrix)):.2f}")
    print(f"Distância máxima: {np.max(distance_matrix):.2f}")
    
    return distance_matrix



# Changing part

distance_matrix1 = calculate_dtw(df_100_samples)
print(distance_matrix1)

distance_matrix2 = calculate_dtw(df_300_samples)
print(distance_matrix2)

# distance_matrix3 = calculate_dtw(df_500_samples)
# print(distance_matrix3)

"""distance_matrix4 = calculate_dtw(df_800_samples)
print(distance_matrix4)

distance_matrix5 = calculate_dtw(df_1000_samples)
print(distance_matrix5)

distance_matrix6 = calculate_dtw(df_1500_samples)
print(distance_matrix6)"""


tempo_inicio_dbscan = time.time()
dbscan_object = DBSCAN(
    eps=25,
    metric="precomputed",
    min_samples=3
)


labels1 = dbscan_object.fit_predict(distance_matrix1)
print("labels 100")
print(labels1)
# Analisando resultados
n_clusters = len(set(labels1)) - (1 if -1 in labels1 else 0)
n_noise = list(labels1).count(-1)
print(f"Número de clusters encontrados: {n_clusters}")
print(f"Número de pontos de ruído: {n_noise}")
print(f"Distribuição dos clusters: {np.bincount(labels1 + 1)}")


labels2 = dbscan_object.fit_predict(distance_matrix2)
print("labels 100")
print(labels2)
# Analisando resultados
n_clusters = len(set(labels2)) - (1 if -1 in labels2 else 0)
n_noise = list(labels2).count(-1)
print(f"Número de clusters encontrados: {n_clusters}")
print(f"Número de pontos de ruído: {n_noise}")
print(f"Distribuição dos clusters: {np.bincount(labels2 + 1)}")


"""labels3 = dbscan_object.fit_predict(distance_matrix3)
print("labels 100")
print(labels3)
# Analisando resultados
n_clusters = len(set(labels3)) - (1 if -1 in labels3 else 0)
n_noise = list(labels3).count(-1)
print(f"Número de clusters encontrados: {n_clusters}")
print(f"Número de pontos de ruído: {n_noise}")
print(f"Distribuição dos clusters: {np.bincount(labels3 + 1)}")"""


tempo_fim_dbscan = time.time()

# Tempo total de execução
tempo_fim_total = time.time()
print("\n" + "="*50)
print(f"TEMPO TOTAL DE EXECUÇÃO: {tempo_fim_total - tempo_inicio_total:.2f} segundos")
print(f"Execução concluída às {time.strftime('%H:%M:%S')}")
print("="*50)

