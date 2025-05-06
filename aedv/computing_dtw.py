""" Módulo do cálculo do fast DTW - NÃO MAIS USADO
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
    
# Transformando em 
df_time_s_entrada = pd.read_csv(reading_data.df_time_s_entrada)
df_time_s_entrada["Empresa"] = df_time_s_entrada["Empresa"].astype(str)

segundo_df_time_s_entrada = pd.read_csv(reading_data.segundo_df_time_s_entrada)
segundo_df_time_s_entrada["Empresa"] = segundo_df_time_s_entrada["Empresa"].astype(str)
# emps_suspicious = emps_suspicious.astype(int)

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
    distance_matrix = cdist_dtw(series_array)
    tempo_fim = time.time()
    
    print(f"Matriz de distância calculada em {tempo_fim - tempo_calculo:.2f} segundos")
    print(f"Tempo total da função calculate_dtw: {tempo_fim - tempo_inicio:.2f} segundos")
    
    # Informações sobre a matriz gerada
    print(f"Dimensões da matriz de distância: {distance_matrix.shape}")
    print(f"Média das distâncias: {np.mean(distance_matrix):.2f}")
    print(f"Distância mínima (excluindo diagonais): {np.min(distance_matrix + np.eye(len(distance_matrix)) * np.max(distance_matrix)):.2f}")
    print(f"Distância máxima: {np.max(distance_matrix):.2f}")
    
    return distance_matrix

distance_matrix = calculate_dtw(segundo_df_time_s_entrada)
print(distance_matrix, distance_matrix)

tempo_inicio_dbscan = time.time()
dbscan_object = DBSCAN(
    eps=25,
    metric="precomputed",
    min_samples=3
)

labels = dbscan_object.fit_predict(distance_matrix)
print(labels)
tempo_fim_dbscan = time.time()

# Analisando resultados
n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
n_noise = list(labels).count(-1)
print(f"Número de clusters encontrados: {n_clusters}")
print(f"Número de pontos de ruído: {n_noise}")
print(f"Distribuição dos clusters: {np.bincount(labels + 1)}")

# Tempo total de execução
tempo_fim_total = time.time()
print("\n" + "="*50)
print(f"TEMPO TOTAL DE EXECUÇÃO: {tempo_fim_total - tempo_inicio_total:.2f} segundos")
print(f"Execução concluída às {time.strftime('%H:%M:%S')}")
print("="*50)

