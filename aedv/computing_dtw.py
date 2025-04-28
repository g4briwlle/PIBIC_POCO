""" Módulo do cálculo do fast DTW - NÃO MAIS USADO
03

"""

from fastdtw import fastdtw
from scipy.spatial.distance import euclidean
from tslearn.metrics import cdist_dtw
from sklearn.cluster import DBSCAN
import pandas as pd
import numpy as np
import reading_data
from valid_time_s import *

# Usando o df de emps que trocaram de nome ao longo dos 11 anos (55 emps)
df_hist_cambios = pd.read_csv(reading_data.historial_cambios_me_epp_solo)
emps_suspicious = df_hist_cambios[["CPF_CNPJ_Rem"]].rename(columns = {"CPF_CNPJ_Rem": "Empresa"})
    
df_time_s_entrada["Empresa"] = df_time_s_entrada["Empresa"].astype(int)
emps_suspicious = emps_suspicious.astype(int)

def calculate_dtw(time_series_passed: pd.DataFrame):
    # converting df to array to use in cdist_dtw
    series_array = time_series_passed.values.astype(float)
    # calculating cross similarity matrix
    distance_matrix = cdist_dtw(series_array)
    return distance_matrix

distance_matrix = calculate_dtw(segundo_df_time_s_entrada)
print(distance_matrix, distance_matrix)

dbscan_object = DBSCAN(
    eps=25,
    metric="precomputed",
    min_samples=3
)

labels = dbscan_object.fit_predict(distance_matrix)
print(labels)

