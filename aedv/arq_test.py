import pandas as pd
import numpy as np
from tslearn.metrics import cdist_dtw

# Exemplo: 5 empresas ao longo de 10 períodos
df_series = pd.DataFrame({
    "Empresa": ["E1", "E2", "E3", "E4", "E5"],
    "2010": [100, 90, 50, 300, 400],
    "2011": [110, 85, 55, 310, 420],
    "2012": [120, 88, 53, 290, 430],
    "2013": [115, 80, 60, 280, 410],
})

df_series.set_index("Empresa", inplace=True)
print(df_series.head())

# Cada linha vira uma série temporal
series_array = df_series.values.astype(float)  # shape: (n_series, time_length)
print(series_array)
print('')

# Matriz de distância DTW (n_series x n_series)
distance_matrix = cdist_dtw(series_array)
print(distance_matrix)

from sklearn.cluster import DBSCAN

dbscan = DBSCAN(eps=100, min_samples=2, metric="precomputed")
labels = dbscan.fit_predict(distance_matrix)

print("Clusters:", labels)