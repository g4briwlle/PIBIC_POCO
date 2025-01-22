from fastdtw import fastdtw
from scipy.spatial.distance import euclidean
import pandas as pd
import numpy as np
from valid_time_s import *

# Usando o df de emps que trocaram de nome ao longo dos 11 anos (55 emps +/-)
df_hist_cambios = pd.read_csv(r'data\historial_cambios_me_epp_solo.csv')
emps_suspicious = df_hist_cambios["CPF_CNPJ_Rem"]
emps_suspicious.rename(columns = {"CPF_CNPJ_Rem": "Empresa"})

df_filtrado_entrada = df_entrada[df_entrada['Empresa'].isin(emps_suspicious)]
    
df_time_s_entrada["Empresa"] = df_time_s_entrada["Empresa"].astype(int)
emps_suspicious = emps_suspicious.astype(int)

distancias = []
for index, emp1 in enumerate(emps_suspicious):
    if index < len(emps_suspicious) - 1:
        for emp2 in emps_suspicious[index + 1:]:
            if emp1 != emp2:
                serie_emp1 = df_time_s_entrada[df_time_s_entrada["Empresa"] == emp1]["Volume_Saida"].values
                serie_emp2 = df_time_s_entrada[df_time_s_entrada["Empresa"] == emp2]["Volume_Saida"].values
                serie_emp1 = serie_emp1.reshape(-1, 1)
                serie_emp2 = serie_emp2.reshape(-1, 1)
                if serie_emp1.size > 0 and serie_emp2.size > 0:
                    # Mantém como arrays NumPy
                    distance, path = fastdtw(serie_emp1.reshape(-1, 1), serie_emp2.reshape(-1, 1), dist=euclidean)
                    distancias.append(distance)
print(distancias)