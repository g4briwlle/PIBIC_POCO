""" Módulo de clustering de séries temporais com K-means
05
"""

from valid_time_s import *
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

num_clusters = 4 # Settando num de clusters
kmeans = KMeans(n_clusters=num_clusters) # Criando o objeto algoritmo kmeans

# Criando os rotulos dos clusters das series temporais de entrada e saida
labels_entrada = kmeans.fit_predict(segundo_df_time_s_entrada)
labels_saida = kmeans.fit_predict(segundo_df_time_s_saida)

# Adicionando uma coluna com a classificação de cada série em um cluster
segundo_df_time_s_saida['Cluster'] = labels_saida
segundo_df_time_s_entrada['Cluster'] = labels_entrada

# Criando plots
def plot_subclusters(entrada_ou_saida: bool):
    fig, axes = plt.subplots(num_clusters, 1, figsize=(10, 5*num_clusters), sharex=True)   
    # Configura a plotagem pra cada cluster
    for cluster_id in range(num_clusters):
        ax = axes[cluster_id]
        if entrada_ou_saida == True: # Caso o df for o de entrada
            cluster_data = segundo_df_time_s_entrada[segundo_df_time_s_entrada['Cluster'] == cluster_id].drop(columns='Cluster').T
        else: # Caso o df for o de saida
            cluster_data = segundo_df_time_s_saida[segundo_df_time_s_saida['Cluster'] == cluster_id].drop(columns='Cluster').T

        for serie in cluster_data.columns:
            ax.plot(cluster_data.index, cluster_data[serie], alpha=0.5)

        ax.set_title(f'Cluster: {cluster_id}')
        if entrada_ou_saida == True:
            ax.set_ylabel('valor de entrada')
        else: 
            ax.set_ylabel('valor de saida')

plot_subclusters(entrada_ou_saida=False)
plt.xlabel('Tempo')
plt.tight_layout()
plt.show()
