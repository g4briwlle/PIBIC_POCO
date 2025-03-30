""" Módulo de clustering de séries temporais com K-means
05
"""

from valid_time_s import *
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

num_clusters = 5 # Settando num de clusters
kmeans = KMeans(n_clusters=num_clusters) # Criando o objeto algoritmo kmeans

# Criando os rotulos de classificaçao dos clusters das series temporais de entrada e saida 
labels_entrada = kmeans.fit_predict(segundo_df_time_s_entrada)
labels_saida = kmeans.fit_predict(segundo_df_time_s_saida)

# Adicionando uma coluna com a classificação de cada série
segundo_df_time_s_saida['Cluster'] = labels_saida
segundo_df_time_s_entrada['Cluster'] = labels_entrada

# Criando plots
def plot_subclusters(entrada_ou_saida: bool):
    # Configura como as figuras estarao na interface
    fig, axes = plt.subplots(num_clusters, 1, figsize=(10, 5*num_clusters), sharex=True)   
    dict_cluster_entrada = {}
    dict_cluster_saida = {}

    # Configura a plotagem pra cada cluster
    for cluster_id in range(num_clusters):
        # Itera sobre cada gráfico da visualização (correspondente a cada cluster)
        ax = axes[cluster_id]
        # Adiciona os dados de cada um
        if entrada_ou_saida == True: # Caso o df for o de entrada
            cluster_data = segundo_df_time_s_entrada[segundo_df_time_s_entrada['Cluster'] == cluster_id]
            dict_cluster_entrada[f'Cluster {cluster_id}'] = cluster_data.index
            print('\n'*2, f'Essas são as empresas do cluster {cluster_id} respectivas ao volume de entrada: ', '\n', dict_cluster_entrada)
        else: # Caso o df for o de saida
            cluster_data = segundo_df_time_s_saida[segundo_df_time_s_saida['Cluster'] == cluster_id]
            dict_cluster_saida[f'Cluster {cluster_id}'] = cluster_data.index
            print('\n'*2, f'Essas são as empresas do cluster {cluster_id} respectivas ao volume de saida: ', '\n',dict_cluster_saida)

        # Reinicializando o dicionario
        dict_cluster_entrada.clear()
        dict_cluster_saida.clear()

        monthly_data = cluster_data.drop(columns=['Cluster'])
        monthly_data = monthly_data.T
        monthly_data.index = pd.to_datetime(monthly_data.index)
        monthly_data = monthly_data.groupby(monthly_data.index.to_period('M')).sum()
        
        # Plota as diversas séries pertencentes a um cluster no gráfico de cada um
        for serie in monthly_data.columns:
            ax.plot(monthly_data.index.astype(str), monthly_data[serie], alpha=0.7, marker='o')

        # Legenda de cada ax
        ax.set_title(f'Cluster: {cluster_id}')
        ax.set_ylabel('volume de entrada' if entrada_ou_saida else 'volume de saida')
    
    # ajustes finais do grafico    
    plt.xlabel('Mês')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    plot_subclusters(entrada_ou_saida=True)
    plot_subclusters(entrada_ou_saida=False)