# # Usando .get() para acessar um dicionário aninhado
# dict_exemplo = {
#     "produto2": {"na": 1}
# }

# print(dict_exemplo.get('produto2', {}).get('nome', 'Nome não disponível'))  # Resultado: Camiseta

# # Se o 'produto1' ou a chave 'nome' não existirem, ele retorna o valor padrão 'Nome não disponível'


# import matplotlib.pyplot as plt
# import networkx as nx

# G = nx.DiGraph()

# # Adicionando nós e arestas
# G.add_edge(1, 2)
# G.add_edge(2, 3)
# G.add_edge(3, 1)

# # Desenhando o grafo
# nx.draw(G, with_labels=True, node_color='lightblue', font_weight='bold')
# plt.show()
import pandas as pd

emps = pd.read_csv("./isso.csv")

emp_type = {}

for i,node in emps.iterrows():
  emp_type[node['id_emp']] = node['type']

print(emp_type)