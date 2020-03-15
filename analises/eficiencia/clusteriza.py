"""
Clusteriza as unidades de saude e salva planilhas com a coluna CLUSTER
adicionada com os respectivos clusters encontrados.
"""

import os
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

if __name__ == '__main__':

    #
    ANO = '2018'

    # Obtem diretório raiz do projeto
    DIRETORIO_RAIZ_PROJETO = os.path.dirname(os.path.realpath(__file__))

    # Diretórios de dados e resultados
    DIRETORIO_DADOS_ORIGINAIS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'originais')
    DIRETORIO_DADOS_INTERMEDIARIOS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'intermediarios')
    DIRETORIO_DADOS_RESULTADOS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'resultados')

    # Carrega dados da planilha gerada pelo Eric
    arquivo_para_clusterizacao = os.path.join(DIRETORIO_DADOS_INTERMEDIARIOS, 'hosp_pubs_{ANO}.xlsx'.format(ANO=ANO))
    df_hosp_pubs = pd.read_excel(arquivo_para_clusterizacao, index_col='CNES')

    # Cria df somente com as colunas utilizadas na clusterização (as colunas de procedimentos iniciam na SIA-0101)
    idx_coluna_primeiro_proc = df_hosp_pubs.columns.to_list().index('SIA-0101')
    colunas_para_clust = df_hosp_pubs.columns[idx_coluna_primeiro_proc::].to_list()
    df_para_clust = df_hosp_pubs[colunas_para_clust]
    #df_para_clust = df_para_clust.set_index(df_hosp_pubs['CNES'])

    # Mostra gráfico do cotovelo para mostrar a variação da inércia de acordo com o número de clusters
    min_max_num_clusters = range(4, 30)
    inercias = []
    
    # Treina k-means para cada valor de k
    for k in min_max_num_clusters:
        modelo = KMeans(n_clusters=k, random_state=42)
        modelo.fit(df_para_clust)
        inercias.append(modelo.inertia_)

    # Plota gráfico da variação da inércia de acordo com o número de clusters
    plt.ioff()  # Desabilita o modo iterativo do matplotlib (para não mostrar os gráficos)
    plt.plot(min_max_num_clusters, inercias, '-o')
    plt.xlabel('Número de clusters')
    plt.ylabel('Inércia')
    plt.title('Gráfico do "cotovelo" para determinação do número de clusters')
    plt.grid()
    plt.xticks(min_max_num_clusters)
    arquivo_grafico_inercia = os.path.join(DIRETORIO_DADOS_RESULTADOS, 'kmeans_inercia_vs_k.png')
    plt.savefig(arquivo_grafico_inercia)

    # Número de clusters determinado pela técnica do jolho
    NUMERO_CLUSTERS = 10

    # Treina kmeans
    modelo = KMeans(n_clusters=NUMERO_CLUSTERS, random_state=42)
    modelo.fit(df_para_clust)

    # Adiciona a coluna CLUSTER ao dataframe
    df_hosp_pubs.insert(loc=1, column='CLUSTER', value=modelo.labels_)

    # Salva planilha original com a coluna CLUSTER adicionada
    arquivo_dados_basename = os.path.basename(os.path.splitext(arquivo_para_clusterizacao)[0])
    arquivo_dados_clusterizados = os.path.join(DIRETORIO_DADOS_INTERMEDIARIOS, arquivo_dados_basename + '_clusterizado.xlsx')
    df_hosp_pubs.to_excel(arquivo_dados_clusterizados, index=False)

    """
    A Criação dos clusters termina aqui. Após isto, há somente código para análise da clusterização.
    """

    # Carrega mapeamente entre códigos de subgrupos da SIGTAP e suas descrições
    arquivo_mapa_subgrupo_desc = os.path.join(DIRETORIO_DADOS_ORIGINAIS, 'sigtap_mapa_codigo_subgrupo.xlsx')
    df_mapa_subgrupo_desc = pd.read_excel(arquivo_mapa_subgrupo_desc, index_col=0)
    df_mapa_subgrupo_desc.index = ['0' + str(codigo) for codigo in df_mapa_subgrupo_desc.index]
    mapa_subgrupo_desc = dict(zip(df_mapa_subgrupo_desc.index, df_mapa_subgrupo_desc.DESCRICAO))

    # Adiciona a descrição textual ao nome das coluans de procedimentos
    novos_nomes_colunas = {}
    for coluna in df_para_clust.columns:
        codigo = coluna[4:8]
        desc = mapa_subgrupo_desc[codigo]
        novos_nomes_colunas[coluna] = coluna + ' - ' + desc

    # Renomeia colunas
    df_para_clust = df_para_clust.rename(columns=novos_nomes_colunas)

    # Dict para armazenar os centroids
    centroids = {}

    # Para cada cluster
    for k in range(0, NUMERO_CLUSTERS):

        # Obtém hospitais do cluster (Repara que no filtro, utilizamos a variável df_hosp_pubs ao invés da df_cluster)
        df_cluster = df_para_clust[df_hosp_pubs.CLUSTER == k]

        # Obtém centroid
        centroid = df_cluster.mean(axis=0)
        centroids[k] = centroid

        # Plota gráfico de barras para o centroid
        plt.ioff()  # Desabilita o modo iterativo do matplotlib (para não mostrar os gráficos)
        plt.figure(figsize=(10, 25))
        plt.title('Perfil do Cluster {k}'.format(k=k))
        plt.xlabel('Proporção por procedimento (%)')
        plt.ylabel('Procedimento')
        ax = centroid.plot(kind='barh')
        ax.grid()
        ax.set_xlim((0, 1.))
        plt.tight_layout()
        arquivo_grafico_barras_cluster = os.path.join(DIRETORIO_DADOS_INTERMEDIARIOS, 'cluster_{k}.png'.format(k=k))
        plt.savefig(arquivo_grafico_barras_cluster)

    # Salva planilha com os centroids
    df_centroids = pd.DataFrame(centroids)
    arquivo_centroids = os.path.join(DIRETORIO_DADOS_INTERMEDIARIOS, 'centroids_kmeans_{k}_clusters.xlsx'.format(k=NUMERO_CLUSTERS))
    df_centroids.to_excel(arquivo_centroids)
