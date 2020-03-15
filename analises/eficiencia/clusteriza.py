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
    ANO = '2019'

    # Obtem diretório raiz do projeto
    DIRETORIO_RAIZ_PROJETO = os.path.dirname(os.path.realpath(__file__))

    # Diretórios de dados e resultados
    DIRETORIO_DADOS_ORIGINAIS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'originais')
    DIRETORIO_DADOS_INTERMEDIARIOS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'intermediarios')
    DIRETORIO_DADOS_RESULTADOS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'resultados')

    # Carrega dados da planilha gerada pelo Eric
    arquivo_para_clusterizacao = os.path.join(DIRETORIO_DADOS_INTERMEDIARIOS, 'perfil_hosp_pubs_oss_dez_{ANO}.xlsx'.format(ANO=ANO))
    df_hosp_pubs_oss_com_perfil = pd.read_excel(arquivo_para_clusterizacao)

    # Cria df somente com as colunas utilizadas na clusterização (os subrupos começam na coluna 13)
    colunas_para_clust = df_hosp_pubs_oss_com_perfil.columns[13::].to_list()
    df_para_clust = df_hosp_pubs_oss_com_perfil[colunas_para_clust]
    df_para_clust = df_para_clust.set_index(df_hosp_pubs_oss_com_perfil['CNES'])

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
    plt.xticks(min_max_num_clusters)
    arquivo_grafico_inercia = os.path.join(DIRETORIO_DADOS_RESULTADOS, 'kmenas_inercia_vs_k.png')
    plt.savefig(arquivo_grafico_inercia)

    # Número de clusters determinado pela técnica do jolho
    NUMERO_CLUSTERS = 10

    # Treina kmeans
    modelo = KMeans(n_clusters=NUMERO_CLUSTERS, random_state=42)
    modelo.fit(df_para_clust)

    # Adiciona os clusters ao dataframe
    df_hosp_pubs_oss_com_perfil['CLUSTER'] = modelo.labels_
    df_para_clust['CLUSTER'] = modelo.labels_ # TODO: remover uma destas variáveis

    # Salva planilha original com a coluna CLUSTER adicionar com os respectivos clusters
    arquivo_dados_basename = os.path.basename(os.path.splitext(arquivo_para_clusterizacao)[0])
    arquivo_dados_clusterizados = os.path.join(DIRETORIO_DADOS_INTERMEDIARIOS, arquivo_dados_basename + '_clusterizado.xlsx')
    df_hosp_pubs_oss_com_perfil.to_excel(arquivo_dados_clusterizados, index=False)

    """
    A Criação dos clusters termina aqui. Após isto, há somente código para análise da clusterização.
    """

    # Carrega mapeamente entre códigos de subgrupos da SIGTAP e suas descrições
    arquivo_mapa_subgrupo_desc = os.path.join(DIRETORIO_DADOS_ORIGINAIS, 'sigtap_mapa_codigo_subgrupo.xlsx')
    df_mapa_subgrupo_desc = pd.read_excel(arquivo_mapa_subgrupo_desc, index_col=0)
    df_mapa_subgrupo_desc.index = ['0' + str(codigo) for codigo in df_mapa_subgrupo_desc.index]
    mapa_subgrupo_desc = dict(zip(df_mapa_subgrupo_desc.index, df_mapa_subgrupo_desc.DESCRICAO))

    # Para cada coluna com código de subgrupo, i.e. da 11 até a penúltima (que é a com o id do cluster)
    novos_nomes_colunas = {}
    for coluna in df_hosp_pubs_oss_com_perfil.columns[12:-1]:

        # Cria nomvo nome para a coluna, incluindo código e descrição do subgrupo de procedimentos
        sistema = 'SIH' if coluna[0] == '1' else 'SIA'
        codigo = coluna[4:8]
        desc = mapa_subgrupo_desc[codigo]
        novos_nomes_colunas[coluna] = sistema + '-' + codigo + ' - ' + desc

    # Renomeia colunas
    df_para_clust = df_para_clust.rename(columns=novos_nomes_colunas)

    # Dict para armazenar os centroids
    centroids = {}

    # Para cada cluster
    for k in range(0, NUMERO_CLUSTERS):

        # Obtém hospitais do cluster  (exclui 1ª e última coluna (CNES e Cluster))
        df_cluster = df_para_clust[df_para_clust.CLUSTER == k][df_para_clust.columns[1:-1]]

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
        arquivo_grafico_barras_cluster = os.path.join(DIRETORIO_DADOS_RESULTADOS, 'cluster_{k}.png'.format(k=k))
        plt.savefig(arquivo_grafico_barras_cluster)

    # Salva planilha com os centroids
    df_centroids = pd.DataFrame(centroids)
    arquivo_centroids = os.path.join(DIRETORIO_DADOS_RESULTADOS, 'centroids_kmeans_{k}_clusters.xlsx'.format(k=NUMERO_CLUSTERS))
    df_centroids.to_excel(arquivo_centroids)
