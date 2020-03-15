import os
import time
import pandas as pd
from pydea_wrapper import ModeloDEA


if __name__ == '__main__':

    # Obtem diretório raiz do projeto
    DIRETORIO_RAIZ_PROJETO = os.path.dirname(os.path.realpath(__file__))

    # Diretórios de dados e resultados
    DIRETORIO_DADOS_INTERMEDIARIOS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'intermediarios')
    DIRETORIO_DADOS_RESULTADOS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'resultados')

    # Carrega resultados da análise DEA por cluster
    arquivo_resultado_2018 = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'resultados', 'perfil_hosp_pubs_oss_dez_2018_clusterizado_resultado_clusterizado.xlsx')
    df_resultado = pd.read_excel(arquivo_resultado_2018)

    # Remove undiades com eficiência 'Infeasible' ou 'Unbounded'
    df_feasible = df_resultado[pd.to_numeric(df_resultado.EFICIENCIA, errors='coerce').notnull()]
    df_feasible.EFICIENCIA = df_feasible.EFICIENCIA.astype('float')

    # Substuir NANs na coluna indicando a gestão por OSS
    df_feasible.SE_GERIDA_OSS = df_feasible.SE_GERIDA_OSS.fillna('NA')

    # Calcula médias e desvios padrões por cluster
    medias = df_feasible.groupby(['CLUSTER']).EFICIENCIA.mean().to_dict()
    desvios = df_feasible.groupby(['CLUSTER']).EFICIENCIA.std().to_dict()

    # Padroniza escores por cluster
    std_scores = pd.Series([
        (e - medias[c]) / desvios[c] for c, e in zip(df_feasible.CLUSTER.to_list(), df_feasible.EFICIENCIA.to_list())
    ])

    # Normaliza eficiências para o intervalor de 0 a 1
    df_feasible.loc[:, 'EFICIENCIA_NORMALIZADA'] = ((std_scores - std_scores.min()) / (std_scores.max() - std_scores.min())).values

