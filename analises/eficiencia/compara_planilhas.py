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


    #
    arquivo_eric = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'originais', 'dados_eric_completos', 'Planilha_DEA_Cluster_2018.csv')
    df_eric = pd.read_csv(arquivo_eric, sep=';', decimal=',')

    arquivo_marcelo = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'originais', 'unidades_perfil_todos_dez_2018.csv')
    df_marcelo = pd.read_csv(arquivo_marcelo, sep=';', decimal=',')