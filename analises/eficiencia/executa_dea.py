"""
TODO:
"""


import os
import time
import pandas as pd
from pydea_wrapper import ModeloDEA


if __name__ == '__main__':

    t0 = time.time()

    # Obtem diretório raiz do projeto
    DIRETORIO_RAIZ_PROJETO = os.path.dirname(os.path.realpath(__file__))

    # Diretórios de dados e resultados
    DIRETORIO_DADOS_INTERMEDIARIOS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'intermediarios')
    DIRETORIO_DADOS_RESULTADOS = os.path.join(DIRETORIO_RAIZ_PROJETO, 'dados', 'resultados')

    #
    ANO = 2019

    # Parâmetros do modelo do TCU
    input_categories = ['CNES_LEITOS_SUS', 'CNES_MEDICOS', 'CNES_PROFISSIONAIS_ENFERMAGEM', 'CNES_SALAS']
    output_categories = ['SIA_SIH_VALOR']
    virtual_weight_restrictions = ['CNES_SALAS >= 0.09', 'CNES_PROFISSIONAIS_ENFERMAGEM >= 0.09',
                                   'CNES_LEITOS_SUS >= 0.16', 'CNES_MEDICOS >= 0.16']

    # Instancia modelo
    modelo_tcu = ModeloDEA(input_categories, output_categories, virtual_weight_restrictions)

    # Carrega planilha para DEA
    arquivo_dados_dea = os.path.join(DIRETORIO_DADOS_INTERMEDIARIOS, 'perfil_hosp_pubs_oss_dez_{ANO}_clusterizado.xlsx'.format(ANO=ANO))
    #arquivo_dados_dea = os.path.join(DIRETORIO_DADOS_INTERMEDIARIOS, 'amostra.xlsx')
    df = pd.read_excel(arquivo_dados_dea)

    # Executa DEA
    modelo_tcu.executa_por_cluster(arquivo_dados_dea, DIRETORIO_DADOS_RESULTADOS, 'CLUSTER')

    """
    # Carrega resultados do arquivo
    arquivo_resultados_basename = os.path.splitext(os.path.basename(arquivo_dados_dea))[0] + '_result.xlsx'
    arquivo_resultados = os.path.join(DIRETORIO_DADOS_RESULTADOS, arquivo_resultados_basename)
    df_resultado = pd.read_excel(arquivo_resultados, skiprows=1)

    # Adicionar resultado de eficiência ao dataframe original e salva resultado
    df = pd.concat((df, df_resultado['Efficiency']), axis=1)
    #df.to_excel('')
    """
    print('Duração total da execução: ' + str(time.time() - t0))


