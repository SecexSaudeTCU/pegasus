from sih.dao_sih import DaoSIH
from ibge.ibge_facade import get_df_populacao_ibge
import pandas as pd

def get_df_populacao():
    df_populacao = get_df_populacao_ibge()

    dao = DaoSIH()
    df_coordenadas = dao.get_df_coordenadas()
    df_populacao = pd.merge(df_populacao, df_coordenadas, on='cod_municipio')

    print(df_populacao.shape)
    print(df_populacao.head(2))

    return df_populacao

def get_df_lista_procedimento_ano(ano):
    dao = DaoSIH()
    df_rd = dao.get_df_procedimentos_realizados_por_municipio(ano)

    df_lista_procedimento_ano = df_rd[['ano_cmpt', 'proc_rea']].drop_duplicates()
    df_lista_procedimento_ano['key'] = 0

    print(df_lista_procedimento_ano.shape)
    return df_lista_procedimento_ano

def get_df_lista_municipio_ano(ano):
    dao = DaoSIH()
    df_rd = dao.get_df_procedimentos_realizados_por_municipio(ano)

    df_lista_municipio_ano = df_rd[['ano_cmpt', 'cod_municipio']].drop_duplicates()
    df_lista_municipio_ano['key'] = 0

    print(df_lista_municipio_ano.shape)
    return df_lista_municipio_ano

def get_df_procedimentos_realizados_por_municipio_e_populacao(ano):
    dao = DaoSIH()
    df_rd = dao.get_df_procedimentos_realizados_por_municipio(ano)

    df_populacao = get_df_populacao()

    df_analise1 = pd.merge(df_rd, df_populacao, on=['cod_municipio'])

    df_analise1['COD_FORMA'] = df_analise1['proc_rea'].str[:6]
    df_analise1['COD_SUBGRUPO'] = df_analise1['proc_rea'].str[:4]
    df_analise1['COD_GRUPO'] = df_analise1['proc_rea'].str[:2]

    df_analise1['qtd_procedimento'] = df_analise1['qtd_procedimento'].fillna(0)
    df_analise1['vl_total'] = df_analise1['vl_total'].fillna(0)

    print(df_analise1.shape)
    df_analise1.head()

if __name__ == '__main__':
    get_df_procedimentos_realizados_por_municipio_e_populacao(2015)
    #get_df_lista_municipio_ano(2015)
    #get_df_lista_procedimento_ano(2015)
    #get_df_populacao()