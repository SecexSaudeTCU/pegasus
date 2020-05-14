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

    df_analise = pd.merge(df_rd, df_populacao, on=['cod_municipio'])

    df_analise['COD_FORMA'] = df_analise['proc_rea'].str[:6]
    df_analise['COD_SUBGRUPO'] = df_analise['proc_rea'].str[:4]
    df_analise['COD_GRUPO'] = df_analise['proc_rea'].str[:2]

    df_analise['qtd_procedimento'] = df_analise['qtd_procedimento'].fillna(0)
    df_analise['vl_total'] = df_analise['vl_total'].fillna(0)

    print(df_analise.shape)
    df_analise.head()

    return df_analise

def get_df_procedimento_painel(ano):
    df_analise = get_df_procedimentos_realizados_por_municipio_e_populacao(ano)
    df_analise.groupby(['ano_cmpt', 'cod_municipio', 'LATITUDE', 'LONGITUDE',
                         'nm_municipio', 'proc_rea', 'uf',
                         'POPULACAO', 'POPULACAO_UF', 'POPULACAO_BRASIL']).sum()[['qtd_procedimento',
                                                                                  'vl_total']].reset_index()
    print(df_analise.head())

if __name__ == '__main__':
    get_df_procedimento_painel(2019)
    #get_df_procedimentos_realizados_por_municipio_e_populacao(2015)
    #get_df_lista_municipio_ano(2015)
    #get_df_lista_procedimento_ano(2015)
    #get_df_populacao()