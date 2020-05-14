from sih.dao_sih import DaoSIH
from ibge.ibge_facade import get_df_populacao_ibge
import pandas as pd


def get_df_populacao():
    df_populacao = get_df_populacao_ibge()

    dao = DaoSIH()
    df_coordenadas = dao.get_df_coordenadas()
    df_populacao = pd.merge(df_populacao, df_coordenadas, on='cod_municipio')

    return df_populacao


def get_df_lista_procedimento_ano(ano):
    dao = DaoSIH()
    df_rd = dao.get_df_procedimentos_realizados_por_municipio(ano)

    df_lista_procedimento_ano = df_rd[['ano_cmpt', 'proc_rea']].drop_duplicates()
    df_lista_procedimento_ano['key'] = 0

    return df_lista_procedimento_ano


def get_df_lista_municipio_ano(ano):
    dao = DaoSIH()
    df_rd = dao.get_df_procedimentos_realizados_por_municipio(ano)

    df_lista_municipio_ano = df_rd[['ano_cmpt', 'cod_municipio']].drop_duplicates()
    df_lista_municipio_ano['key'] = 0

    return df_lista_municipio_ano


def get_df_procedimentos_realizados_por_municipio_e_populacao(ano):
    dao = DaoSIH()
    df_rd = dao.get_df_procedimentos_realizados_por_municipio(ano)

    df_populacao = get_df_populacao()

    #df_analise = pd.merge(df_rd, df_populacao, on=['cod_municipio'])
    df_analise = pd.merge(df_rd, df_populacao, on=['cod_municipio'], how="left")

    df_analise['COD_FORMA'] = df_analise['proc_rea'].str[:6]
    df_analise['COD_SUBGRUPO'] = df_analise['proc_rea'].str[:4]
    df_analise['COD_GRUPO'] = df_analise['proc_rea'].str[:2]

    df_analise['qtd_procedimento'] = df_analise['qtd_procedimento'].fillna(0)
    df_analise['vl_total'] = df_analise['vl_total'].fillna(0)

    return df_analise


def get_df_procedimento_painel(ano, habitantes_tx):
    return __get_df_painel(ano, habitantes_tx, coluna='proc_rea', nivel='PROCEDIMENTO')


def get_df_forma_painel(ano,habitantes_tx):
    return __get_df_painel(ano, habitantes_tx, coluna='COD_FORMA', nivel='FORMA')

def get_df_subgrupo_painel(ano,habitantes_tx):
    return __get_df_painel(ano, habitantes_tx, coluna='COD_SUBGRUPO', nivel='SUBGRUPO')

def get_df_grupo_painel(ano,habitantes_tx):
    return __get_df_painel(ano, habitantes_tx, coluna='COD_GRUPO', nivel='GRUPO')

def __get_df_painel(ano, habitantes_tx, coluna, nivel):
    df_analise = get_df_procedimentos_realizados_por_municipio_e_populacao(ano)
    df_painel = df_analise.groupby(
        ['ano_cmpt', 'cod_municipio', 'LATITUDE', 'LONGITUDE', 'nm_municipio', coluna, 'uf', 'POPULACAO',
         'POPULACAO_UF', 'POPULACAO_BRASIL']).sum()[['qtd_procedimento', 'vl_total']].reset_index()
    df_uf_painel = __get_df_painel_uf(df_analise, coluna)
    df_brasil_painel = __get_df_painel_brasil(df_analise, coluna)

    df_painel = df_painel.join(df_uf_painel, on=['ano_cmpt', 'uf', coluna, 'POPULACAO_UF'], rsuffix='_UF')
    df_painel = df_painel.join(df_brasil_painel, on=['ano_cmpt', coluna, 'POPULACAO_BRASIL'], rsuffix='_BRASIL')
    df_painel = df_painel.rename(columns={"ano_cmpt": "ANO", "proc_rea": "PROCEDIMENTO"})

    df_painel['TX'] = df_painel['qtd_procedimento'] * habitantes_tx / df_painel['POPULACAO']
    df_painel['TX_UF'] = df_painel['qtd_procedimento_UF'] * habitantes_tx / df_painel['POPULACAO_UF']
    df_painel['TX_BRASIL'] = df_painel['qtd_procedimento_BRASIL'] * habitantes_tx / df_painel['POPULACAO_BRASIL']

    df_painel['NIVEL'] = nivel

    return df_painel


def __get_df_painel_uf(df_analise, coluna):
    df_procedimento_uf_painel = df_analise.groupby(['ano_cmpt', 'uf', coluna, 'POPULACAO_UF']).sum()[
        ['qtd_procedimento', 'vl_total']]
    return df_procedimento_uf_painel


def __get_df_painel_brasil(df_analise, coluna):
    df_procedimento_brasil_painel = df_analise.groupby(['ano_cmpt', coluna, 'POPULACAO_BRASIL']).sum()[
        ['qtd_procedimento', 'vl_total']]
    return df_procedimento_brasil_painel




if __name__ == '__main__':
    habitantes_tx = 100
    get_df_grupo_painel(2014, habitantes_tx)
    #get_df_subgrupo_painel(2008, habitantes_tx)
    #get_df_forma_painel(2019, habitantes_tx)
    #get_df_procedimento_painel()
    # get_df_procedimento_painel_brasil(2019)
    # get_df_procedimento_painel_uf(2019)
    # get_df_procedimentos_realizados_por_municipio_e_populacao(2015)
    # get_df_lista_municipio_ano(2015)
    # get_df_lista_procedimento_ano(2015)
    # get_df_populacao()
