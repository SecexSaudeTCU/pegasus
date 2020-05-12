from ibge.dao_ibge import DaoIBGE

def get_df_populacao_ibge():
    dao = DaoIBGE()

    df_populacao_ufs = dao.get_df_populacao_ufs()
    df_populacao_ufs.rename(columns={'ID':'COD_UF'}, inplace=True)

    df_populacao_municipios = dao.get_df_populacao_municipios()
    df_populacao_municipios['COD_UF'] = df_populacao_municipios['cod_municipio'].str[0:2]

    df_populacao = df_populacao_municipios.merge(df_populacao_ufs, on=['COD_UF'], suffixes=('','_UF'))
    df_populacao.drop(['COD_UF', 'ESTADO'], axis=1, inplace=True)
    df_populacao['POPULACAO_BRASIL'] = df_populacao_municipios.sum()['POPULACAO']

    return df_populacao

if __name__ == '__main__':
    get_df_populacao_ibge()