from ibge.dao_ibge import DaoIBGE


class IBGEFacade:
    def __init__(self, arquivo_configuracao):
        self.dao = DaoIBGE(arquivo_configuracao)

    def get_df_populacao_ibge(self):
        df_populacao_ufs = self.get_df_populacao_ufs()

        df_populacao_municipios = self.dao.get_df_populacao_municipios()
        df_populacao_municipios['COD_UF'] = df_populacao_municipios['cod_municipio'].str[0:2]

        df_populacao = df_populacao_municipios.merge(df_populacao_ufs, on=['COD_UF'], suffixes=('', '_UF'))
        df_populacao.drop(['COD_UF', 'ESTADO'], axis=1, inplace=True)
        df_populacao['POPULACAO_BRASIL'] = df_populacao_municipios.sum()['POPULACAO']

        return df_populacao

    def get_df_populacao_ufs(self):
        df_populacao_ufs = self.dao.get_df_populacao_ufs()
        df_populacao_ufs.rename(columns={'ID': 'COD_UF'}, inplace=True)
        return df_populacao_ufs

