from ibge.dao_ibge import DaoIBGE
from util.metricas import downcast, mem_usage

class IBGEFacade:
    def __init__(self, arquivo_configuracao):
        self.__dao = DaoIBGE(arquivo_configuracao)

    def get_df_populacao_ibge(self):
        df_populacao_ufs = self.get_df_populacao_ufs()

        df_populacao_municipios = self.__dao.get_df_populacao_municipios()
        df_populacao_municipios['COD_UF'] = df_populacao_municipios['cod_municipio'].str[0:2]

        df_populacao = df_populacao_municipios.merge(df_populacao_ufs, on=['COD_UF'], suffixes=('', '_UF'))
        #df_populacao.drop(['COD_UF', 'ESTADO'], axis=1, inplace=True)
        df_populacao.drop(['COD_UF'], axis=1, inplace=True)
        df_populacao['POPULACAO_BRASIL'] = df_populacao_municipios.sum()['POPULACAO']
        df_populacao = downcast(df_populacao)
        print(mem_usage(df_populacao))
        df_populacao = df_populacao.astype({'POPULACAO_BRASIL':'uint32', 'POPULACAO_UF':'uint32'})
        print(mem_usage(df_populacao))
        return df_populacao

    def get_df_populacao_ufs(self):
        df_populacao_ufs = self.__dao.get_df_populacao_ufs()
        df_populacao_ufs.rename(columns={'ID': 'COD_UF'}, inplace=True)
        return df_populacao_ufs

