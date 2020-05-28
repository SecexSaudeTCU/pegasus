from sih.dao_sih import DaoSIH
from ibge.ibge_facade import IBGEFacade
import pandas as pd
from config.configuracoes import ConfiguracoesAnalise
import time
from util.metricas import downcast, mem_usage


class SIHFacade:
    def __init__(self, arquivo_configuracao):
        self.__dao = DaoSIH(arquivo_configuracao)
        self.__ibge_facade = IBGEFacade(arquivo_configuracao)
        self.__habitantes_tx = ConfiguracoesAnalise(arquivo_configuracao).get_propriedade('habitantes_tx')

    # TODO: ANÁLISE 2
    def get_df_lista_procedimento_ano(self, ano):
        df_rd = self.__dao.get_df_procedimentos_realizados_por_municipio(ano)
        df_lista_procedimento_ano = df_rd[['ano_cmpt', 'proc_rea']].drop_duplicates()

        return df_lista_procedimento_ano

    def get_df_lista_municipio_ano(self, ano):
        df_rd = self.__dao.get_df_procedimentos_realizados_por_municipio(ano)

        df_lista_municipio_ano = df_rd[['ano_cmpt', 'cod_municipio']].drop_duplicates()
        df_lista_municipio_ano['key'] = 0

        return df_lista_municipio_ano

    def get_df_populacao(self):
        df_populacao = self.__ibge_facade.get_df_populacao_ibge()
        # df_coordenadas = self.__dao.get_df_coordenadas()
        # df_populacao = pd.merge(df_populacao, df_coordenadas, on='cod_municipio')

        return df_populacao

    def __get_df_procedimentos_realizados_por_municipio_e_populacao(self, df_analise, df_populacao):
        df_analise['COD_FORMA'] = df_analise['proc_rea'].str[:6]
        df_analise['COD_SUBGRUPO'] = df_analise['proc_rea'].str[:4]
        df_analise['COD_GRUPO'] = df_analise['proc_rea'].str[:2]

        df_analise['qtd_procedimento'] = df_analise['qtd_procedimento'].fillna(0)
        df_analise['vl_total'] = df_analise['vl_total'].fillna(0)

        df_analise = downcast(df_analise)
        print(mem_usage(df_analise))
        df_analise = df_analise.astype({'POPULACAO': 'uint32', 'POPULACAO_UF': 'uint32', 'POPULACAO_BRASIL': 'uint32'})
        print(mem_usage(df_analise))
        return df_analise

    def __get_df_procedimento_painel(self, df_analise):
        return self.__get_df_painel('proc_rea', 'PROCEDIMENTO', df_analise)

    def __get_df_forma_painel(self, df_analise):
        return self.__get_df_painel('COD_FORMA', 'FORMA', df_analise)

    def __get_df_subgrupo_painel(self, df_analise):
        return self.__get_df_painel('COD_SUBGRUPO', 'SUBGRUPO', df_analise)

    def __get_df_grupo_painel(self, df_analise):
        return self.__get_df_painel('COD_GRUPO', 'GRUPO', df_analise)

    def __get_df_procedimentos_ano_para_analise(self, df_analise, df_populacao):
        df_analise = self.__get_df_procedimentos_realizados_por_municipio_e_populacao(df_analise, df_populacao)

        df_procedimento_painel = self.__get_df_procedimento_painel(df_analise)
        df_forma_painel = self.__get_df_forma_painel(df_analise)
        df_subgrupo_painel = self.__get_df_subgrupo_painel(df_analise)
        df_grupo_painel = self.__get_df_grupo_painel(df_analise)

        # df_proc_ano_analise = pd.DataFrame(
        #     columns=['ANO', 'cod_municipio', 'nm_municipio', 'PROCEDIMENTO', 'uf', 'POPULACAO', 'POPULACAO_UF',
        #              'POPULACAO_BRASIL', 'LATITUDE', 'LONGITUDE', 'qtd_procedimento', 'vl_total', 'qtd_procedimento_UF',
        #              'vl_total_UF', 'qtd_procedimento_BRASIL', 'vl_total_BRASIL', 'TX', 'TX_UF', 'TX_BRASIL', 'NIVEL'])
        df_proc_ano_analise = pd.DataFrame(
            columns=['ANO', 'cod_municipio', 'PROCEDIMENTO', 'POPULACAO', 'POPULACAO_UF',
                     'POPULACAO_BRASIL', 'qtd_procedimento', 'vl_total', 'qtd_procedimento_UF',
                     'vl_total_UF', 'qtd_procedimento_BRASIL', 'vl_total_BRASIL', 'TX', 'TX_UF', 'TX_BRASIL', 'NIVEL'])
        df_proc_ano_analise = df_proc_ano_analise.append(df_procedimento_painel, sort=False)
        df_proc_ano_analise = df_proc_ano_analise.append(df_forma_painel, sort=False)
        df_proc_ano_analise = df_proc_ano_analise.append(df_subgrupo_painel, sort=False)
        df_proc_ano_analise = df_proc_ano_analise.append(df_grupo_painel, sort=False)

        print(mem_usage(df_proc_ano_analise))
        df_proc_ano_analise = df_proc_ano_analise.astype(
            {'ANO': 'category', 'cod_municipio': 'category', 'PROCEDIMENTO': 'category', 'POPULACAO': 'uint32',
             'POPULACAO_UF': 'uint32', 'POPULACAO_BRASIL': 'uint32', 'qtd_procedimento': 'uint32',
             'qtd_procedimento_UF': 'uint32', 'qtd_procedimento_BRASIL': 'uint32', 'NIVEL': 'category'})
        print(mem_usage(df_proc_ano_analise))

        #TODO: Retomar a comparação de código a partir deste ponto em diante.
        return df_proc_ano_analise

    def get_df_nivel(self, df_analise, df_populacao):
        df_nivel = self.__get_df_procedimentos_ano_para_analise(df_analise, df_populacao).groupby('NIVEL').count()[
            'ANO'].reset_index()

        df_nivel['ORDEM'] = 0
        df_nivel.loc[df_nivel[df_nivel['NIVEL'] == 'FORMA'].index, 'ORDEM'] = 1
        df_nivel.loc[df_nivel[df_nivel['NIVEL'] == 'SUBGRUPO'].index, 'ORDEM'] = 2
        df_nivel.loc[df_nivel[df_nivel['NIVEL'] == 'GRUPO'].index, 'ORDEM'] = 3

        df_nivel = df_nivel.sort_values(by='ORDEM')
        return df_nivel

    def get_df_nivel_procedimento(self, df_analise, df_populacao):
        df_nivel_proc = self.__get_df_procedimentos_ano_para_analise(df_analise, df_populacao)[
            ['NIVEL', 'PROCEDIMENTO']].drop_duplicates()
        df_nivel_proc = df_nivel_proc.groupby('NIVEL').count().reset_index()

        df_nivel_proc['ORDEM'] = 0
        df_nivel_proc.loc[df_nivel_proc[df_nivel_proc['NIVEL'] == 'FORMA'].index, 'ORDEM'] = 1
        df_nivel_proc.loc[df_nivel_proc[df_nivel_proc['NIVEL'] == 'SUBGRUPO'].index, 'ORDEM'] = 2
        df_nivel_proc.loc[df_nivel_proc[df_nivel_proc['NIVEL'] == 'GRUPO'].index, 'ORDEM'] = 3

        df_nivel_proc = df_nivel_proc.sort_values(by='ORDEM')
        print(df_nivel_proc)

        return df_nivel_proc

    # TODO: Otimizar.  Esta consulta está levando horas...
    def get_df_descricao_procedimentos(self, df_analise, df_populacao):
        start_time = time.time()
        df_proc_ano_analise = self.__get_df_procedimentos_ano_para_analise(df_analise, df_populacao)
        print("self.__get_df_procedimentos_por_ano(ano): --- %s seconds ---" % (time.time() - start_time))

        df_procedimentos_analise = pd.DataFrame(columns=['PROCEDIMENTO'],
                                                data=df_proc_ano_analise['PROCEDIMENTO'].unique())

        start_time = time.time()
        df_descricao_procedimento = self.__dao.get_df_descricao_procedimentos()
        print("self.dao.get_df_descricao_procedimentos(): --- %s seconds ---" % (time.time() - start_time))

        start_time = time.time()
        df_procedimentos_analise['DESCRICAO_COMPLETA'] = df_procedimentos_analise['PROCEDIMENTO'].apply(
            SIHFacade.__get_proc_descricao, df_descricao_procedimento=df_descricao_procedimento)
        print("SIHFacade.__get_proc_descricao: --- %s seconds ---" % (time.time() - start_time))

        start_time = time.time()
        df_procedimentos_analise['DESCRICAO'] = df_procedimentos_analise['PROCEDIMENTO'].apply(
            SIHFacade.__get_proc_name, df_descricao_procedimento=df_descricao_procedimento)
        print("SIHFacade.__get_proc_name: --- %s seconds ---" % (time.time() - start_time))

        start_time = time.time()
        df_procedimentos_analise = df_procedimentos_analise.set_index('PROCEDIMENTO')
        print("df_procedimentos_analise.set_index('PROCEDIMENTO'): --- %s seconds ---" % (time.time() - start_time))

        #TODO: Retomar as otimizações de dataframes deste ponto em diante.
        return df_procedimentos_analise

    def get_df_procedimentos_por_ano_com_descricao(self, df_analise, df_populacao, df_descricao_procedimentos):
        df_proc_ano_analise = self.__get_df_procedimentos_ano_para_analise(df_analise, df_populacao)
        df_proc_ano_analise = df_proc_ano_analise.join(df_descricao_procedimentos, on=['PROCEDIMENTO'])

        print(df_proc_ano_analise.shape)
        print(df_proc_ano_analise.head())

        return df_proc_ano_analise

    @staticmethod
    def __get_proc_descricao(procedimento, df_descricao_procedimento):
        len_proc = len(procedimento)

        # Obtendo nome do procedimento
        start_time = time.time()
        df_proc = df_descricao_procedimento[df_descricao_procedimento['PROCREA_ID'].str.startswith(procedimento)]
        print("self.__get_proc_descricao() -> df_proc = "
              "df_descricao_procedimento[df_descricao_procedimento['PROCREA_ID'].str.startswith(procedimento)]: --- "
              "%s seconds ---" % (
                      time.time() - start_time))
        if len(df_proc) > 0:
            _procedimento = df_proc.iloc[0]
            # (nível de grupo)
            if (len_proc in (2, 4, 6, 10)):
                proc_name = 'Grupo: ' + _procedimento['GRUPO_ID'] + ' : ' + _procedimento['dsc_grupo']
                # (nível de subgrupo)
                if (len_proc in (4, 6, 10)):
                    proc_name = proc_name + ' - SubGrupo: ' + _procedimento['SUBGRUPO_ID'] + ' : ' + _procedimento[
                        'dsc_subgrupo']
                    # (nível de forma)
                    if (len_proc in (6, 10)):
                        proc_name = proc_name + ' - Forma: ' + _procedimento['cod_forma'] + ' : ' + _procedimento[
                            'dsc_forma']
                        # (nível de procedimento)
                        if (len_proc == 10):
                            proc_name = proc_name + ' - Procedimento: ' + _procedimento['PROCREA_ID'] + ' : ' + \
                                        _procedimento['dsc_proc']
                return proc_name
            else:
                return 'Nível inválido de procedimento - ' + procedimento
        else:
            return 'Procedimento inexistente - ' + procedimento

    @staticmethod
    def __get_proc_name(procedimento, df_descricao_procedimento):
        len_proc = len(procedimento)

        # Obtendo nome do procedimento
        start_time = time.time()
        df_proc = df_descricao_procedimento[df_descricao_procedimento['PROCREA_ID'].str.startswith(procedimento)]
        print("self.__get_proc_descricao() -> df_proc = "
              "df_descricao_procedimento[df_descricao_procedimento['PROCREA_ID'].str.startswith(procedimento)]: --- "
              "%s seconds ---" % (
                      time.time() - start_time))
        if (len(df_proc) > 0):
            _procedimento = df_proc.iloc[0]
            # (nível de grupo)
            if (len_proc == 2):
                proc_name = 'Grupo: ' + _procedimento['GRUPO_ID'] + ' : ' + _procedimento['dsc_grupo']
                # (nível de subgrupo)
            elif (len_proc == 4):
                proc_name = 'SubGrupo: ' + _procedimento['SUBGRUPO_ID'] + ' : ' + _procedimento['dsc_subgrupo']
                # (nível de forma)
            elif (len_proc == 6):
                proc_name = 'Forma: ' + _procedimento['cod_forma'] + ' : ' + _procedimento['dsc_forma']
                # (nível de procedimento)
            elif (len_proc == 10):
                proc_name = 'Procedimento: ' + _procedimento['PROCREA_ID'] + ' : ' + _procedimento['dsc_proc']
            else:
                proc_name = 'Nível inválido de procedimento - ' + procedimento
        else:
            proc_name = 'Procedimento inexistente - ' + procedimento
        return proc_name

    def __get_df_painel(self, coluna, nivel, df_analise):
        # df_painel = df_analise.groupby(
        #     ['ano_cmpt', 'cod_municipio', 'LATITUDE', 'LONGITUDE', 'nm_municipio', coluna, 'uf', 'POPULACAO',
        #      'POPULACAO_UF', 'POPULACAO_BRASIL']).sum()[['qtd_procedimento', 'vl_total']].reset_index()
        df_painel = df_analise.groupby(
            ['ano_cmpt', 'cod_municipio', coluna, 'POPULACAO', 'POPULACAO_UF', 'POPULACAO_BRASIL'],
            observed=True).sum()[['qtd_procedimento', 'vl_total']].reset_index()

        df_uf_painel = self.__get_df_painel_uf(df_analise, coluna)
        df_brasil_painel = self.__get_df_painel_brasil(df_analise, coluna)

        # df_painel = df_painel.join(df_uf_painel, on=['ano_cmpt', 'uf', coluna, 'POPULACAO_UF'], rsuffix='_UF')
        df_painel = df_painel.join(df_uf_painel, on=['ano_cmpt', coluna, 'POPULACAO_UF'], rsuffix='_UF')
        df_painel = df_painel.join(df_brasil_painel, on=['ano_cmpt', coluna, 'POPULACAO_BRASIL'], rsuffix='_BRASIL')

        df_painel = df_painel.rename(columns={"ano_cmpt": "ANO", coluna: "PROCEDIMENTO"})

        df_painel['TX'] = df_painel['qtd_procedimento'] * self.__habitantes_tx / df_painel['POPULACAO']
        df_painel['TX_UF'] = df_painel['qtd_procedimento_UF'] * self.__habitantes_tx / df_painel['POPULACAO_UF']
        df_painel['TX_BRASIL'] = df_painel['qtd_procedimento_BRASIL'] * self.__habitantes_tx / df_painel[
            'POPULACAO_BRASIL']

        df_painel['NIVEL'] = nivel

        return df_painel

    def __get_df_painel_uf(self, df_analise, coluna):
        # df_procedimento_uf_painel = df_analise.groupby(['ano_cmpt', 'uf', coluna, 'POPULACAO_UF']).sum()[
        #     ['qtd_procedimento', 'vl_total']]
        df_procedimento_uf_painel = df_analise.groupby(['ano_cmpt', coluna, 'POPULACAO_UF'], observed=True).sum()[
            ['qtd_procedimento', 'vl_total']]
        return df_procedimento_uf_painel

    def __get_df_painel_brasil(self, df_analise, coluna):
        df_procedimento_brasil_painel = \
            df_analise.groupby(['ano_cmpt', coluna, 'POPULACAO_BRASIL'], observed=True).sum()[
                ['qtd_procedimento', 'vl_total']]
        return df_procedimento_brasil_painel
