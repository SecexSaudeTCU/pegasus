import time

import pandas as pd

from util.metricas import downcast
from util.postgres.dao_util import DaoPostgresSQL


class DaoSIH(DaoPostgresSQL):

    def __init__(self, arquivo_configuracao):
        super(DaoSIH, self).__init__(arquivo_configuracao)

    def get_df_descricao_procedimentos(self):
        """
        Retorna as descrições dos procedimentos, bem como dos seus grupos, subgrupos e formas.
        :return:
        """
        sql = 'select p."PROCREA_ID", p."GRUPO_ID", g."GRUPO" as DSC_GRUPO, p."SUBGRUPO_ID", ' \
              's."SUBGRUPO" as DSC_SUBGRUPO, p."FORMA_ID" as COD_FORMA, f."FORMA" as DSC_FORMA, ' \
              'pr."PROCEDIMENTO" as DSC_PROC ' \
              'from sih_rd.rdbr p ' \
              'join sih_rd.grupo g on p."GRUPO_ID" = g."ID" ' \
              'join sih_rd.subgrupo s on p."SUBGRUPO_ID" = s."ID" ' \
              'join sih_rd.forma f on p."FORMA_ID" = f."ID" ' \
              'join sih_rd.procrea pr on p."PROCREA_ID" = pr."ID"'
        conexao = self.get_conexao()

        start_time = time.time()
        resultado = pd.read_sql(sql, conexao, chunksize=1000)
        print("resultado = pd.read_sql(sql, conexao, chunksize=1000): --- %s seconds ---" % (time.time() - start_time))

        # df = df.astype('category')

        # retorno = pd.DataFrame(
        #     columns=['PROCREA_ID', 'GRUPO_ID', 'dsc_grupo', 'SUBGRUPO_ID', 'dsc_subgrupo', 'cod_forma', 'dsc_forma',
        #              'dsc_proc'])
        retorno = pd.DataFrame(
            dict(PROCREA_ID=pd.Series([], dtype='category'),
                 GRUPO_ID=pd.Series([], dtype='category'),
                 dsc_grupo=pd.Series([], dtype='category'),
                 SUBGRUPO_ID=pd.Series([], dtype='category'),
                 dsc_subgrupo= pd.Series([], dtype='category'),
                 cod_forma=pd.Series([], dtype='category'),
                 dsc_proc=pd.Series([], dtype='category')
                 )
        )
        for df in resultado:
            df = df.astype('category')
            retorno = retorno.append(df, ignore_index=True)
            retorno = retorno.astype('category')

        # start_time = time.time()
        # retorno = pd.concat([df.astype('category') for df in resultado], ignore_index=True)
        # print(
        #     "retorno = pd.concat([df.astype('category') for df in resultado], ignore_index=True): --- %s seconds ---" % (
        #                 time.time() - start_time))

        conexao.close()

        # return df
        return retorno

    def get_df_coordenadas(self):
        """
        Retorna as coordenadas de cada municípios.
        :return:
        """
        sql = 'select "ID" as COD_MUNICIPIO, "LATITUDE", "LONGITUDE" ' \
              'from sih_rd.ufzi where "LATITUDE" <> 0 and "LONGITUDE" <> 0'
        conexao = self.get_conexao()
        df = pd.read_sql(sql, conexao)
        conexao.close()
        return df

    def get_df_procedimentos_realizados_por_municipio(self, ano):
        """
        Retorna os procedimentos realizados agrpados por município.
        :param ano: O ano de realização.
        :return:
        """
        sql = 'select "ANO_RD" as ANO_CMPT, "PROCREA_ID" as PROC_REA, "UFZI_ID" as COD_MUNICIPIO, ' \
              'count(*) as QTD_PROCEDIMENTO,  sum("VAL_TOT") as VL_TOTAL ' \
              'from sih_rd.rdbr ' \
              'where "ANO_RD" = ' + str(ano) + ' and "IDENT_ID" = \'1\' ' \
                                               'group by "ANO_RD", "PROCREA_ID", "UFZI_ID"'
        conexao = self.get_conexao()
        df = pd.read_sql(sql, conexao)
        df = downcast(df)
        conexao.close()
        return df

    def get_df_estados(self):
        """
        Retorna todos os estados da federação, com os respectivos códigos, nomes e siglas.
        :return:
        """
        sql = 'SELECT * FROM sih_rd.ufcod'
        conexao = self.get_conexao()
        df = pd.read_sql(sql, conexao)
        conexao.close()
        return df
