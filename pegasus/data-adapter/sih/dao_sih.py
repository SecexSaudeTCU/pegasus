import pandas as pd

from util.postgres.dao_util import DaoPostgresSQL
from util.metricas import downcast, mem_usage

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
        df = pd.read_sql(sql, conexao)
        print(mem_usage(df))
        df = df.astype('category')
        print(mem_usage(df))
        conexao.close()
        return df

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
