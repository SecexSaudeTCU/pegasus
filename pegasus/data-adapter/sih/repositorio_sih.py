import pandas as pd

# Dúvidas:
# - Onde recuperar a informação sobre forma/grupo/subgrupo?
# - Temos script de carga da base do IBGE?
from util.postgres.repositorio_util import RepositorioPostgresSQL


class RepositorioSIH(RepositorioPostgresSQL):

    def get_df_estabelecimento_regiao_saude(self):
        """
        Retorna os estabelecimentos juntamente com código de UF/município e código de regiões de saúde
        :return:
        """
        sql = 'select "CNES_ID", "CODUFMUN_ID", c."RSAUDCOD" from ' \
              '(select distinct "CNES_ID", "CODUFMUN_ID", b."RSAUDCOD" from cnes_st.stbr a ' \
              'left join cnes_st.codufmun b ' \
              'on a."CODUFMUN_ID" = b."ID") AS c'
        df = pd.read_sql(sql, self.conexao)
        return df

    def get_df_descricao_procedimento(self):
        # TODO
        pass

    def get_df_populacao(self):
        # TODO
        pass

    def get_df_coordenadas(self):
        """
        Retorna as coordenadas de cada municípios.
        :return:
        """
        sql = 'select "ID", "LATITUDE", "LONGITUDE" from sih_rd.ufzi where "LATITUDE" <> 0 and "LONGITUDE" <> 0'
        df = pd.read_sql(sql, self.conexao)
        return df

    def get_df_procedimentos_realizados_por_municipio(self, ano):
        """
        Retorna os procedimentos realizados agrpados por município.
        :param ano: O ano de realização.
        :return:
        """
        sql = 'select "ANO_RD", "PROCREA_ID", "UFZI_ID", count(*) as "QTD_PROCEDIMENTO",  sum("VAL_TOT") as "VL_TOTAL" ' \
              'from sih_rd.rdbr ' \
              'where "ANO_RD" = ' + str(ano) + ' and "IDENT_ID" = \'1\' ' \
                                               'group by "ANO_RD", "PROCREA_ID", "UFZI_ID"'
        df = pd.read_sql(sql, self.conexao)
        return df
