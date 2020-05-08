from util.postgres.repositorio_util import RepositorioPostgresSQL
import pandas as pd


class RepositorioCNES(RepositorioPostgresSQL):
    def __init__(self):
        super(RepositorioCNES, self).__init__(arquivo_configuracao='../util/postgres/config.yml')

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

if __name__ == '__main__':
    repositorio = RepositorioCNES()
    df = repositorio.get_df_estabelecimento_regiao_saude()
    print(df.head())