from util.postgres.dao_util import DaoPostgresSQL
import pandas as pd


class DaoSIA(DaoPostgresSQL):

    def __init__(self):
        super(DaoSIA, self).__init__(arquivo_configuracao='../util/postgres/config.yml')

    def obter_nome_procedimento(self, id):
        """
        Obtém o nome de um(a) determinado(a) grupo/subgrupo/forma/procedimento.
        :param id: Identificador único do(a) grupo/subgrupo/forma/procedimento.
        :return:
        """
        if len(id) == 2:
            df = self.__get_df_grupo_por_id(id)
        elif len(id) == 4:
            df = self.__get_df_subgrupo_por_id(id)
        elif len(id) == 6:
            df = self.__get_df_forma_por_id(id)
        elif len(id) == 10:
            df = self.__get_df_procedimento_por_id(id)

        return df.iloc[0, 1]

    def obter_procedimentos(self, seletor):
        """
        Obtém todos os procedimentos que se enquadram nos valores passados como argumentos. É possível, inclusive,
        solicitar todos os procedimentos que fazem parte de um determinado grupo (ex.: 0206 - Diagnóstico por Tomografia
        - trará todos os procedimentos que são iniciados por 0206).
        :param seletor: String que permite filtrar por identificação hiearárquica dos procedimentos.
        :return:
        """
        sql = 'SELECT \"ID\", \"PROCEDIMENTO\" FROM sia_pa.paproc'
        conexao = self.get_conexao()
        df = pd.read_sql(sql, conexao)
        conexao.close()
        return df[df.ID.str.startswith(seletor)]

    def __get_df_grupo_por_id(self, id):
        """
        Retorna um dado grupo.
        :param id: Identificador único do grupo.
        :return:
        """
        sql = f"SELECT \"ID\", \"GRUPO\" FROM sia_pa.grupo where \"ID\" = '{id}'"
        conexao = self.get_conexao()
        df = pd.read_sql(sql, conexao)
        conexao.close()
        return df

    def __get_df_subgrupo_por_id(self, id):
        """
        Retorna um dado subgrupo.
        :param id: Identificador único do subgrupo.
        :return:
        """
        sql = f"SELECT \"ID\", \"SUBGRUPO\" FROM sia_pa.subgrupo where \"ID\" = '{id}'"
        conexao = self.get_conexao()
        df = pd.read_sql(sql, conexao)
        conexao.close()
        return df

    def __get_df_forma_por_id(self, id):
        """
        Retorna uma dada forma.
        :param id: Identificador único da forma.
        :return:
        """
        sql = f"SELECT \"ID\", \"FORMA\" FROM sia_pa.forma where \"ID\" = '{id}'"
        conexao = self.get_conexao()
        df = pd.read_sql(sql, conexao)
        conexao.close()
        return df

    def __get_df_procedimento_por_id(self, id):
        """
        Retorna um dado procedimento.
        :param id: Identificador único do procedimento.
        :return:
        """
        sql = f"SELECT \"ID\", \"PROCEDIMENTO\" FROM sia_pa.paproc where \"ID\" = '{id}'"
        conexao = self.get_conexao()
        df = pd.read_sql(sql, conexao)
        conexao.close()
        return df

if __name__ == '__main__':
    dao = DaoSIA()
    nome = dao.obter_nome_procedimento('01')
    print(nome)

    nome = dao.obter_nome_procedimento('0101')
    print(nome)

    nome = dao.obter_nome_procedimento('010101')
    print(nome)

    nome = dao.obter_nome_procedimento('0101010010')
    print(nome)

    df = dao.obter_procedimentos('01')
    print(df.head())

    df = dao.obter_procedimentos('0101')
    print(df.head())

    df = dao.obter_procedimentos('010101')
    print(df.head())

    df = dao.obter_procedimentos('0101010010')
    print(df.head())