from util.postgres.conexao import ConfiguracoesConexaoPostgresSQL


class DaoPostgresSQL:
    def __init__(self, arquivo_configuracao):
        self.config = ConfiguracoesConexaoPostgresSQL(arquivo_configuracao)

    def get_conexao(self):
        return self.config.get_conexao()

