from util.postgres.conexao import get_conexao_banco


class RepositorioPostgresSQL:
    def __init__(self, arquivo_configuracao="../config.yml"):
        self.conexao = get_conexao_banco(arquivo_configuracao)

