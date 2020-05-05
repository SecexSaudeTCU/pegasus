import psycopg2
import yaml


class RepositorioPostgresSQL:
    def __init__(self, arquivo_configuracao="../config.yml"):
        self.__get_conexao_banco(arquivo_configuracao)

    def __get_conexao_banco(self, arquivo_configuracao):
        with open(arquivo_configuracao, "r") as ymlfile:
            cfg = yaml.load(ymlfile)
        config_banco = cfg['postgres_sql']
        self.conexao = psycopg2.connect(user=config_banco['user'],
                                        password=config_banco['password'],
                                        host=config_banco['host'],
                                        port=config_banco['port'],
                                        database=config_banco['database'])