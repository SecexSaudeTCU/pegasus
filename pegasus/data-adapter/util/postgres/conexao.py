import psycopg2
from util.config.configuracoes import Config

class ConfiguracoesConexaoPostgresSQL(Config):

    def __init__(self, arquivo_configuracao):
        super(ConfiguracoesConexaoPostgresSQL, self).__init__(arquivo_configuracao)
        self.config_banco = self.cfg['postgres_sql']

    def get_conexao(self):
        #TODO: Usar o parâmetro connection_factory
        conexao = psycopg2.connect(user=self.config_banco['user'],
                                   password=self.config_banco['password'],
                                   host=self.config_banco['host'],
                                   port=self.config_banco['port'],
                                   database=self.config_banco['database'])
        return conexao

    def get_string_conexao(self):
        return '%s+%s://%s:%s@%s:%s/%s' % (
            'postgresql', 'psycopg2', self.config_banco['user'], self.config_banco['password'],
            self.config_banco['host'], self.config_banco['port'], self.config_banco['database'])
