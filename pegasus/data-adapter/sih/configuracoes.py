from util.config.configuracoes import Config

class ConfiguracoesAnalise(Config):
    def __init__(self, arquivo_configuracao):
        super(ConfiguracoesAnalise, self).__init__(arquivo_configuracao)
        self.config_analise = self.cfg['analise']

    def get_propriedade(self, nome_propriedade):
        return self.config_analise[nome_propriedade]