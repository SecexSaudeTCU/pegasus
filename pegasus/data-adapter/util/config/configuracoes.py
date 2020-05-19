import yaml

class Config:
    def __init__(self, arquivo_configuracao='config.yml'):
        with open(arquivo_configuracao, "r", encoding="utf8") as ymlfile:
            self.cfg = yaml.load(ymlfile)

