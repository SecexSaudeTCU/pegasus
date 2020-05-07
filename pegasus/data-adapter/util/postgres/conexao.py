import psycopg2
import yaml


def get_conexao_banco(arquivo_configuracao):
    with open(arquivo_configuracao, "r") as ymlfile:
        cfg = yaml.load(ymlfile)
    config_banco = cfg['postgres_sql']
    conexao = psycopg2.connect(user=config_banco['user'],
                                    password=config_banco['password'],
                                    host=config_banco['host'],
                                    port=config_banco['port'],
                                    database=config_banco['database'])
    return conexao