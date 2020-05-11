import urllib.request
import os
import pandas as pd

def get_dados_populacionais_municipios():
    arquivo = 'municipios.xlsx'
    return ler_arquivo(arquivo,
                       f"https://sidra.ibge.gov.br/geratabela?format=xlsx&name={arquivo}&terr=NC&rank=-&query=t/6579/"
                       f"n6/all/v/all/p/last%201/l/v,p,t")

def get_dados_populacionais_ufs():
    arquivo = 'ufs.xlsx'
    return ler_arquivo(arquivo,
                       f"https://sidra.ibge.gov.br/geratabela?format=xlsx&name={arquivo}&terr=NC&rank=-&query=t/6579/"
                       f"n3/all/v/all/p/last%201/l/v,p,t")

def ler_arquivo(arquivo, url):
    if not os.path.exists(arquivo):
        urllib.request.urlretrieve(url, arquivo)
    df = pd.read_excel(arquivo, sheet_name="Tabela", header=3)
    return df


if __name__ == '__main__':
    get_dados_populacionais_municipios()
