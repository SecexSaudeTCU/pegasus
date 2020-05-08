import urllib.request
import os
import pandas as pd

def get_dados_populacionais():
    if not os.path.exists('populacoes.xlsx'):
        url = 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6579.xlsx&terr=NC&rank=-&query=t/6579/n6/all/v/all/p/last%201/l/v,p,t'
        urllib.request.urlretrieve(url, 'populacoes.xlsx')
    df = pd.read_excel('populacoes.xlsx', sheet_name="Tabela", header=3)
    return df

if __name__ == '__main__':
    get_dados_populacionais()
