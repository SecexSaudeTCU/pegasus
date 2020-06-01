###########################################################################################################################
# SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN #
###########################################################################################################################

import os
from datetime import datetime

import numpy as np
import pandas as pd

from transform.extract.download_SINAN import download_SINANXXaa, download_table_dbf, download_table_cnv


"""
Módulo de limpeza/tratamento de dados do SINAN.

"""

# Função para converter um "value" num certo "type" de objeto ou caso não seja possível utiliza o valor "default"
def tryconvert(value, default, type):
    try:
        return type(value)
    except (ValueError, TypeError):
        return default


# Classe de dados principais do SINAN
class DataSinanMain:

    # Construtor
    def __init__(self, base, state, year):
        self.base = base
        self.state = state
        self.year = year


    # Método para ler como um objeto pandas DataFrame um arquivo principal de dados do SINAN e adequar e formatar suas...
    # colunas e valores
    def get_SINANXXaamm_treated(self):
        # Lê o arquivo "dbc" ou "parquet", se já tiver sido baixado, como um objeto pandas DataFrame
        dataframe = download_SINANXXaa(self.base, self.state, self.year)
        print(f'O número de linhas do arquivo {self.base}{self.state}{self.year} é {dataframe.shape[0]}.')

        ###################################################################################################################
        # SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG #
        ###################################################################################################################
        if self.base == 'DENG':
            # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela dengbr da base de dados
            lista_columns = np.array(['NU_NOTIFIC', 'TP_NOT', 'ID_AGRAVO', 'ID_MUNICIP', 'RES_CHIKS1', 'RES_CHIKS2',
                                      'RESUL_PRNT', 'RESUL_SORO', 'RESUL_NS1', 'SOROTIPO', 'HOSPITALIZ', 'CLASSI_FIN',
                                      'EVOLUCAO', 'GRAV_PULSO',  'GRAV_CONV', 'GRAV_ENCH',  'GRAV_INSUF', 'GRAV_TAQUI',
                                      'GRAV_EXTRE', 'GRAV_HIPOT', 'GRAV_HEMAT', 'GRAV_MELEN', 'GRAV_METRO', 'GRAV_SANG',
                                      'GRAV_AST', 'GRAV_MIOC', 'GRAV_CONSC', 'GRAV_ORGAO'])

            # Criação de um objeto pandas DataFrame vazio com as colunas especificadas acima
            df = pd.DataFrame(columns=lista_columns)

            # Colocação dos dados da variável "dataframe" na variável "df" nas colunas de mesmo nome preenchendo...
            # automaticamente com o float NaN as colunas da variável "df" não presentes na variável dataframe
            for col in df.columns.values:
                for coluna in dataframe.columns.values:
                    if coluna == col:
                        df[col] = dataframe[coluna].tolist()
                        break

            # Coloca na variável "dif_set" o objeto array dos nomes das colunas da variável "df" que não estão...
            # presentes na variável "dataframe"
            dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

            # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
            for col in dif_set:
                df[col].replace(np.nan, '', inplace=True)

            # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de...
            # controle do código do Município
            # Foi detectado que para alguns Municípios o cálculo do dígito de controle não é válido
            # Esse dígito de controle esteve presente nos arquivos DOXXxxxx até o ano de 2005 (a confirmar!)
            if len(df.loc[0, 'ID_MUNICIP']) == 7:
                df['ID_MUNICIP'].replace(regex='.$',value='', inplace=True)

            # Atualiza/corrige os labels das colunas especificadas
            df['ID_MUNICIP'] = df['ID_MUNICIP'].apply(lambda x: x if len(x) == 6 else '')
            df['ID_MUNICIP'].replace(['000000', '150475', '207540', '207695', '241005',
                                      '282580', '279982', '282586', '292586', '315205',
                                      '321213', '355038', '405028', '421265', '422000',
                                      '431454', '500627', '596382', '613167', '613592',
                                      '990010', '990014', '999999'], '', inplace=True)
            df['ID_MUNICIP'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df['ID_MUNICIP'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df['ID_MUNICIP'].replace(['530000', '530100', '530200', '530300', '530400',
                                      '530500', '530600', '530700', '530800', '530900',
                                      '531000', '531200', '531300', '531400', '531500',
                                      '531600', '531700', '531800'] + \
                                      [str(i) for i in range(539900, 540000)], '530010', inplace=True)

            df['CLASSI_FIN'].replace(['0', '6', '7', '9'], '', inplace=True)
            df['EVOLUCAO'].replace(['0', '9'], '', inplace=True)

            # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
            for col in ['ID_MUNICIP', 'CLASSI_FIN']:
                df[col].replace('', 'NA', inplace=True)

            # Substitui uma string vazia por None nas colunas de atributos especificadas
            for col in ['TP_NOT', 'ID_AGRAVO', 'RES_CHIKS1', 'RES_CHIKS2', 'RESUL_PRNT',
                        'RESUL_SORO', 'RESUL_NS1', 'SOROTIPO', 'HOSPITALIZ', 'EVOLUCAO']:
                df[col].replace('', None, inplace=True)

            # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
            for col in np.array(['GRAV_PULSO', 'GRAV_CONV', 'GRAV_ENCH', 'GRAV_INSUF',
                                 'GRAV_TAQUI', 'GRAV_EXTRE', 'GRAV_HIPOT', 'GRAV_HEMAT',
                                 'GRAV_MELEN', 'GRAV_METRO', 'GRAV_SANG', 'GRAV_AST',
                                 'GRAV_MIOC', 'GRAV_CONSC', 'GRAV_ORGAO']):
                df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

            # Renomeia colunas
            df.rename(index=str, columns={'ID_MUNICIP': 'MUNICIP_ID', 'CLASSI_FIN': 'CLASSIFIN_ID'}, inplace=True)

            print(f'Tratou o arquivo DENG{self.state}{self.year} (shape final: {df.shape[0]} x {df.shape[1]}).')

        return df


# Classe de dados auxiliares do SINAN
class DataSinanAuxiliary:

    # Construtor
    def __init__(self, path):
        self.path = path


    ###########################################################################################################################
    # SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN SINAN #
    ###########################################################################################################################




    ###################################################################################################################
    # SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG SINAN_DENG #
    ###################################################################################################################
    # Função para adequar e formatar as colunas e valores da Tabela TABUF (arquivo TABUF.dbf)
    def get_TABUF_treated(self):
        # Conversão da Tabela TABUF para um objeto pandas DataFrame
        file_name = 'TABUF'
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'CODIGO': 'ID', 'DESCRICAO': 'ESTADO'}, inplace=True)
        # Reordena as colunas
        df = df[['ID', 'ESTADO', 'SIGLA_UF']]
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE', '?']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela RSAUDE (do IBGE)
    def get_RSAUDE_treated(self):
        # Conversão da Tabela RSAUDE (em formato "xlsx") para um objeto pandas DataFrame
        df = pd.read_excel(self.path + 'RSAUDE' + '.xlsx')
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'REGIAO'}, inplace=True)
        # Converte para string a coluna especificada
        df['ID'] = df['ID'].astype('str')
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Método para adequar e formatar as colunas e valores da Tabela CADMUN (arquivo CADMUN.dbf)
    def get_CADMUN_treated(self):
        # Conversão da Tabela CADMUN para um objeto pandas DataFrame
        file_name = 'CADMUN'
        df1 = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df1.rename(index=str, columns={'MUNCOD': 'ID', 'UFCOD': 'UFCOD_ID'}, inplace=True)
        # Drop a linha inteira em que a coluna "ID" tem o valor especificado por não representar nenhum município
        df1 = df1.drop(df1[df1['ID']=='000000'].index)
        # Remove colunas indesejáveis do objeto pandas DataFrame
        df1 = df1.drop(['MUNSINON', 'MUNSINONDV', 'MESOCOD', 'MICROCOD', 'MSAUDCOD',
                        'RSAUDCOD', 'CSAUDCOD', 'RMETRCOD', 'AGLCOD'], axis=1)
        # Substitui uma string vazia pela string "?" nas colunas especificadas
        for col in ['SITUACAO', 'MUNSINP', 'MUNSIAFI', 'MUNNOME', 'MUNNOMEX', 'OBSERV',
                    'AMAZONIA', 'FRONTEIRA', 'CAPITAL', 'ANOINST', 'ANOEXT', 'SUCESSOR']:
            df1[col].replace('', '?', inplace=True)
        # Substitui uma string vazia pela string "NA" nas colunas especificadas
        df1['UFCOD_ID'].replace('', 'NA', inplace=True)
        # Substitui uma string vazia pelo float "NaN" nas colunas especificadas
        for col in ['LATITUDE', 'LONGITUDE', 'ALTITUDE', 'AREA']:
            df1[col].replace('', np.nan, inplace=True)
        # Converte do tipo object para float as colunas especificadas
        df1[['LATITUDE', 'LONGITUDE', 'ALTITUDE', 'AREA']] = \
        df1[['LATITUDE', 'LONGITUDE', 'ALTITUDE', 'AREA']].astype('float')
        # Coloca todas as string das colunas especificadas como UPPER CASE
        df1['MUNNOME'] = df1['MUNNOME'].apply(lambda x: x.upper())
        df1['MUNNOMEX'] = df1['MUNNOMEX'].apply(lambda x: x.upper())
        # Insere uma linha referente ao Município de Nazária/PI não constante originalmente do arquivo
        df1.loc[df1.shape[0]] = ['220672', '2206720', 'ATIVO', '?', '?', 'NAZÁRIA', 'NAZARIA', '?',
                                 'N', 'N', 'N', '22', '?', '?', '?', np.nan, np.nan, np.nan, 363.589]
        # Ordena as linhas de "df1" por ordem crescente dos valores da coluna ID
        df1.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à exclusão e inclusão das linhas referidas acima
        df1.reset_index(drop=True, inplace=True)
        # Conversão da Tabela rl_municip_regsaud para um objeto pandas DataFrame
        file_name = 'rl_municip_regsaud'
        df2 = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df2.rename(index=str, columns={'CO_MUNICIP': 'ID', 'CO_REGSAUD': 'RSAUDE_ID'}, inplace=True)
        # Faz o merge de "df1" e "df2" pela coluna ID tendo por base "df1"
        df = pd.merge(df1, df2, how='left', left_on='ID', right_on='ID')
        # Converte o float NaN para a string "NA"
        df['RSAUDE_ID'].replace(np.nan, 'NA', inplace=True)
        # Reordena as colunas priorizando as "mais" relevantes
        df = df[['ID', 'MUNNOME', 'MUNNOMEX', 'MUNCODDV', 'OBSERV', 'SITUACAO', 'MUNSINP',
                 'MUNSIAFI', 'UFCOD_ID', 'AMAZONIA', 'FRONTEIRA', 'CAPITAL', 'RSAUDE_ID',
                 'LATITUDE', 'LONGITUDE', 'ALTITUDE', 'AREA', 'ANOINST', 'ANOEXT', 'SUCESSOR']]
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE', '?', '?', '?', '?', '?', '?', 'NA', '?',
                               '?', '?', 'NA', np.nan, np.nan, np.nan, np.nan, '?', '?', '?']
        return df


    # Função para adequar e formatar as colunas e valores da TCC Classdeng (arquivo Classdeng.cnv)
    def get_Classdeng_treated(self):
        # Conversão da TCC Classdeng para um objeto pandas DataFrame
        file_name = 'Classdeng'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'CLASSIFICACAO'}, inplace=True)

        # Converte para string a coluna especificada
        df['ID'] = df['ID'].astype('str')
        # Inserção de umas linhas no objeto pandas DataFrame
        df.loc[df.shape[0]] = ['10', 'DENGUE']
        df.loc[df.shape[0]] = ['11', 'DENGUE COM SINAIS DE ALARME']
        df.loc[df.shape[0]] = ['12', 'DENGUE GRAVE']
        df.loc[df.shape[0]] = ['13', 'CHIKUNGUNYA']
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df



if __name__ == '__main__':

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
