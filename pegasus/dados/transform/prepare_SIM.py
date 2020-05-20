###########################################################################################################################
# SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM #
###########################################################################################################################

import os
from datetime import datetime

import numpy as np
import pandas as pd

from transform.extract.download_SIM import download_DOXXaaaa, download_table_dbf, download_table_cnv


"""
Módulo de limpeza/tratamento de dados do SIM.

"""

# Função para converter um "value" num certo "type" de objeto ou caso não seja possível utiliza o valor "default"
def tryconvert(value, default, type):
    try:
        return type(value)
    except (ValueError, TypeError):
        return default


# Classe de dados principais do SIM
class DataSimMain:

    # Construtor
    def __init__(self, state, year):
        self.state = state
        self.year = year


    # Método para ler como um objeto pandas DataFrame o arquivo principal de dados do SIM e adequar e formatar suas...
    # colunas e valores
    def get_DOXXaaaa_treated(self):
        # Lê o arquivo "dbc" ou "parquet", se já tiver sido baixado, como um objeto pandas DataFrame
        dataframe = download_DOXXaaaa(self.state, self.year)
        print(f'O número de linhas do arquivo DO{self.state}{self.year} é {dataframe.shape[0]}.')

        for coluna in dataframe.columns.values:
            dataframe[coluna] = dataframe[coluna].apply(lambda x: x if '\x00' not in x else '')

        # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela dobr
        lista_columns = np.array(['NUMERODO', 'CODINST', 'TIPOBITO', 'DTOBITO', 'HORAOBITO', 'NUMSUS',
                                  'NATURAL',  'CODMUNNATU', 'DTNASC', 'IDADE', 'SEXO', 'RACACOR', 'ESTCIV',
                                  'ESC', 'ESC2010', 'OCUP', 'CODMUNRES', 'LOCOCOR', 'CODESTAB', 'CODMUNOCOR',
                                  'TPMORTEOCO', 'ASSISTMED', 'EXAME', 'CIRURGIA', 'NECROPSIA', 'LINHAA',
                                  'LINHAB', 'LINHAC', 'LINHAD', 'LINHAII', 'CAUSABAS', 'CRM', 'DTATESTADO',
                                  'CIRCOBITO', 'ACIDTRAB', 'FONTE', 'TPPOS', 'DTINVESTIG', 'CAUSABAS_O',
                                  'DTCADASTRO', 'ATESTANTE', 'FONTEINV', 'DTRECEBIM', 'ATESTADO', 'ESCMAEAGR1',
                                  'ESCFALAGR1', 'STDOEPIDEM', 'STDONOVA', 'DIFDATA', 'DTCADINV', 'TPOBITOCOR',
                                  'DTCONINV', 'FONTES'])

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

        # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do...
        # código do município
        # Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
        # Esse dígito de controle esteve presente nos arquivos DOXXxxxx até o ano de 2005 (a confirmar!)
        if len(df.loc[0, 'CODMUNNATU']) == 7:
            df['CODMUNNATU'].replace(regex='.$',value='', inplace=True)
        if len(df.loc[0, 'CODMUNRES']) == 7:
            df['CODMUNRES'].replace(regex='.$',value='', inplace=True)
        if len(df.loc[0, 'CODMUNOCOR']) == 7:
            df['CODMUNOCOR'].replace(regex='.$',value='', inplace=True)

        # Simplifica/corrige a apresentação dos dados das colunas especificadas
        df['HORAOBITO'] = df['HORAOBITO'].apply(lambda x: x[:4] if len(x) > 4 else x)

        df['NATURAL'] = df['NATURAL'].apply(lambda x: x.zfill(3))

        df['OCUP'] = df['OCUP'].apply(lambda x: x.zfill(6))
        df['OCUP'] = df['OCUP'].apply(str.strip)
        df['OCUP'] = df['OCUP'].apply(lambda x: x if len(x) == 6 else '')

        df['CODESTAB'] = df['CODESTAB'].apply(lambda x: x.zfill(7))

        for col in np.array(['ESCMAEAGR1', 'ESCFALAGR1']):
            for i in np.array(['00', '01', '02', '03', '04', '05', '06', '07', '08', '09']):
                df[col].replace(i, str(int(i)), inplace=True)

        # Atualiza/corrige os labels das colunas especificadas
        df['NATURAL'].replace(['000', '999'], '', inplace=True)
        df['NATURAL'].replace('800', '001', inplace=True)
        df['NATURAL'].replace(['00.', '8s9'], '', inplace=True)

        for col in np.array(['DTOBITO', 'DTNASC']):
            df[col] = df[col].apply(lambda x: x if len(x) == 8 else '')
            df[col] = df[col].apply(lambda x: x if ' ' not in x else '')
            df[col] = df[col].apply(lambda x: x if '/' not in x else '')
            df[col] = df[col].apply(lambda x: x if '¾' not in x else '')
            df[col] = df[col].apply(lambda x: x if 'ó' not in x else '')
            df[col] = df[col].apply(lambda x: x if 1 <= tryconvert(x[0:2], 0, int) <= 31 else '')
            df[col] = df[col].apply(lambda x: x if 1 <= tryconvert(x[2:4], 0, int) <= 12 else '')

        for col in np.array(['CODMUNNATU', 'CODMUNRES', 'CODMUNOCOR']):
            df[col].replace(['000000', '150475', '421265', '422000', '431454',
                             '500627', '990002', '990010', '990014', '999999'], '', inplace=True)
            df[col].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df[col].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df[col].replace(['530000', '530500', '530600', '530800', '530900', '531700', '539901',
                             '539902', '539904', '539905', '539906', '539907', '539914', '539916',
                             '539918', '539919', '539920', '539921', '539924', '539925'], '530010', inplace=True)

        df['SEXO'].replace('1', 'M', inplace=True) # Label "M" de Masculino
        df['SEXO'].replace('2', 'F', inplace=True) # Label "F" de Feminino
        df['SEXO'].replace('0', '3', inplace=True)
        df['SEXO'].replace('3', 'IN', inplace=True) # Label "IN" de INdefinido

        df['ESTCIV'].replace(['²', '±'], '', inplace=True)

        df['ESC'].replace('A', '', inplace=True)

        df['OCUP'] = df['OCUP'].apply(lambda x: x if ' ' not in x else '')
        df['OCUP'] = df['OCUP'].apply(lambda x: x if '.' not in x else '')
        df['OCUP'] = df['OCUP'].apply(lambda x: x if '+' not in x else '')
        df['OCUP'] = df['OCUP'].apply(lambda x: x if 'X' not in x else '')
        df['OCUP'].replace('000000', '', inplace=True)

        df['CODESTAB'].replace('0000000', '', inplace=True)
        df['CODESTAB'].replace('2306840', '2461234', inplace=True)
        df['CODESTAB'].replace('2464276', '2726688', inplace=True)
        df['CODESTAB'].replace('2517825', '3563308', inplace=True)
        df['CODESTAB'].replace('2772299', '2465140', inplace=True)
        df['CODESTAB'].replace('3064115', '3401928', inplace=True)

        df['TPMORTEOCO'].replace('8', '6', inplace=True)
        df['TPMORTEOCO'].replace('9', '7', inplace=True)

        for col in np.array(['ASSISTMED', 'EXAME', 'CIRURGIA', 'NECROPSIA', 'ACIDTRAB']):
            df[col].replace(['0', '3', '4', '5', '6', '7', '8', '9'], '', inplace=True)
            df[col].replace('2', '0', inplace=True) # "2", representativo de "Não", é convertido para o objeto...
                                                    # string "0" do domínio binário

        for col in np.array(['CAUSABAS', 'CAUSABAS_O']):
            df[col].replace('B501', 'B508', inplace=True)
            df[col].replace('B656', 'B653', inplace=True)
            df[col].replace('C141', 'C140', inplace=True)
            df[col].replace('M723', 'M724', inplace=True)
            df[col].replace('M725', 'M728', inplace=True)
            df[col].replace('N975', 'N978', inplace=True)
            df[col].replace('Q314', 'P288', inplace=True)
            df[col].replace('Q350', 'Q351', inplace=True)
            df[col].replace('Q352', 'Q353', inplace=True)
            df[col].replace('Q354', 'Q355', inplace=True)
            df[col].replace(['Q356', 'Q358'], 'Q359', inplace=True)
            df[col].replace('R500', 'R508', inplace=True)
            df[col].replace('R501', 'R500', inplace=True)
            df[col].replace(['X590', 'X591', 'X592', 'X593', 'X594',
                             'X595', 'X596', 'X597', 'X598'], 'X599', inplace=True)
            df[col].replace('Y34', 'Y349', inplace=True)
            df[col].replace('Y447', 'Y448', inplace=True)

        df['CAUSABAS_O'].replace(regex='.$',value='', inplace=True)

        df['TPPOS'].replace('2', '0', inplace=True) # "2", representativo de "Não", é convertido para o objeto...
                                                    # string "0" do domínio binário

        df['DTATESTADO'].replace('09201608', '', inplace=True)
        df['DTATESTADO'] = df['DTATESTADO'].apply(lambda x: x if len(x) == 8 else '')
        df['DTATESTADO'] = df['DTATESTADO'].apply(lambda x: x if x[2:4] != '20' else '')

        df['CIRCOBITO'].replace(['á', 'ß', 'C'], '', inplace=True)

        for col in np.array(['ESCMAEAGR1', 'ESCFALAGR1']):
            df[col].replace('9', '', inplace=True)

        df['TPOBITOCOR'].replace('0', '', inplace=True)

        # Atribui um único label para uma mesma significação nas colunas especificadas
        df['TIPOBITO'].replace(['0', '3', '4', '5', '6', '7', '8' '9'], '', inplace=True)

        for col in np.array(['RACACOR', 'ESTCIV', 'ESC', 'LOCOCOR', 'ATESTANTE']):
            df[col].replace(['0', '6', '7', '8', '9'], '', inplace=True)

        df['ESC2010'].replace(['6', '7', '8', '9'], '', inplace=True)

        df['TPMORTEOCO'].replace(['0', '7', '8', '9'], '', inplace=True)

        for col in np.array(['CIRCOBITO', 'FONTE']):
            df[col].replace(['0', '5', '6', '7', '8', '9'], '', inplace=True)

        df['FONTEINV'].replace(['0', '9'], '', inplace=True)

        # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
        # A coluna FONTES é apenas considerada como tal pois recebe tratamento específico mais adiante
        for col in np.array(['TIPOBITO', 'NATURAL', 'CODMUNNATU', 'RACACOR',
                             'ESTCIV', 'ESC', 'ESC2010', 'OCUP', 'CODMUNRES',
                             'LOCOCOR', 'CODESTAB', 'CODMUNOCOR', 'TPMORTEOCO',
                             'CAUSABAS', 'CIRCOBITO', 'FONTE', 'CAUSABAS_O',
                             'ATESTANTE', 'FONTEINV', 'ESCMAEAGR1', 'ESCFALAGR1',
                             'TPOBITOCOR', 'FONTES']):
            df[col].replace('', 'NA', inplace=True)

        # Substitui uma string vazia por None nas colunas de atributos especificadas
        for col in np.array(['CODINST', 'HORAOBITO', 'NUMSUS', 'SEXO', 'LINHAA',
                             'LINHAB', 'LINHAC', 'LINHAD', 'LINHAII', 'CRM', 'ATESTADO']):
            df[col].replace('', None, inplace=True)

        # Divisão da coluna "FONTES" em seis colunas conforme Dicionário de Dados da Tabela DOM ("M" de...
        # investigação materna)
        df['FONTES'] = df['FONTES'].apply(lambda x: x if len(x) == 6 else x)
        for col in np.array(['FONTENTREV', 'FONTEAMBUL', 'FONTEPRONT', 'FONTESVO', 'FONTEIML', 'FONTEPROF']):
            df[col] = df['FONTES'].apply(lambda x: 'NA' if x == 'NA' else x[0]) # O valor quando a condição...
                                                                                # "else" se verifica é "S" de "Sim"
            df[col].replace('X', '0', inplace=True) # Substitui a string "X" por "0" de "Não" tornando a...
                                                    # coluna "col" com domínio "binário"
            df[col].replace('S', '1', inplace=True) # Substitui a string "X" por "1" de "Sim" tornando a...
                                                    # coluna "col" com domínio "binário"

        # Eliminação da coluna "FONTES" por se tornar desnecessária com a adição das seis colunas especificadas acima
        df.drop('FONTES', axis=1, inplace=True)
        # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes ("NaT") pela...
        # data futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
        for col in np.array(['DTOBITO', 'DTNASC', 'DTATESTADO', 'DTINVESTIG',
                             'DTCADASTRO', 'DTRECEBIM', 'DTCADINV', 'DTCONINV']):
            df[col] = df[col].apply(lambda x: datetime.strptime(x, '%d%m%Y').date() \
                                    if x != '' else datetime(2099, 1, 1).date())

        # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui pela...
        # data futura "2099-01-01"
        for col in np.array(['DTOBITO', 'DTATESTADO', 'DTINVESTIG',
                             'DTCADASTRO', 'DTRECEBIM', 'DTCADINV', 'DTCONINV']):
            df[col] = df[col].apply(lambda x: x if datetime(2000, 12, 31).date() < x < \
                                    datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

        df['DTNASC'] = df['DTNASC'].apply(lambda x: x if datetime(1850, 12, 31).date() < x < \
                                          datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

        # Computa a diferença entre as datas de óbito e de nascimento em dias e a aloca como a coluna "IDADE"...
        # do objeto pandas DataFrame
        df['IDADE'] = df['DTOBITO'] - df['DTNASC']

        # Converte os valores da coluna IDADE de datetime.timedelta para string
        # Ainda na mesma linha, cria uma lista de dois objetos string de cada valor da coluna IDADE e aproveita...
        # apenas o primeiro objeto de cada lista
        df['IDADE'] = df['IDADE'].apply(lambda x: str(x).split(' day')[0])

        # Os valores em que a operação anterior forneceu a string "0:00:00" são substituídos pela string...
        # "0" (RN que viveram menos de um dia)
        df['IDADE'].replace('0:00:00', '0', inplace=True)

        # Converte os valores da coluna IDADE de string para float (em dias) atribuindo o float NaN para as...
        # string que começam com "-"
        df['IDADE'] = df['IDADE'].apply(lambda x: np.nan if x[0] == '-' else float(x))

        # Transforma o valor da coluna referida de dias para anos mantendo cinco casas decimais
        df['IDADE']=df['IDADE'].div(365).round(5)

        # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
        for col in np.array(['ASSISTMED', 'EXAME', 'CIRURGIA', 'NECROPSIA', 'ACIDTRAB',
                             'TPPOS', 'STDOEPIDEM', 'STDONOVA', 'FONTENTREV', 'FONTEAMBUL',
                             'FONTEPRONT', 'FONTESVO', 'FONTEIML', 'FONTEPROF']):
            df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

        # Converte do tipo object para float sem casas decimais as colunas de atributos de valores...
        # representativos de quantidades ou para o valor None caso a coluna esteja com a string vazia
        df['DIFDATA'] = df['DIFDATA'].apply(lambda x: round(float(x),0) if x != '' else None)

        # Reordena o objeto pandas DataFrame por ordem crescente de valores da coluna DTOBITO
        df.sort_values(by=['DTOBITO'], inplace=True)

        # Renumera os índices devido à alteração efetivada no passo anterior
        df.reset_index(drop=True, inplace=True)

        # Renomeia colunas
        df.rename(index=str, columns={'TIPOBITO': 'TIPOBITO_ID', 'NATURAL': 'NATURALE_ID',
                                      'CODMUNNATU': 'CODMUNNATU_ID', 'RACACOR': 'RACACOR_ID',
                                      'ESTCIV': 'ESTCIV_ID', 'ESC': 'ESC_ID',
                                      'ESC2010': 'ESC2010_ID', 'OCUP': 'OCUP_ID',
                                      'CODMUNRES': 'CODMUNRES_ID', 'LOCOCOR': 'LOCOCOR_ID',
                                      'CODESTAB': 'CODESTAB_ID', 'CODMUNOCOR': 'CODMUNOCOR_ID',
                                      'TPMORTEOCO': 'TPMORTEOCO_ID', 'CAUSABAS': 'CAUSABAS_ID',
                                      'CIRCOBITO': 'CIRCOBITO_ID', 'FONTE': 'FONTE_ID',
                                      'CAUSABAS_O': 'CAUSABAS_O_ID', 'ATESTANTE': 'ATESTANTE_ID',
                                      'FONTEINV': 'FONTEINV_ID', 'ESCMAEAGR1': 'ESCMAEAGR1_ID',
                                      'ESCFALAGR1': 'ESCFALAGR1_ID', 'TPOBITOCOR': 'TPOBITOCOR_ID'}, inplace=True)

        print(f'Tratou o arquivo DO{self.state}{self.year} (shape final: {df.shape[0]} x {df.shape[1]}).')

        return df


# Classe de dados auxiliares do SIM
class DataSimAuxiliary:

    # Construtor
    def __init__(self, path):
        self.path = path


    # Função para adequar e formatar as colunas e valores da TCC TIPOBITO (arquivo TIPOBITO.cnv)
    def get_TIPOBITO_treated(self):
        # Conversão da TCC TIPOBITO para um objeto pandas DataFrame
        file_name = 'TIPOBITO'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC NAT1212 (arquivo NAT1212.cnv)
    def get_NAT1212_treated(self):
        # Conversão da TCC NAT1212 para um objeto pandas DataFrame
        file_name = 'NAT1212'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'LOCAL'}, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df" até formar uma...
        # "string" de tamanho = 3
        df['ID'] = df['ID'].apply(lambda x: x.zfill(3))
        # Upload do arquivo "xlsx" que contém os NATURAL presentes nos arquivos DOXXxxxx (a partir do ano...
        # de 2001) e não presentes na TCC NAT1212. Ou seja, isso parece ser uma falha dos dados do Datasus
        dataframe = pd.read_excel(self.path + 'NATURAL_OUT_NAT1212_ANOS_1997_2017' + '.xlsx')
        # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
        dataframe['ID'] = dataframe['ID'].astype('str')
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "dataframe" até...
        # formar uma "string" de tamanho = 3
        dataframe['ID'] = dataframe['ID'].apply(lambda x: x.zfill(3))
        # Adiciona a coluna "LOCAL" e respectivos valores ao objeto "dataframe"
        dataframe['LOCAL'] = ['NAO PROVIDO NA TCC NAT1212'] * (dataframe.shape[0])
        # Concatenação do objeto "dataframe" ao objeto "df"
        frames = []
        frames.append(df)
        frames.append(dataframe)
        dfinal = pd.concat(frames, ignore_index=True)
        # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        dfinal.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena eventualmente as linhas por ordem crescente dos valores da coluna ID
        dfinal.sort_values(by=['ID'], inplace=True)
        # Reset eventualmente o index devido ao sorting prévio e à eventual eliminação de duplicates
        dfinal.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar célula de...
        # string vazia da coluna "NATURAL_ID" da tabela DOBR
        dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
        return dfinal


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


    # Função para adequar e formatar as colunas e valores da TCC RACA (arquivo RACA.cnv)
    def get_RACA_treated(self):
        # Conversão da TCC RACA para um objeto pandas DataFrame
        file_name = 'RACA'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df

    # Função para adequar e formatar as colunas e valores da TCC ESTCIV (arquivo ESTCIV.cnv)
    def get_ESTCIV_treated(self):
        # Conversão da TCC ESTCIV para um objeto pandas DataFrame
        file_name = 'ESTCIV'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'SITUACAO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC INSTRUC (arquivo INSTRUC.cnv)
    def get_INSTRUC_treated(self):
        # Conversão da TCC INSTRUC para um objeto pandas DataFrame
        file_name = 'INSTRUC'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'FAIXA_DE_ANOS_INSTRUCAO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC ESCSERIE (arquivo ESCSERIE.cnv)
    def get_ESCSERIE_treated(self):
        # Conversão da TCC ESCSERIE para um objeto pandas DataFrame
        file_name = 'ESCSERIE'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'ESCOLARIDADE'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela TABOCUP (arquivo TABOCUP.dbf)
    # e das TCC CBO2002 e OCUPA (arquivos CBO2002.cnv e OCUPA.cnv, respectivamente)
    def get_TABOCUP_2TCC_treated(self):
        # Conversão da Tabela TABOCUP para um objeto pandas DataFrame
        file_name = 'TABOCUP'
        df1 = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        df1.rename(index=str, columns={'CODIGO': 'ID', 'DESCRICAO': 'OCUPACAO'}, inplace=True)
        # Ordena as linhas de "df1" por ordem crescente dos valores da coluna ID
        df1.sort_values(by=['ID'], inplace=True)
        # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df1.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Reset o index devido ao sorting prévio
        df1.reset_index(drop=True, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df1" até formar...
        # uma "string" de tamanho = 6
        df1['ID'] = df1['ID'].apply(lambda x: x.zfill(6))
        # Conversão da TCC CBO2002 para um objeto pandas DataFrame
        file_name = 'CBO2002'
        df2 = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df2.rename(index=str, columns={'SIGNIFICACAO': 'OCUPACAO'}, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df2" até formar...
        # uma "string" de tamanho = 6
        df2['ID'] = df2['ID'].apply(lambda x: x.zfill(6))
        # Conversão da TCC OCUPA para um objeto pandas DataFrame
        file_name = 'OCUPA'
        df3 = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df3.rename(index=str, columns={'SIGNIFICACAO': 'OCUPACAO'}, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df3" até formar...
        # uma "string" de tamanho = 6
        df3['ID'] = df3['ID'].apply(lambda x: x.zfill(6))
        # Concatena os três objetos pandas DataFrame
        frames = []
        frames.append(df1)
        frames.append(df2)
        frames.append(df3)
        df = pd.concat(frames, ignore_index=True)
        # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas por ordem crescente dos valores da coluna "ID"
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
        df.reset_index(drop=True, inplace=True)
        # Upload do arquivo "xlsx" que contém os OCUP presentes nos arquivos DOXXaaaa (dos anos de...
        #  1997 a 2017) e não presentes na Tabela TABOCUP e nas TCC CBO2002 e OCUPA. Ou seja, isso...
        # parece ser uma falha dos dados do Datasus
        dataframe = pd.read_excel(self.path + 'OCUP_OUT_TABOCUP_E_2TCC_ANOS_1997_2017' + '.xlsx')
        # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
        dataframe['ID'] = dataframe['ID'].astype('str')
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "dataframe" até formar...
        # uma "string" de tamanho = 6
        dataframe['ID'] = dataframe['ID'].apply(lambda x: x.zfill(6))
        # Adiciona a coluna "OCUPACAO" e respectivos valores ao objeto "dataframe"
        dataframe['OCUPACAO'] = ['NAO PROVIDO EM TABOCUP.DBF E NAS TCC CBO2002/OCUPA'] * (dataframe.shape[0])
        # Concatenação do objeto "dataframe" ao objeto "df"
        frames = []
        frames.append(df)
        frames.append(dataframe)
        dfinal = pd.concat(frames, ignore_index=True)
        # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        dfinal.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena eventualmente as linhas por ordem crescente dos valores da coluna ID
        dfinal.sort_values(by=['ID'], inplace=True)
        # Reset eventualmente o index devido ao sorting prévio e à eventual eliminação de duplicates
        dfinal.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
        return dfinal


    # Função para adequar e formatar as colunas e valores da TCC LOCOCOR (arquivo LOCOCOR.cnv)
    def get_LOCOCOR_treated(self):
        # Conversão da TCC LOCOCOR para um objeto pandas DataFrame
        file_name = 'LOCOCOR'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'LUGAR'}, inplace=True)
        # Converte para string a coluna especificada
        df['ID'] = df['ID'].astype('str')
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela CNESDO18 do SIM (arquivo CNESDO18.dbf)...
    # e da TCC ESTAB06 (arquivo ESTAB06.cnv)
    # Além disso faz o "merge" a elas das TCC ESFERA e NAT_ORG (arquivos ESFERA.cnv e NAT_ORG.cnv, respectivamente)
    def get_CNESDO18_3TCC_treated(self):
        # Conversão da Tabela CNESDO18 para um objeto pandas DataFrame
        file_name = 'CNESDO18'
        df1 = download_table_dbf(file_name)
        # Ordena as linhas de "df1" por ordem crescente dos valores da coluna CODESTAB
        df1.sort_values(by=['CODESTAB'], inplace=True)
        # Elimina eventuais linhas duplicadas tendo por base a coluna CODESTAB e mantém a primeira ocorrência
        df1.drop_duplicates(subset='CODESTAB', keep='first', inplace=True)
        # Reset o index devido ao sorting prévio
        df1.reset_index(drop=True, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "CODESTAB" do objeto "df1" até formar...
        # uma "string" de tamanho = 7
        df1['CODESTAB'] = df1['CODESTAB'].apply(lambda x: x.zfill(7))
        # Conversão da TCC ESTAB06 para um objeto pandas DataFrame
        file_name = 'ESTAB06'
        df2 = download_table_cnv(file_name)
        df2.rename(index=str, columns={'ID': 'CODESTAB', 'SIGNIFICACAO': 'DESCESTAB'}, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "CODESTAB" do objeto "df2" até formar...
        # uma "string" de tamanho = 7
        df2['CODESTAB'] = df2['CODESTAB'].apply(lambda x: x.zfill(7))
        # Concatena os dois objetos pandas DataFrame
        frames = []
        frames.append(df1)
        frames.append(df2)
        df = pd.concat(frames, ignore_index=True)
        # Elimina linhas duplicadas
        df.drop_duplicates(subset='CODESTAB', keep='first', inplace=True)
        # Ordena as linhas por ordem crescente dos valores da coluna "CODESTAB"
        df.sort_values(by=['CODESTAB'], inplace=True)
        # Reseta os índices
        df.reset_index(drop=True, inplace=True)
        # Conversão da TCC ESFERA18 para um objeto pandas DataFrame
        file_name = 'ESFERA18'
        df3 = download_table_cnv(file_name)
        # Adequa e formata a TCC ESFERA18
        df3.rename(index=str, columns={'ID': 'CODESTAB', 'SIGNIFICACAO': 'ESFERA'}, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "CODESTAB" do objeto "df3" até formar...
        # uma "string" de tamanho = 7
        df3['CODESTAB'] = df3['CODESTAB'].apply(lambda x: x.zfill(7))
        # Conversão da TCC NAT_ORG (já em formato "xlsx" e não "cnv") para um objeto pandas DataFrame
        file_name = 'NAT_ORG'
        df4 = download_table_cnv(file_name)
        # Adequa e formata a TCC NAT_ORG
        df4.rename(index=str, columns={'ID': 'CODESTAB', 'SIGNIFICACAO': 'REGIME'}, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "CODESTAB" do objeto "df4" até formar...
        # uma "string" de tamanho = 7
        df4['CODESTAB'] = df4['CODESTAB'].apply(lambda x: x.zfill(7))
        # Realiza o "merge" da TCC ESFERA18 à TCC NAT_ORG
        df5 = df3.append(df4, sort=False)
        df6 = df5.replace(np.nan,'').groupby('CODESTAB',as_index=False).agg(''.join)
        df6.sort_values(by=['CODESTAB'], inplace=True)
        df6.reset_index(drop=True, inplace=True)
        # Realiza o "merge" da TCC ESFERA18 (+ TCC NAT_ORG) à (Tabela CNESDO18 + TCC ESTAB06)
        df7 = df.append(df6, sort=False)
        df8 = df7.replace(np.nan,'').groupby('CODESTAB',as_index=False).agg(''.join)
        df8.sort_values(by=['CODESTAB'], inplace=True)
        df8.reset_index(drop=True, inplace=True)
        # Substitui os valores de string vazia das colunas especificadas pela string "?"
        df8['DESCESTAB'].replace('','?', inplace=True)
        df8['ESFERA'].replace('','?', inplace=True)
        df8['REGIME'].replace('','?', inplace=True)
        # Upload do arquivo "xlsx" que contém os CODESTAB presentes nos arquivos DOXXaaaa (dos anos de...
        # 1997 a 2017) e não presentes na tabela CNESDO18 e nas TCC ESTAB06, ESFERA18 e NAT_ORG. Ou seja,...
        # isso parece ser uma falha dos dados do Datasus
        dataframe = pd.read_excel(self.path + 'CODESTAB_OUT_CNESDO18_E_3TCC_ANOS_1997_2017' + '.xlsx')
        # Converte a coluna "CODESTAB" do objeto "dataframe" de "int" para "string"
        dataframe['CODESTAB'] = dataframe['CODESTAB'].astype('str')
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "CODESTAB" do objeto "dataframe" até formar...
        # uma "string" de tamanho = 7
        dataframe['CODESTAB'] = dataframe['CODESTAB'].apply(lambda x: x.zfill(7))
        # Adiciona as colunas "DESCESTAB", "ESFERA" e "REGIME" e respectivos valores ao objeto "dataframe"
        dataframe['DESCESTAB'] = ['NAO PROVIDO EM CNESDO18.DBF E NAS TCC ESTAB06/ESFERA18/NAT_ORG'] * (dataframe.shape[0])
        dataframe['ESFERA'] = ['?'] * (dataframe.shape[0])
        dataframe['REGIME'] = ['?'] * (dataframe.shape[0])
        # Concatenação do objeto "dataframe" ao objeto "df8"
        frames = []
        frames.append(df8)
        frames.append(dataframe)
        dfinal = pd.concat(frames, ignore_index=True)
        # Renomeia a coluna "CODESTAB"
        dfinal.rename(index=str, columns={'CODESTAB': 'ID'}, inplace=True)
        # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        dfinal.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena eventualmente as linhas por ordem crescente dos valores da coluna ID
        dfinal.sort_values(by=['ID'], inplace=True)
        # Reset eventualmente o index devido ao sorting prévio e à eventual eliminação de duplicates
        dfinal.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE', '?', '?']
        return dfinal


    # Função para adequar e formatar as colunas e valores da Tabela TPMORTEOCO ("constando" apenas...
    # do Dicionário de Dados do SIM)
    def get_TPMORTEOCO_treated(self):
        # Conversão da Tabela TPMORTEOCO (em formato "xlsx") para um objeto pandas DataFrame
        df = pd.read_excel(self.path + 'TPMORTEOCO' + '.xlsx')
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'EPOCA_MORTE'}, inplace=True)
        # Converte para string a coluna especificada
        df['ID'] = df['ID'].astype('str')
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela CID10 (arquivo CID10.dbf) e de...
    # 21 TCC com nome CID10_XX (arquivos "cnv") sendo XX indo de 01 a 21, um para cada capítulo do CID 10.
    def get_CID10_treated(self):
        # Conversão da Tabela CID10 para um objeto pandas DataFrame
        file_name = 'CID10'
        df1 = download_table_dbf(file_name)
        # Remove colunas indesejáveis do objeto pandas DataFrame
        df1 = df1.drop(['OPC', 'CAT', 'SUBCAT', 'RESTRSEXO'], axis=1)
        # Renomeia as colunas especificadas
        df1.rename(index=str, columns={'CID10': 'ID', 'DESCR': 'DOENCA'}, inplace=True)
        # Coloca todas as string da coluna especificada como UPPER CASE
        df1['DOENCA'] = df1['DOENCA'].apply(lambda x: x.upper())
        # Ordena as linhas de "df1" por ordem crescente dos valores da coluna ID
        df1.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio
        df1.reset_index(drop=True, inplace=True)
        # Conversão das 21 TCC CID10_XX para um objeto pandas DataFrame
        frames = []
        for i in range(1, 22):
            i = str(i).zfill(2)
            file_name = 'CID10_' + i
            dfi = download_table_cnv(file_name)
            frames.append(dfi)
        df2 = pd.concat(frames, ignore_index=True)
        df2.drop_duplicates(subset='ID', keep='first', inplace=True)
        df2.sort_values(by=['ID'], inplace=True)
        df2.reset_index(drop=True, inplace=True)
        # Renomeia a coluna SIGNIFICACAO
        df2.rename(index=str, columns={'SIGNIFICACAO': 'DOENCA'}, inplace=True)
        # Concatena os dois objetos pandas DataFrame
        frames = []
        frames.append(df1)
        frames.append(df2)
        df = pd.concat(frames, ignore_index=True)
        # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
        df.reset_index(drop=True, inplace=True)
        # Upload do arquivo "xlsx" que contém os CAUSABAS OU CAUSABAS_O presentes nos arquivos...
        # DOXXaaaa (dos anos de 1997 a 2017) e não presentes na Tabela CID10 ou nas TCC CID10. Ou seja,...
        # isso parece ser uma falha dos dados do Datasus
        dataframe = pd.read_excel(self.path + 'CAUSABAS_OUT_CID10_ANOS_1997_2017' + '.xlsx')
        # Adiciona a coluna "DOENCA" e respectivos valores ao objeto "dataframe"
        dataframe['DOENCA'] = ['NAO PROVIDO EM CID10.DBF E NAS TCC CID10'] * (dataframe.shape[0])
        # Concatenação do objeto "dataframe" ao objeto "df"
        frames = []
        frames.append(df)
        frames.append(dataframe)
        dfinal = pd.concat(frames, ignore_index=True)
        # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        dfinal.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena eventualmente as linhas por ordem crescente dos valores da coluna ID
        dfinal.sort_values(by=['ID'], inplace=True)
        # Reset eventualmente o index devido ao sorting prévio e à eventual eliminação de duplicates
        dfinal.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
        return dfinal


    # Função para adequar e formatar as colunas e valores da TCC TIPOVIOL (arquivo TIPOVIOL.cnv)
    def get_TIPOVIOL_treated(self):
        # Conversão da TCC TIPOVIOL para um objeto pandas DataFrame
        file_name = 'TIPOVIOL'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'CIRCUNSTANCIA'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC FONTINFO (arquivo FONTINFO.cnv)
    def get_FONTINFO_treated(self):
        # Conversão da TCC FONTINFO para um objeto pandas DataFrame
        file_name = 'FONTINFO'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'ORIGEM'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC ATESTANT (arquivo ATESTANT.cnv)
    def get_ATESTANT_treated(self):
        # Conversão da TCC ATESTANT para um objeto pandas DataFrame
        file_name = 'ATESTANT'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'ATESTADOR'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC FONTEINV (arquivo FONTEINV.cnv)
    def get_FONTEINV_treated(self):
        # Conversão da TCC FONTEINV para um objeto pandas DataFrame
        file_name = 'FONTEINV'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'ORIGEM'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC ESCAGR1 (arquivo ESCAGR1.cnv)
    def get_ESCAGR1_treated(self):
        # Conversão da TCC ESCAGR1 para um objeto pandas DataFrame
        file_name = 'ESCAGR1'
        df = download_table_cnv(file_name)
        # Renomeia a coluna "SIGNIFICACAO"
        df.rename(index=str, columns={'SIGNIFICACAO': 'ESCOLARIDADE'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela TPOBITOCOR ("constando" apenas...
    # do Dicionário de Dados do SIM)
    def get_TPOBITOCOR_treated(self):
        # Conversão da Tabela TPOBITOCOR (em formato "xlsx") para um objeto pandas DataFrame
        df = pd.read_excel(self.path + 'TPOBITOCOR' + '.xlsx')
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'EPOCA_MORTE'}, inplace=True)
        # Converte para string a coluna especificada
        df['ID'] = df['ID'].astype('str')
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df



if __name__ == '__main__':

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    # Ajustar o "the_path_..." para a localização dos arquivos "xlsx"
    the_path_Tabelas = os.getcwd()[:-len('\\insertion\\data_wrangling')] + 'files\\SIM\\'
