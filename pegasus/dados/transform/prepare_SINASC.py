##########################################################################################################################
# SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC #
##########################################################################################################################

import os
from datetime import datetime

import numpy as np
import pandas as pd

from transform.extract.download_SINASC import download_DNXXaaaa, download_table_dbf, download_table_cnv


"""
Módulo de limpeza/tratamento de dados do SINASC.

"""


# Função para converter um "value" num certo "type" de objeto ou caso não seja possível utiliza o valor "default"
def tryconvert(value, default, type):
    try:
        return type(value)
    except (ValueError, TypeError):
        return default


# Classe de dados principais do SINASC
class DataSinascMain:

    # Construtor
    def __init__(self, state, year):
        self.state = state
        self.year = year


    # Método para ler como um objeto pandas DataFrame o arquivo principal de dados do SINASC e adequar e formatar suas...
    # colunas e valores
    def get_DNXXaaaa_treated(self):
        # Lê o arquivo "dbc" como um objeto pandas DataFrame e o salva no formato "parquet"
        dataframe = download_DNXXaaaa(self.state, self.year)
        print(f'O número de linhas do arquivo DN{self.state}{self.year} é {dataframe.shape[0]}.')

        for coluna in dataframe.columns.values:
            dataframe[coluna] = dataframe[coluna].apply(lambda x: x if '\x00' not in x else '')

        # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela dnbr
        lista_columns = np.array(['NUMERODN', 'CODINST', 'CODESTAB', 'CODMUNNASC', 'LOCNASC', 'IDADEMAE',
                                  'ESTCIVMAE', 'ESCMAE', 'CODOCUPMAE', 'QTDFILVIVO', 'QTDFILMORT', 'CODMUNRES',
                                  'GESTACAO', 'GRAVIDEZ', 'PARTO', 'CONSULTAS', 'DTNASC', 'HORANASC', 'SEXO',
                                  'APGAR1', 'APGAR5', 'RACACOR', 'PESO', 'IDANOMAL', 'DTCADASTRO', 'CODANOMAL',
                                  'DTRECEBIM', 'DIFDATA', 'NATURALMAE', 'CODMUNNATU', 'ESCMAE2010', 'DTNASCMAE',
                                  'RACACORMAE', 'QTDGESTANT', 'QTDPARTNOR', 'QTDPARTCES', 'IDADEPAI', 'DTULTMENST',
                                  'SEMAGESTAC', 'TPMETESTIM', 'CONSPRENAT', 'MESPRENAT', 'TPAPRESENT', 'STTRABPART',
                                  'STCESPARTO', 'TPNASCASSI', 'TPFUNCRESP', 'TPDOCRESP', 'DTDECLARAC', 'ESCMAEAGR1',
                                  'STDNEPIDEM', 'STDNNOVA', 'CODPAISRES', 'TPROBSON', 'PARIDADE', 'KOTELCHUCK'])

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
        # Esse dígito de controle esteve presente nos arquivos DNXXxxxx até o ano de 2005
        if len(df.loc[0, 'CODMUNNASC']) == 7:
            df['CODMUNNASC'].replace(regex='.$',value='', inplace=True)
        if len(df.loc[0, 'CODMUNRES']) == 7:
            df['CODMUNRES'].replace(regex='.$',value='', inplace=True)
        if len(df.loc[0, 'CODMUNNATU']) == 7:
            df['CODMUNNATU'].replace(regex='.$',value='', inplace=True)

        # Simplifica/corrige a apresentação dos dados das colunas especificadas
        df['CODESTAB'] = df['CODESTAB'].apply(lambda x: x.zfill(7))

        df['CODOCUPMAE'] = df['CODOCUPMAE'].apply(lambda x: x.zfill(6))
        df['CODOCUPMAE'] = df['CODOCUPMAE'].apply(str.strip)
        df['CODOCUPMAE'] = df['CODOCUPMAE'].apply(lambda x: x if len(x) == 6 else '')

        df['HORANASC'] = df['HORANASC'].apply(lambda x: x.replace(';','') if ';' in x else x)
        df['HORANASC'] = df['HORANASC'].apply(lambda x: x[:4] if len(x) > 4 else x)

        for col in np.array(['ESCMAEAGR1', 'TPROBSON']):
            for i in np.array(['00', '01', '02', '03', '04', '05', '06', '07', '08', '09']):
                df[col].replace(i, str(int(i)), inplace=True)

        df['CODANOMAL'] = df['CODANOMAL'].apply(lambda x: x[:4] if len(x) > 4 else x)
        df['CODANOMAL'] = df['CODANOMAL'].apply(lambda x: x.replace('X','') if 'X' in x else x)
        df['CODANOMAL'] = df['CODANOMAL'].apply(str.strip)

        df['NATURALMAE'] = df['NATURALMAE'].apply(lambda x: x.zfill(3))

        # Atualiza/corrige os labels das colunas especificadas
        df['CODESTAB'].replace('0000000', '', inplace=True)
        df['CODESTAB'].replace('0000110', '6148425', inplace=True)
        df['CODESTAB'].replace('0000126', '6069134', inplace=True)
        df['CODESTAB'].replace('0000216', '6183948', inplace=True)
        df['CODESTAB'].replace('0000178', '6205224', inplace=True)

        for col in np.array(['CODMUNNASC', 'CODMUNRES', 'CODMUNNATU']):
            df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')
            df[col].replace(['000000', '150475', '241005', '282580', '279982',
                             '282586', '292586', '315205', '421265', '422000',
                             '431454', '500627', '990010', '990014', '999999'], '', inplace=True)
            df[col].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
            df[col].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
            df[col].replace(['530000', '530100', '530200', '530300', '530400',
                             '530500', '530600', '530700', '530800', '530900',
                             '531000', '531200', '531300', '531400', '531500',
                             '531600', '531700', '531800'] + \
                             [str(i) for i in range(539900, 540000)], '530010', inplace=True)

        df['QTDFILVIVO'].replace(['.', '\x06Ò', '\x7f'], '', inplace=True)
        df['QTDFILMORT'].replace(['t', 'b0', 'O\x1e', 'þ\x01', 'â0', '.'], '', inplace=True)

        df['IDADEMAE'].replace('99', '', inplace=True)

        df['CODOCUPMAE'] = df['CODOCUPMAE'].apply(lambda x: x if 'ó' not in x else '')
        df['CODOCUPMAE'] = df['CODOCUPMAE'].apply(lambda x: x if ' ' not in x else '')
        df['CODOCUPMAE'] = df['CODOCUPMAE'].apply(lambda x: x if 'X' not in x else '')
        df['CODOCUPMAE'].replace('000000', '', inplace=True)

        df['DTNASC'] = df['DTNASC'].apply(lambda x: x if '00' not in x[0:2] else '')
        df['DTNASC'] = df['DTNASC'].apply(lambda x: x if '00' not in x[2:4] else '')

        df['SEXO'].replace('1', 'M', inplace=True) # Label "M" de Masculino
        df['SEXO'].replace('2', 'F', inplace=True) # Label "F" de Feminino
        df['SEXO'].replace('0', '3', inplace=True)
        df['SEXO'].replace('3', 'IN', inplace=True) # Label "IN" de INdefinido

        for col in np.array(['APGAR1', 'APGAR5']):
            df[col].replace(['-', '+', '\x109', ' è', '..', '.', ' \x17', ' .', '--'], '', inplace=True)

        df['PESO'] = df['PESO'].apply(lambda x: x if ' ' not in x else '')
        df['PESO'].replace('340\x10', '', inplace=True)


        df['IDANOMAL'].replace(['0', '3', '4', '5', '6', '7', '8', '9'], '', inplace=True)
        df['IDANOMAL'].replace('2', '0', inplace=True) # "2", representativo de "Não", é convertido para o objeto...
                                                       # string "0" do domínio binário

        df['CODANOMAL'].replace('Q314', 'P288', inplace=True)
        df['CODANOMAL'].replace('Q350', 'Q351', inplace=True)
        df['CODANOMAL'].replace('Q352', 'Q353', inplace=True)
        df['CODANOMAL'].replace('Q354', 'Q355', inplace=True)
        df['CODANOMAL'].replace(['Q356', 'Q358'], 'Q359', inplace=True)

        df['NATURALMAE'].replace(['000', '999'], '', inplace=True)
        df['NATURALMAE'].replace('800', '001', inplace=True)

        df['IDADEPAI'].replace('5D', '', inplace=True)

        df['TPDOCRESP'].replace('1', 'CNES', inplace=True)
        df['TPDOCRESP'].replace('2', 'CRM', inplace=True)
        df['TPDOCRESP'].replace('3', 'COREN', inplace=True)
        df['TPDOCRESP'].replace('4', 'RG', inplace=True)
        df['TPDOCRESP'].replace('5', 'CPF', inplace=True)
        df['TPDOCRESP'].replace(['0', '6', '7', '8', '9'], '', inplace=True)

        df['ESCMAEAGR1'].replace('9', '', inplace=True)

        df['CODPAISRES'].replace('1', '', inplace=True)

        df['TPROBSON'].replace('12', '11', inplace=True)

        # Atribui um único label para uma mesma significação nas colunas especificadas
        for col in np.array(['LOCNASC', 'CONSULTAS', 'TPNASCASSI']):
            df[col].replace(['0', '5', '6', '7', '8', '9'], '', inplace=True)

        for col in np.array(['ESTCIVMAE', 'ESCMAE', 'RACACOR', 'RACACORMAE', 'TPFUNCRESP']):
            df[col].replace(['0', '6', '7', '8', '9'], '', inplace=True)

        df['GESTACAO'].replace(['0', '7', '8', '9'], '', inplace=True)

        for col in np.array(['GRAVIDEZ', 'TPAPRESENT', 'STTRABPART', 'STCESPARTO']):
            df[col].replace(['0', '4', '5', '6', '7', '8', '9'], '', inplace=True)

        for col in np.array(['PARTO', 'TPMETESTIM']):
            df[col].replace(['0', '3', '4', '5', '6', '7', '8', '9'], '', inplace=True)

        df['ESCMAE2010'].replace(['6', '7', '8', '9'], '', inplace=True)

        # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
        for col in np.array(['CODESTAB', 'CODMUNNASC', 'LOCNASC', 'ESTCIVMAE', 'ESCMAE',
                             'CODOCUPMAE', 'CODMUNRES', 'GESTACAO', 'GRAVIDEZ', 'PARTO',
                             'CONSULTAS', 'RACACOR', 'CODANOMAL', 'NATURALMAE', 'CODMUNNATU',
                             'ESCMAE2010', 'RACACORMAE', 'TPMETESTIM', 'TPAPRESENT',
                             'STTRABPART', 'STCESPARTO', 'TPNASCASSI', 'TPFUNCRESP',
                             'ESCMAEAGR1', 'CODPAISRES', 'TPROBSON']):
            df[col].replace('', 'NA', inplace=True)

        # Substitui uma string vazia por None nas colunas de atributos especificadas
        for col in np.array(['CODINST', 'HORANASC', 'SEXO',
                             'MESPRENAT', 'TPDOCRESP', 'KOTELCHUCK']):
            df[col].replace('', None, inplace=True)

        # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes ("NaT") pela...
        # data futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
        for col in np.array(['DTNASC', 'DTCADASTRO', 'DTRECEBIM',
                             'DTNASCMAE', 'DTULTMENST', 'DTDECLARAC']):
            df[col] = df[col].apply(lambda x: datetime.strptime(x, '%d%m%Y').date() \
                                    if x != '' else datetime(2099, 1, 1).date())

        # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui pela...
        # data futura "2099-01-01"
        for col in np.array(['DTNASC', 'DTCADASTRO', 'DTRECEBIM', 'DTDECLARAC']):
            df[col] = df[col].apply(lambda x: x if datetime(2000, 12, 31).date() < x < \
                                    datetime(2025, 12, 31).date() else datetime(2099, 1, 1).date())
        df['DTNASCMAE'] = df['DTNASCMAE'].apply(lambda x: x if datetime(1900, 12, 31).date() < x < \
                                                datetime(2025, 12, 31).date() else datetime(2099, 1, 1).date())
        df['DTULTMENST'] = df['DTULTMENST'].apply(lambda x: x if datetime(1990, 12, 31).date() < x < \
                                                  datetime(2025, 12, 31).date() else datetime(2099, 1, 1).date())

        # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
        for col in np.array(['IDANOMAL', 'STDNEPIDEM', 'STDNNOVA', 'PARIDADE']):
            df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

        # Converte do tipo object para float sem casas decimais as colunas de atributos de valores ...
        # representativos de quantidades ou para o valor None caso a coluna esteja com a string vazia
        for col in np.array(['IDADEMAE', 'QTDFILVIVO', 'QTDFILMORT', 'APGAR1', 'APGAR5', 'PESO', 'DIFDATA',
                             'QTDGESTANT', 'QTDPARTNOR', 'QTDPARTCES', 'IDADEPAI', 'SEMAGESTAC', 'CONSPRENAT']):
            df[col] = df[col].apply(lambda x: round(float(x),0) if x != '' else None)

        # Ordena as linhas de "df" por ordem crescente dos valores da coluna DTNASC
        df.sort_values(by=['DTNASC'], inplace=True)

        # Reset o index devido ao sorting prévio
        df.reset_index(drop=True, inplace=True)

        # Renomeia colunas
        df.rename(index=str, columns={'CODESTAB': 'CODESTAB_ID', 'CODMUNNASC': 'CODMUNNASC_ID',
                                      'LOCNASC': 'LOCNASC_ID', 'ESTCIVMAE': 'ESTCIVMAE_ID',
                                      'ESCMAE': 'ESCMAE_ID', 'CODOCUPMAE': 'CODOCUPMAE_ID',
                                      'CODMUNRES': 'CODMUNRES_ID', 'GESTACAO': 'GESTACAO_ID',
                                      'GRAVIDEZ': 'GRAVIDEZ_ID', 'PARTO': 'PARTO_ID',
                                      'CONSULTAS': 'CONSULTAS_ID', 'RACACOR': 'RACACOR_ID',
                                      'CODANOMAL': 'CODANOMAL_ID', 'NATURALMAE': 'NATURALMAE_ID',
                                      'CODMUNNATU': 'CODMUNNATU_ID', 'ESCMAE2010': 'ESCMAE2010_ID',
                                      'RACACORMAE': 'RACACORMAE_ID', 'TPMETESTIM': 'TPMETESTIM_ID',
                                      'TPAPRESENT': 'TPAPRESENT_ID', 'STTRABPART': 'STTRABPART_ID',
                                      'STCESPARTO': 'STCESPARTO_ID', 'TPNASCASSI': 'TPNASCASSI_ID',
                                      'TPFUNCRESP':'TPFUNCRESP_ID', 'ESCMAEAGR1': 'ESCMAEAGR1_ID',
                                      'CODPAISRES': 'CODPAISRES_ID', 'TPROBSON': 'TPROBSON_ID'}, inplace=True)

        print(f'Tratou o arquivo DN{self.state}{self.year} (shape final: {df.shape[0]} x {df.shape[1]}).')

        return df


# Classe de dados auxiliares do SINASC
class DataSinascAuxiliary:

    # Construtor
    def __init__(self, path):
        self.path = path


    # Função para adequar e formatar as colunas e valores da Tabela CNESDN18 do SINASC (arquivo CNDESDN18.dbf)...
    # e da TCC CNESDN07 (arquivo CNESDN07.cnv)
    # Além disso faz o "merge" a elas das TCC ESFEDN07 e NATDN07 (arquivos ESFEDN07.cnv e NATDN07.cnv, respectivamente)
    def get_CNESDN_treated(self):
        # Conversão da Tabela CNESDN18 para um objeto pandas DataFrame
        file_name = 'CNESDN18'
        df1 = download_table_dbf(file_name)
        # Ordena as linhas de "df1" por ordem crescente dos valores da coluna CODESTAB
        df1.sort_values(by=['CODESTAB'], inplace=True)
        # Elimina eventuais linhas duplicadas tendo por base a coluna CODESTAB e mantém a primeira ocorrência
        df1.drop_duplicates(subset='CODESTAB', keep='first', inplace=True)
        # Reset o index devido ao sorting prévio
        df1.reset_index(drop=True, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "CODESTAB" do objeto "df1" até...
        # formar uma "string" de tamanho = 7
        df1['CODESTAB'] = df1['CODESTAB'].apply(lambda x: x.zfill(7))
        # Conversão da TCC CNESDN07 para um objeto pandas DataFrame
        file_name = 'CNESDN07'
        df2 = download_table_cnv(file_name)
        # Renomeia as colunas especificadas
        df2.rename(index=str, columns={'ID': 'CODESTAB', 'SIGNIFICACAO': 'DESCESTAB'}, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "CODESTAB" do objeto "df2" até...
        # formar uma "string" de tamanho = 7
        df2['CODESTAB'] = df2['CODESTAB'].apply(lambda x: x.zfill(7))
        # Concatena os dois objetos pandas DataFrame
        frames = []
        frames.append(df1)
        frames.append(df2)
        df = pd.concat(frames, ignore_index=True)
        # Elimina linhas duplicadas tendo por base a coluna CODESTAB e mantém a primeira ocorrência
        df.drop_duplicates(subset='CODESTAB', keep='first', inplace=True)
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna CODESTAB
        df.sort_values(by=['CODESTAB'], inplace=True)
        # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
        df.reset_index(drop=True, inplace=True)
        # Conversão da TCC ESFEDN07 para um objeto pandas DataFrame
        file_name = 'ESFEDN07'
        df3 = download_table_cnv(file_name)
        # Adequa e formata a TCC ESFEDN07
        df3.rename(index=str, columns={'ID': 'CODESTAB', 'SIGNIFICACAO': 'ESFERA'}, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "CODESTAB" do objeto "df3" até...
        # formar uma "string" de tamanho = 7
        df3['CODESTAB'] = df3['CODESTAB'].apply(lambda x: x.zfill(7))
        # Conversão da TCC NATDN07 para um objeto pandas DataFrame
        file_name = 'NATDN07'
        df4 = download_table_cnv(file_name)
        # Adequa e formata a TCC NATDN07
        df4.rename(index=str, columns={'ID': 'CODESTAB', 'SIGNIFICACAO': 'REGIME'}, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "CODESTAB" do objeto "df4" até...
        # formar uma "string" de tamanho = 7
        df4['CODESTAB'] = df4['CODESTAB'].apply(lambda x: x.zfill(7))
        # Realiza o "merge" da TCC NATDN07 à TCC ESFEDN07
        df3['REGIME'] = df4['REGIME'].tolist() # Isso só é possível corretamente com essa rotina pois o...
                                               # número de linhas e a ordem dos valores das colunas...
                                               # "CODESTAB" dos objetos pandas DataFrame "df3" e "df4" são os mesmos
        # Elimina linhas duplicadas tendo por base a coluna CODESTAB e mantém a primeira ocorrência
        df3.drop_duplicates(subset='CODESTAB', keep='first', inplace=True)
        # Realiza o "merge" da TCC ESFEDN07 (+ TCC NATDN07) à (Tabela CNESDN18 + TCC CNESDN07)
        df5 = df.append(df3, sort=False)
        df6 = df5.replace(np.nan,'').groupby('CODESTAB',as_index=False).agg(''.join)
        # Ordena as linhas de "df6" por ordem crescente dos valores da coluna CODESTAB
        df6.sort_values(by=['CODESTAB'], inplace=True)
        # Reset o index devido ao sorting prévio
        df6.reset_index(drop=True, inplace=True)
        # Substitui os valores de string vazia das colunas especificadas pela string "?"
        df6['ESFERA'].replace('','?', inplace=True)
        df6['REGIME'].replace('','?', inplace=True)
        # Upload do arquivo "xlsx" que contém os CODESTAB presentes nos arquivos DNXXaaaa (dos anos de...
        # 1997 a 2017) e não presentes na tabela CNESDN18 e na TCC CNESDN07. Ou seja,...
        # isso parece ser uma falha dos dados do Datasus
        dataframe = pd.read_excel(self.path + 'CODESTAB_OUT_CNESDN_07_E_18_ANOS_1997_2017' + '.xlsx')
        # Converte a coluna "CODESTAB" do objeto "dataframe" de "int" para "string"
        dataframe['CODESTAB'] = dataframe['CODESTAB'].astype('str')
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna CODESTAB do objeto "dataframe" até...
        # formar uma "string" de tamanho = 7
        dataframe['CODESTAB'] = dataframe['CODESTAB'].apply(lambda x: x.zfill(7))
        # Adiciona as colunas DESCESTAB, ESFERA e REGIME e respectivos valores ao objeto "dataframe"...
        # para torná-lo com as mesmas colunas do "df6"
        dataframe['DESCESTAB'] = ['NAO PROVIDO EM CNESDN18.DBF E NA TCC CNESDN07'] * (dataframe.shape[0])
        dataframe['ESFERA'] = ['?'] * (dataframe.shape[0])
        dataframe['REGIME'] = ['?'] * (dataframe.shape[0])
        # Concatenação do objeto "dataframe" ao objeto "df6"
        frames = []
        frames.append(df6)
        frames.append(dataframe)
        dfinal = pd.concat(frames, ignore_index=True)
        # Renomeia a coluna CODESTAB
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


    # Função para adequar e formatar as colunas e valores da TCC LOCOCOR (arquivo LOCOCOR.cnv)
    def get_LOCOCOR_treated(self):
        # Conversão da TCC LOCOCOR para um objeto pandas DataFrame
        file_name = 'LOCOCOR'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'LOCAL'}, inplace=True)
        # Coloca todas as string da coluna especificada como UPPER CASE
        df['LOCAL'] = df['LOCAL'].apply(lambda x: x.upper())
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC SITCONJU (arquivo SITCONJU.cnv)
    def get_SITCONJU_treated(self):
        # Conversão da TCC SITCONJU para um objeto pandas DataFrame
        file_name = 'SITCONJU'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'SITUACAO'}, inplace=True)
        # Coloca todas as string da coluna especificada como UPPER CASE
        df['SITUACAO'] = df['SITUACAO'].apply(lambda x: x.upper())
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
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df1" até...
        # formar uma "string" de tamanho = 6
        df1['ID'] = df1['ID'].apply(lambda x: x.zfill(6))
        # Conversão da TCC CBO2002 para um objeto pandas DataFrame
        file_name = 'CBO2002'
        df2 = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df2.rename(index=str, columns={'SIGNIFICACAO': 'OCUPACAO'}, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df2" até...
        # formar uma "string" de tamanho = 6
        df2['ID'] = df2['ID'].apply(lambda x: x.zfill(6))
        # Conversão da TCC OCUPA para um objeto pandas DataFrame
        file_name = 'OCUPA'
        df3 = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df3.rename(index=str, columns={'SIGNIFICACAO': 'OCUPACAO'}, inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df3" até...
        # formar uma "string" de tamanho = 6
        df3['ID'] = df3['ID'].apply(lambda x: x.zfill(6))
        # Concatena os dois objetos pandas DataFrame
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
        # 1997 a 2017) e não presentes na Tabela TABOCUP e nas TCC CBO2002 e OCUPA. Ou seja,...
        # isso parece ser uma falha dos dados do Datasus
        dataframe = pd.read_excel(self.path + 'CODOCUPMAE_OUT_TABOCUP_E_2TCC_ANOS_1997_2017' + '.xlsx')
        # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
        dataframe['ID'] = dataframe['ID'].astype('str')
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "dataframe" até...
        # formar uma "string" de tamanho = 6
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


    # Função para adequar e formatar as colunas e valores da TCC SEMANAS (arquivo SEMANAS.cnv)
    def get_SEMANAS_treated(self):
        # Conversão da TCC SEMANAS para um objeto pandas DataFrame
        file_name = 'SEMANAS'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'FAIXA_DE_SEMANAS_GESTACAO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC GRAVIDEZ (arquivo GRAVIDEZ.cnv)
    def get_GRAVIDEZ_treated(self):
        # Conversão da TCC GRAVIDEZ para um objeto pandas DataFrame
        file_name = 'GRAVIDEZ'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'MULTIPLICIDADE_GESTACAO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC PARTO (arquivo PARTO.cnv)
    def get_PARTO_treated(self):
        # Conversão da TCC PARTO para um objeto pandas DataFrame
        file_name = 'PARTO'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC CONSULT (arquivo CONSULT.cnv)
    def get_CONSULT_treated(self):
        # Conversão da TCC CONSULT para um objeto pandas DataFrame
        file_name = 'CONSULT'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'FAIXA_DE_NUMERO_CONSULTAS'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
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


    # Função para adequar e formatar as colunas e valores da Tabela CID10 (arquivo CID10.dbf) e...
    # de 21 TCC com nome CID10_XX (arquivos "cnv") sendo XX indo de 01 a 21, um para cada capítulo do CID 10.
    # Utiliza-se esses 21 CID10_XX.cnv do SIM ao invés do CID1017.cnv do SINASC pois aquele é completo e...
    # este contém apenas o capítulo 17
    def get_CID10_treated(self):
        # Conversão da Tabela CID10 para um objeto pandas DataFrame
        file_name = 'CID10'
        df1 = download_table_dbf(file_name)
        # Remove colunas indesejáveis do objeto pandas DataFrame
        df1 = df1.drop(['OPC', 'CAT', 'SUBCAT', 'RESTRSEXO'], axis=1)
        # Renomeia as colunas especificadas
        df1.rename(index=str, columns={'CID10': 'ID', 'DESCR': 'ANOMALIA'}, inplace=True)
        # Coloca todas as string da coluna especificada como UPPER CASE
        df1['ANOMALIA'] = df1['ANOMALIA'].apply(lambda x: x.upper())
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
        df2.rename(index=str, columns={'SIGNIFICACAO': 'ANOMALIA'}, inplace=True)
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
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC NAT1212 (arquivo NAT1212.cnv)
    # Utiliza-se o NAT1212.cnv (pertencente do SIM) por não constar nenhuma referência a um "cnv"...
    # no Dicionário de Dados do SINASC
    def get_NAT1212_treated(self):
        # Conversão da TCC NAT1212 para um objeto pandas DataFrame
        file_name = 'NAT1212'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'LOCAL'}, inplace=True)
        # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df" até...
        # formar uma "string" de tamanho = 3
        df['ID'] = df['ID'].apply(lambda x: x.zfill(3))
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC ESC2010 (arquivo ESC2010.cnv)
    def get_ESC2010_treated(self):
        # Conversão da TCC ESC2010 para um objeto pandas DataFrame
        file_name = 'ESC2010'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'ESCOLARIDADE'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC TPMETODO (arquivo TPMETODO.cnv)
    def get_TPMETODO_treated(self):
        # Conversão da TCC TPMETODO para um objeto pandas DataFrame
        file_name = 'TPMETODO'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'METODO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela TPAPRESENT ("constando" apenas do...
    # Dicionário de Dados do SINASC)
    def get_TPAPRESENT_treated(self):
        # Conversão da Tabela TPAPRESENT (em formato "xlsx") para um objeto pandas DataFrame
        file_name = 'TPAPRESENT'
        df = pd.read_excel(self.path + file_name + '.xlsx')
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'POSICAO'}, inplace=True)
        # Converte para string a coluna especificada
        df['ID'] = df['ID'].astype('str')
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela STTRABPART ("constando" apenas do...
    # Dicionário de Dados do SINASC)
    def get_STTRABPART_treated(self):
        # Conversão da Tabela STTRABPART (em formato "xlsx") para um objeto pandas DataFrame
        file_name = 'STTRABPART'
        df = pd.read_excel(self.path + file_name + '.xlsx')
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'INDUCAO'}, inplace=True)
        # Converte para string a coluna especificada
        df['ID'] = df['ID'].astype('str')
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC STPARTO (arquivo STPARTO.cnv)
    def get_STPARTO_treated(self):
        # Conversão da Tabela TCC STPARTO para um objeto pandas DataFrame
        file_name = 'STPARTO'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'CESAREA_ANTES_PARTO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC TPASSIST (arquivo TPASSIST.cnv)
    def get_TPASSIST_treated(self):
        # Conversão da TCC TPASSIST para um objeto pandas DataFrame
        file_name = 'TPASSIST'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'ASSISTENCIA'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela TPFUNCRESP ("constando" apenas do...
    # Dicionário de Dados do SINASC)
    def get_TPFUNCRESP_treated(self):
        # Conversão da Tabela TPFUNCRESP (em formato "xlsx") para um objeto pandas DataFrame
        file_name = 'TPFUNCRESP'
        df = pd.read_excel(self.path + file_name + '.xlsx')
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'FUNCAO'}, inplace=True)
        # Converte para string a coluna especificada
        df['ID'] = df['ID'].astype('str')
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC ESCAGR1 (arquivo ESCAGR1.cnv)
    def get_ESCAGR1_treated(self):
        # Conversão da TCC ESCAGR1 para um objeto pandas DataFrame
        file_name = 'ESCAGR1'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'ESCOLARIDADE'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da Tabela TABPAIS (arquivo TABPAIS.dbf)
    def get_TABPAIS_treated(self):
        # Conversão da Tabela TABPAIS para um objeto pandas DataFrame
        file_name = 'TABPAIS'
        df = download_table_dbf(file_name)
        # Adequa e formata a Tabela TABPAIS
        df.rename(index=str, columns={'CODIGO': 'ID', 'DESCRICAO': 'PAIS'}, inplace=True)
        # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Reset o index devido à eventual eliminação de duplicates
        df.reset_index(drop=True, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df


    # Função para adequar e formatar as colunas e valores da TCC ROBSON (arquivo ROBSON.cnv)
    def get_ROBSON_treated(self):
        # Conversão da TCC ROBSON para um objeto pandas DataFrame
        file_name = 'ROBSON'
        df = download_table_cnv(file_name)
        # Renomeia a coluna SIGNIFICACAO
        df.rename(index=str, columns={'SIGNIFICACAO': 'DESCRICAO'}, inplace=True)
        # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value"
        df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
        return df



if __name__ == '__main__':

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    # Ajustar o "the_path_..." para a localização dos arquivos "xlsx"
    the_path_Tabelas = os.getcwd()[:-len('\\insertion\\data_wrangling')] + '\\files\\SINASC\\'
