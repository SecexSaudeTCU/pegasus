###########################################################################################################################################################################
# CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP #
###########################################################################################################################################################################

import os
from datetime import datetime

import numpy as np
import pandas as pd

from .online.download_CNES import download_CNESXXaamm, download_table_dbf, download_table_cnv

# import sys
# sys.path.append('C:\\Users\\ericc\\Desktop\\8dbs\\insertion\\data_wrangling\\online\\')
# from download_CNES import download_CNESXXaamm, download_table_dbf, download_table_cnv


"""
Script de tratamento de dados do CNES_EP (Equipes) para atender ao framework do SGBD PostgreSQL.
Válido para os arquivos de dados EPXXaamm (EP = Equipes; XX = Estado; aa = Ano; mm = Mês)...
(originalmente em formato "dbc") a partir do ano de 2008.

"""


# Função para converter um "value" num certo "type" de objeto ou caso não seja possível utiliza o valor "default"
def tryconvert(value, default, type):
    try:
        return type(value)
    except (ValueError, TypeError):
        return default


# Função para ler como um objeto pandas DataFrame um arquivo de dados EPXXaamm do CNES e adequar e formatar suas colunas e valores
def get_EPXXaamm_treated(state, year, month):
    # Lê o arquivo "dbc" como um objeto pandas DataFrame e o salva no formato "parquet"
    dataframe = download_CNESXXaamm('EP', state, year, month)
    print('O número de linhas do arquivo EP{}{}{} é {}.'.format(state, year, month, dataframe.shape[0]))

    # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela EPBR da base de dados
    lista_columns = np.array(['CNES', 'CODUFMUN', 'IDEQUIPE', 'TIPO_EQP', 'NOME_EQP', 'ID_AREA', 'NOMEAREA', 'ID_SEGM',
                              'DESCSEGM', 'TIPOSEGM', 'DT_ATIVA', 'DT_DESAT', 'QUILOMBO', 'ASSENTAD', 'POPGERAL', 'ESCOLA',
                              'INDIGENA', 'PRONASCI', 'MOTDESAT', 'TP_DESAT'])

    # Criação de um objeto pandas DataFrame vazio com as colunas especificadas acima
    df = pd.DataFrame(columns=lista_columns)

    # Colocação dos dados da variável "dataframe" na variável "df" nas colunas de mesmo nome preenchendo automaticamente com o float NaN...
    # as colunas da variável "df" não presentes na variável dataframe
    for col in df.columns.values:
        for coluna in dataframe.columns.values:
            if coluna == col:
                df[col] = dataframe[coluna].tolist()
                break

    # Coloca na variável "dif_set" o objeto array dos nomes das colunas da variável "df" que não estão presentes na variável "dataframe"
    dif_set = np.setdiff1d(df.columns.values, dataframe.columns.values)

    # Substitui o float NaN pela string vazia as colunas da variável "df" não presentes na variável "dataframe"
    for col in dif_set:
        df[col].replace(np.nan, '', inplace=True)

    # Exclui o último dígito numérico das colunas identificadas, o qual corresponde ao dígito de controle do código do município
    # Foi detectado que para alguns municípios o cálculo do dígito de controle não é válido
    if len(df.loc[0, 'CODUFMUN']) == 7:
        df['CODUFMUN'].replace(regex='.$',value='', inplace=True)

    # Simplifica/corrige a apresentação dos dados das colunas especificadas
    for col in np.array(['MOTDESAT', 'TP_DESAT']):
        df[col] = df[col].apply(lambda x: str(tryconvert(x, '', int)))

    # Atualiza/corrige os labels das colunas especificadas
    df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
    df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
    df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
    df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454', '500627', '510445', '999999'], '', inplace=True)
    df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080', '530090',
                            '530100', '530110',  '530120', '530130', '530135', '530140', '530150', '530160',
                            '530170', '530180'] + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

    df['TIPO_EQP'].replace(['58', '59', '60'], '', inplace=True)

    df['ID_AREA'].replace(regex=['^1[1-7][09]{8}$', '^2[1-9][09]{8}$','^3[1235][09]{8}$', '^4[1-3][09]{8}$', '^5[0-3][09]{8}$'], value='', inplace=True)

    df['ID_SEGM'].replace(regex=['^1[1-7][09]{6}$', '^2[1-9][09]{6}$','^3[1235][09]{6}$', '^4[1-3][09]{6}$', '^5[0-3][09]{6}$'], value='', inplace=True)

    df['TIPOSEGM'].replace(['0', '3', '4', '5', '6', '7', '8', '9'], '', inplace=True)

    df['DT_DESAT'].replace('900001', '', inplace=True)

    df['MOTDESAT'].replace(regex='[1-9][0-9]', value='', inplace=True)

    df['TP_DESAT'].replace(regex='[3-9]', value='', inplace=True)

    # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
    for col in np.array(['CNES', 'CODUFMUN', 'IDEQUIPE', 'TIPO_EQP', 'ID_AREA', 'ID_SEGM', 'TIPOSEGM', 'MOTDESAT', 'TP_DESAT']):
        df[col].replace('', 'NA', inplace=True)

    # Substitui uma string vazia por None nas colunas de atributos especificadas
    for col in np.array(['NOME_EQP', 'NOMEAREA', 'DESCSEGM']):
        df[col].replace('', None, inplace=True)

    # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes ("NaT") pela data...
    # futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
    for col in np.array(['DT_ATIVA', 'DT_DESAT']):
        df[col] = df[col].apply(lambda x: datetime.strptime(x, '%Y%m').date() if x != '' else datetime(2099, 1, 1).date())

    # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui pela data futura "2099-01-01"
    for col in np.array(['DT_ATIVA', 'DT_DESAT']):
        df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

    # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
    for col in np.array(['QUILOMBO', 'ASSENTAD', 'POPGERAL', 'ESCOLA', 'INDIGENA', 'PRONASCI']):
        df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

    # Renomeia colunas que são foreign keys
    df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID', 'IDEQUIPE': 'IDEQUIPE_ID', 'TIPO_EQP': 'TIPOEQP_ID',
                                  'ID_AREA': 'IDAREA_ID', 'ID_SEGM': 'IDSEGM_ID', 'TIPOSEGM': 'TIPOSEGM_ID',
                                  'MOTDESAT': 'MOTDESAT_ID', 'TP_DESAT': 'TPDESAT_ID'}, inplace=True)

    print('Terminou de tratar o arquivo EP{}{}{} (shape final: {} x {}).'.format(state, year, month, df.shape[0], df.shape[1]))

    return df


# Função para adequar e formatar as colunas e valores das 27 Tabelas CADGERXX (arquivos CADGERXX.dbf),...
# sendo uma para cada estado do Brasil
def get_CADGERBR_treated(path):
    # Conversão das 27 Tabelas CADGERXX para um objeto pandas DataFrame
    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']
    for state in lista_estados:
        file_name = 'CADGER' + state
        dfi = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        dfi.rename(index=str, columns={'CNES': 'ID', 'FANTASIA': 'DESCESTAB'}, inplace=True)
        # Remove colunas indesejáveis do objeto pandas DataFrame
        dfi = dfi.drop(['RAZ_SOCI', 'LOGRADOU', 'NUM_END', 'COMPLEME', 'BAIRRO', 'COD_CEP', 'TELEFONE',
                        'FAX', 'EMAIL', 'REGSAUDE', 'MICR_REG', 'DISTRSAN', 'DISTRADM', 'CODUFMUN', 'NATUREZA'], axis=1)
        # Reordena as colunas
        dfi = dfi[['ID', 'DESCESTAB', 'RSOC_MAN', 'CPF_CNPJ', 'EXCLUIDO', 'DATAINCL', 'DATAEXCL']]
        # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        dfi.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas por ordem crescente dos valores da coluna ID
        dfi.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
        dfi.reset_index(drop=True, inplace=True)
        frames.append(dfi)
    df = pd.concat(frames, ignore_index=True)
    # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
    df.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Ordena as linhas por ordem crescente dos valores da coluna ID
    df.sort_values(by=['ID'], inplace=True)
    # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
    df.reset_index(drop=True, inplace=True)
    # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
    df['EXCLUIDO'] = df['EXCLUIDO'].apply(lambda x: tryconvert(x, None, int))
    # Substitui o objeto datetime.date "9999-12-31" das duas colunas de "datas" pelo objeto datetime.date "2099-01-01"
    for col in np.array(['DATAINCL', 'DATAEXCL']):
        df[col].replace(datetime(9999, 12, 31).date(), datetime(2099, 1, 1).date(), inplace=True)
    # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui pela data futura "2099-01-01"
    for col in np.array(['DATAINCL', 'DATAEXCL']):
        df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

    # Upload do arquivo "xlsx" que contém os CNES presentes nos arquivos STXXaamm (dos anos de 2008 a 2019) e não presentes...
    # nas 27 Tabelas CADGERXX. Ou seja, isso parece ser uma falha dos dados do Datasus
    dataframe = pd.read_excel(path + 'CNES_OUT_CADGER_XX_ANOS_2008_2019' + '.xlsx')
    # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
    dataframe['ID'] = dataframe['ID'].astype('str')
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "dataframe" até formar uma "string" de tamanho = 7
    dataframe['ID'] = dataframe['ID'].apply(lambda x: x.zfill(7))
    # Adiciona as colunas DESCESTAB, RSOC_MAN, CPF_CNPJ, EXCLUIDO, DATAINCL e DATAEXCL e respectivos valores ao objeto "dataframe"
    dataframe['DESCESTAB'] = 'NAO PROVIDO EM 27 ARQUIVOS DBF DE CNES'
    dataframe['RSOC_MAN'] = '?'
    dataframe['CPF_CNPJ'] = '?'
    dataframe['EXCLUIDO'] = None
    dataframe['DATAINCL'] = datetime(2099, 1, 1)
    dataframe['DATAEXCL'] = datetime(2099, 1, 1)
    # Concatenação do objeto "dataframe" ao objeto "df"
    frames = []
    frames.append(df)
    frames.append(dataframe)
    dfinal = pd.concat(frames, ignore_index=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela EPBR
    dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE', '?', '?', None, datetime(2099, 1, 1), datetime(2099, 1, 1)]
    return dfinal


# Função para adequar e formatar as colunas e valores da Tabela CADMUN (arquivo CADMUN.dbf)
def get_CADMUN_treated():
    # Conversão da Tabela CADMUN para um objeto pandas DataFrame
    file_name = 'CADMUN'
    df = download_table_dbf(file_name)
    # Renomeia as colunas especificadas
    df.rename(index=str, columns={'MUNCOD': 'ID', 'UFCOD': 'UFCOD_ID'}, inplace=True)
    # Drop a linha inteira em que a coluna "ID" tem o valor especificado por não representar nenhum município
    df = df.drop(df[df['ID']=='000000'].index)
    # Remove colunas indesejáveis do objeto pandas DataFrame
    df = df.drop(['MUNSINON', 'MUNSINONDV', 'MESOCOD', 'MICROCOD', 'MSAUDCOD', 'RSAUDCOD', 'CSAUDCOD', 'RMETRCOD', 'AGLCOD'], axis=1)
    # Substitui uma string vazia pela string "?" nas colunas especificadas
    for col in ['SITUACAO', 'MUNSINP', 'MUNSIAFI', 'MUNNOME', 'MUNNOMEX', 'OBSERV', 'AMAZONIA', 'FRONTEIRA', 'CAPITAL', 'ANOINST', 'ANOEXT', 'SUCESSOR']:
        df[col].replace('', '?', inplace=True)
    # Substitui uma string vazia pela string "NA" nas colunas especificadas
    df['UFCOD_ID'].replace('', 'NA', inplace=True)
    # Substitui uma string vazia pelo float "NaN" nas colunas especificadas
    for col in ['LATITUDE', 'LONGITUDE', 'ALTITUDE', 'AREA']:
        df[col].replace('', np.nan, inplace=True)
    # Converte do tipo object para float as colunas especificadas
    df[['LATITUDE', 'LONGITUDE', 'ALTITUDE', 'AREA']] = df[['LATITUDE', 'LONGITUDE', 'ALTITUDE', 'AREA']].astype('float')
    # Reordena as colunas priorizando as "mais" relevantes
    df = df[['ID', 'MUNNOME', 'MUNNOMEX', 'MUNCODDV', 'OBSERV', 'SITUACAO', 'MUNSINP', 'MUNSIAFI', 'UFCOD_ID', 'AMAZONIA', 'FRONTEIRA', 'CAPITAL', 'LATITUDE',
             'LONGITUDE', 'ALTITUDE', 'AREA', 'ANOINST', 'ANOEXT', 'SUCESSOR']]
    # Coloca todas as string das colunas especificadas como UPPER CASE
    df['MUNNOME'] = df['MUNNOME'].apply(lambda x: x.upper())
    df['MUNNOMEX'] = df['MUNNOMEX'].apply(lambda x: x.upper())
    # Insere uma linha referente ao Município de Nazária/PI não constante originalmente da
    df.loc[df.shape[0]] = ['220672', 'NAZÁRIA', 'NAZARIA', '2206720', '?', '?', '?', '?', '22', '?', '?', '?', np.nan, np.nan, np.nan, 363.589, '?', '?', '?']
    # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
    df.sort_values(by=['ID'], inplace=True)
    # Reset o index devido ao sorting prévio e à exclusão e inclusão das linhas referidas acima
    df.reset_index(drop=True, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela EPBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE', '?', '?', '?', '?', '?', '?', 'NA', '?', '?', '?', np.nan, np.nan, np.nan, np.nan, '?', '?', '?']
    return df


# Função para adequar e formatar as colunas e valores das Tabelas EQP_XX (arquivos EQP_XX.dbf, sendo XX a sigla do Estado da RFB)
def get_EQP_XX_treated(path):
    # Lista de Estados da RFB
    estados = np.array(['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                        'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'])
    frames = []
    for uf in estados:
        # Conversão da Tabela EQP_XX para um objeto pandas DataFrame
        file_name = 'EQP_' + uf
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'IDEQUIPE': 'ID', 'NOME_EQP': 'NOME_EQUIPE'}, inplace=True)
        # Elimina linhas duplicadas de "df" tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index de "df" devido ao sorting prévio e à eventual exclusão das linhas referidas acima
        df.reset_index(drop=True, inplace=True)
        # Acrescenta "df" como um elemento de um objeto list
        frames.append(df)
    # Concatena um sobre o outro os objetos pandas DataFrame presentes no objeto list "frames"
    df = pd.concat(frames, axis=0, ignore_index=True)
    # Elimina linhas duplicadas de "df" tendo por base a coluna ID e mantém a primeira ocorrência
    df.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
    df.sort_values(by=['ID'], inplace=True)
    # Reset o index de "df" devido ao sorting prévio e à exclusão das linhas referidas acima
    df.reset_index(drop=True, inplace=True)
    # Upload do arquivo "xlsx" que contém os IDEQUIPE presentes nos arquivos EPXXaamm (dos anos de 2008 a 2019) e não presentes...
    # nas 27 Tabelas EQP_XX. Ou seja, isso parece ser uma falha dos dados do Datasus
    dataframe = pd.read_excel(path + 'IDEQUIPE_OUT_27_EQP_XX_ANOS_2008_2019' + '.xlsx')
    # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
    dataframe['ID'] = dataframe['ID'].astype('str')
    # Adiciona a coluna "NOME_EQUIPE" e respectivos valores ao objeto "dataframe"
    dataframe['NOME_EQUIPE'] = ['NAO PROVIDO NOS 27 EQP_XX.DBF'] * (dataframe.shape[0])
    # Concatenação do objeto "dataframe" ao objeto "df"
    frames = []
    frames.append(df)
    frames.append(dataframe)
    dfinal = pd.concat(frames, ignore_index=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da coluna IDEQUIPE_ID da tabela EPBR
    dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
    return dfinal


# Função para adequar e formatar as colunas e valores da Tabela EQUIPE (arquivo EQUIPE.dbf)
def get_EQUIPE_treated():
    # Conversão da Tabela EQUIPE para um objeto pandas DataFrame
    file_name = 'EQUIPE'
    df = download_table_dbf(file_name)
    # Renomeia colunas especificadas
    df.rename(index=str, columns={'TP_EQUIPE': 'ID', 'DS_EQUIPE': 'TIPO'}, inplace=True)
    # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
    df.sort_values(by=['ID'], inplace=True)
    # Reset o index devido ao sorting prévio e à exclusão e inclusão das linhas referidas acima
    df.reset_index(drop=True, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela EPBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores das Tabelas AREA_XX (arquivos AREA_XX.dbf, sendo XX a sigla do Estado da RFB)
def get_AREA_XX_treated(path):
    # Lista de Estados da RFB
    estados = np.array(['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                        'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'])
    frames = []
    for uf in estados:
        # Conversão da Tabela AREA_XX para um objeto pandas DataFrame
        file_name = 'AREA_' + uf
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'ID_AREA': 'ID', 'NOMEAREA': 'NOME_AREA'}, inplace=True)
        # Elimina linhas duplicadas de "df" tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Desconsidera as linhas de "df" que têm na coluna NOME_AREA a substring discriminada
        df = df[~df['NOME_AREA'].str.contains('AREA NAO INFORMADA', regex=True)]
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index de "df" devido ao sorting prévio e à eventual exclusão das linhas referidas acima
        df.reset_index(drop=True, inplace=True)
        # Acrescenta "df" como um elemento de um objeto list
        frames.append(df)
    # Concatena um sobre o outro os objetos pandas DataFrame presentes no objeto list "frames"
    df = pd.concat(frames, axis=0, ignore_index=True)
    # Elimina linhas duplicadas de "df" tendo por base a coluna ID e mantém a primeira ocorrência
    df.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
    df.sort_values(by=['ID'], inplace=True)
    # Reset o index de "df" devido ao sorting prévio e à exclusão das linhas referidas acima
    df.reset_index(drop=True, inplace=True)
    # Upload do arquivo "xlsx" que contém os ID_AREA presentes nos arquivos EPXXaamm (dos anos de 2008 a 2019) e não presentes...
    # nas 27 Tabelas AREA_XX. Ou seja, isso parece ser uma falha dos dados do Datasus
    dataframe = pd.read_excel(path + 'ID_AREA_OUT_27_AREA_XX_ANOS_2008_2019' + '.xlsx')
    # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
    dataframe['ID'] = dataframe['ID'].astype('str')
    # Adiciona a coluna "NOME_AREA" e respectivos valores ao objeto "dataframe"
    dataframe['NOME_AREA'] = ['NAO PROVIDO NOS 27 AREA_XX.DBF'] * (dataframe.shape[0])
    # Concatenação do objeto "dataframe" ao objeto "df"
    frames = []
    frames.append(df)
    frames.append(dataframe)
    dfinal = pd.concat(frames, ignore_index=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da coluna IDAREA_ID da tabela EPBR
    dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
    return dfinal


# Função para adequar e formatar as colunas e valores das Tabelas SEGM_XX (arquivos SEGM_XX.dbf, sendo XX a sigla do Estado da RFB)
def get_SEGM_XX_treated(path):
    # Lista de Estados da RFB
    estados = np.array(['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                        'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'])
    frames = []
    for uf in estados:
        # Conversão da Tabela SEGM_XX para um objeto pandas DataFrame
        file_name = 'SEGM_' + uf
        df = download_table_dbf(file_name)
        # Renomeia colunas especificadas
        df.rename(index=str, columns={'ID_SEGM': 'ID', 'DESCSEGM': 'DESCRICAO'}, inplace=True)
        # Elimina linhas duplicadas de "df" tendo por base a coluna ID e mantém a primeira ocorrência
        df.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Desconsidera as linhas de "df" que têm na coluna NOME_AREA a substring discriminada
        df = df[~df['DESCRICAO'].str.contains('SEGMENTO NAO INFORMADO', regex=True)]
        # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
        df.sort_values(by=['ID'], inplace=True)
        # Reset o index de "df" devido ao sorting prévio e à eventual exclusão das linhas referidas acima
        df.reset_index(drop=True, inplace=True)
        # Acrescenta "df" como um elemento de um objeto list
        frames.append(df)
    # Concatena um sobre o outro os objetos pandas DataFrame presentes no objeto list "frames"
    df = pd.concat(frames, axis=0, ignore_index=True)
    # Elimina linhas duplicadas de "df" tendo por base a coluna ID e mantém a primeira ocorrência
    df.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
    df.sort_values(by=['ID'], inplace=True)
    # Reset o index de "df" devido ao sorting prévio e à exclusão das linhas referidas acima
    df.reset_index(drop=True, inplace=True)
    # Upload do arquivo "xlsx" que contém os ID_SEGM presentes nos arquivos EPXXaamm (dos anos de 2008 a 2019) e não presentes...
    # nas 27 Tabelas SEGM_XX. Ou seja, isso parece ser uma falha dos dados do Datasus
    dataframe = pd.read_excel(path + 'ID_SEGM_OUT_27_SEGM_XX_ANOS_2008_2019' + '.xlsx')
    # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
    dataframe['ID'] = dataframe['ID'].astype('str')
    # Adiciona a coluna "DESCRICAO" e respectivos valores ao objeto "dataframe"
    dataframe['DESCRICAO'] = ['NAO PROVIDO NOS 27 SEGM_XX.DBF'] * (dataframe.shape[0])
    # Concatenação do objeto "dataframe" ao objeto "df"
    frames = []
    frames.append(df)
    frames.append(dataframe)
    dfinal = pd.concat(frames, ignore_index=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da coluna IDSEGM_ID da tabela EPBR
    dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
    return dfinal


# Função para adequar e formatar as colunas e valores da TCC tiposegm (arquivo tiposegm.cnv)
def get_tiposegm_treated():
    # Conversão da TCC tiposegm para um objeto pandas DataFrame
    file_name = 'tiposegm'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela EPBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC motdesat (arquivo motdesat.cnv)
def get_motdesat_treated():
    # Conversão da TCC motdesat para um objeto pandas DataFrame
    file_name = 'motdesat'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'MOTIVO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela EPBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC TP_DESAT (arquivo TP_DESAT.cnv)
def get_TP_DESAT_treated():
    # Conversão da TCC TP_DESAT para um objeto pandas DataFrame
    file_name = 'TP_DESAT'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela EPBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da Tabela TABUF (arquivo TABUF.dbf)
def get_TABUF_treated():
    # Conversão da Tabela TABUF para um objeto pandas DataFrame
    file_name = 'TABUF'
    df = download_table_dbf(file_name)
    # Renomeia colunas especificadas
    df.rename(index=str, columns={'CODIGO': 'ID', 'DESCRICAO': 'ESTADO'}, inplace=True)
    # Reordena as colunas
    df = df[['ID', 'ESTADO', 'SIGLA_UF']]
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela CODUFMUN
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE', '?']
    return df



if __name__ == '__main__':

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    df = get_SEGM_XX_treated()
    #print(df)
