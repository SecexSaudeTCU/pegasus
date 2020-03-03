###########################################################################################################################################################################
# CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF #
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
Script de tratamento de dados do CNES_PF (Profissionais) para atender ao framework do SGBD PostgreSQL.
Válido para os arquivos de dados PFXXaamm (PF = Profissionais; XX = Estado; aa = Ano; mm = Mês)...
(originalmente em formato "dbc") a partir do ano de 2006.

"""


# Função para converter um "value" num certo "type" de objeto ou caso não seja possível utiliza o valor "default"
def tryconvert(value, default, type):
    try:
        return type(value)
    except (ValueError, TypeError):
        return default


# Função para ler como um objeto pandas DataFrame um arquivo de dados PFXXaamm do CNES e adequar e formatar suas colunas e valores
def get_PFXXaamm_treated(state, year, month):
    # Lê o arquivo "dbc" como um objeto pandas DataFrame e o salva no formato "parquet"
    dataframe = download_CNESXXaamm('PF', state, year, month)
    print('O número de linhas do arquivo PF{}{}{} é {}.'.format(state, year, month, dataframe.shape[0]))

    # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela PFBR da base de dados
    lista_columns = np.array(['CNES', 'CODUFMUN', 'CBO', 'CBOUNICO', 'NOMEPROF', 'CNS_PROF', 'CONSELHO', 'REGISTRO', 'VINCULAC',
                              'VINCUL_C', 'VINCUL_A', 'VINCUL_N', 'PROF_SUS', 'PROFNSUS', 'HORAOUTR', 'HORAHOSP', 'HORA_AMB'])

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
    for col in np.array(['CBO', 'CBOUNICO', 'VINCULAC']):
        df[col] = df[col].apply(lambda x: x.zfill(6))
        df[col] = df[col].apply(str.strip)
        df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')

    df['CONSELHO'] = df['CONSELHO'].apply(lambda x: x.zfill(2))
    df['CONSELHO'] = df['CONSELHO'].apply(str.strip)
    df['CONSELHO'] = df['CONSELHO'].apply(lambda x: x if len(x) == 2 else '')

    # Atualiza/corrige os labels das colunas especificadas
    df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
    df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
    df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
    df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454', '500627', '510445', '999999'], '', inplace=True)
    df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080', '530090',
                            '530100', '530110',  '530120', '530130', '530135', '530140', '530150', '530160',
                            '530170', '530180'] + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

    for col in np.array(['CBO', 'CBOUNICO', 'VINCULAC']):
        df[col].replace('000000', '', inplace=True)

    df['CONSELHO'].replace(['00', '01', '02', '05', '06', '08', '09', '11', '13', '14', '16', '27', '31', '98', '99'], '', inplace=True)

    df['VINCULAC'].replace(['010603', '040101'], '', inplace=True)

    # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
    for col in np.array(['CNES', 'CODUFMUN', 'CBO', 'CBOUNICO', 'CONSELHO', 'VINCULAC']):
        df[col].replace('', 'NA', inplace=True)

    # Substitui uma string vazia por None nas colunas de atributos especificadas
    for col in np.array(['NOMEPROF', 'CNS_PROF', 'REGISTRO']):
        df[col].replace('', None, inplace=True)

    # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
    for col in np.array(['VINCUL_C', 'VINCUL_A', 'VINCUL_N', 'PROF_SUS', 'PROFNSUS']):
        df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

    # Converte do tipo object para float sem casas decimais as colunas de atributos de valores representativos de quantidades
    # ou para o valor None caso a coluna esteja com a string vazia
    for col in np.array(['HORAOUTR', 'HORAHOSP', 'HORA_AMB']):
        df[col] = df[col].apply(lambda x: round(float(x),0) if x != '' else None)

    # Renomeia colunas que são foreign keys
    df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID', 'CBO': 'CBO_ID', 'CBOUNICO': 'CBOUNICO_ID', 'CONSELHO': 'CONSELHO_ID',
                                  'VINCULAC': 'VINCULAC_ID'}, inplace=True)

    print('Terminou de tratar o arquivo PF{}{}{} (shape final: {} x {}).'.format(state, year, month, df.shape[0], df.shape[1]))

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
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela PFBR
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
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela PFBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE', '?', '?', '?', '?', '?', '?', 'NA', '?', '?', '?', np.nan, np.nan, np.nan, np.nan, '?', '?', '?']
    return df


# Função para adequar e formatar as colunas e valores das Tabelas MEDIC_02, NV_SUP_02, TECNIC_02 e CBO_02 (arquivos...
# MEDIC_02.dbf, NV_SUP_02.dbf, TECNIC_02.dbf e CBO_02.dbf)
def get_CBO_treated(path):
    # Conversão da Tabela MEDIC_02 para um objeto pandas DataFrame
    file_name = 'MEDIC_02'
    df1 = download_table_dbf(file_name)
    # Renomeia as colunas especificadas
    df1.rename(index=str, columns={'CBO': 'ID', 'DS_CBO': 'OCUPACAO'}, inplace=True)
    # Ordena as linhas de "df1" por ordem crescente dos valores da coluna ID
    df1.sort_values(by=['ID'], inplace=True)
    # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
    df1.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Reset o index devido ao sorting prévio
    df1.reset_index(drop=True, inplace=True)
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df1" até formar uma "string" de tamanho = 6
    df1['ID'] = df1['ID'].apply(lambda x: x.zfill(6))
    # Conversão da Tabela NV_SUP_02 para um objeto pandas DataFrame
    file_name = 'NV_SUP_02'
    df2 = download_table_dbf(file_name)
    # Renomeia as colunas especificadas
    df2.rename(index=str, columns={'CO_CBO': 'ID', 'DS_CBO': 'OCUPACAO'}, inplace=True)
    # Ordena as linhas de "df2" por ordem crescente dos valores da coluna ID
    df2.sort_values(by=['ID'], inplace=True)
    # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
    df2.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Reset o index devido ao sorting prévio
    df2.reset_index(drop=True, inplace=True)
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df2" até formar uma "string" de tamanho = 6
    df2['ID'] = df2['ID'].apply(lambda x: x.zfill(6))
    # Conversão da Tabela TECNIC_02 para um objeto pandas DataFrame
    file_name = 'TECNIC_02'
    df3 = download_table_dbf(file_name)
    # Renomeia as colunas especificadas
    df3.rename(index=str, columns={'CBO': 'ID', 'DS_CBO': 'OCUPACAO'}, inplace=True)
    # Ordena as linhas de "df3" por ordem crescente dos valores da coluna ID
    df3.sort_values(by=['ID'], inplace=True)
    # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
    df3.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Reset o index devido ao sorting prévio
    df3.reset_index(drop=True, inplace=True)
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df3" até formar uma "string" de tamanho = 6
    df3['ID'] = df3['ID'].apply(lambda x: x.zfill(6))
    # Conversão da Tabela CBO_02 para um objeto pandas DataFrame
    file_name = 'CBO_02'
    df4 = download_table_dbf(file_name)
    # Renomeia as colunas especificadas
    df4.rename(index=str, columns={'CBO': 'ID', 'DS_CBO': 'OCUPACAO'}, inplace=True)
    # Coloca todas as string da coluna especificada como UPPER CASE
    df4['OCUPACAO'] = df4['OCUPACAO'].apply(lambda x: x.upper())
    # Ordena as linhas de "df4" por ordem crescente dos valores da coluna ID
    df4.sort_values(by=['ID'], inplace=True)
    # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
    df4.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Reset o index devido ao sorting prévio
    df4.reset_index(drop=True, inplace=True)
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df4" até formar uma "string" de tamanho = 6
    df4['ID'] = df4['ID'].apply(lambda x: x.zfill(6))
    # Concatena os quatro objetos pandas DataFrame
    frames = []
    frames.append(df1)
    frames.append(df2)
    frames.append(df3)
    frames.append(df4)
    df = pd.concat(frames, ignore_index=True)
    # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
    df.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Ordena as linhas por ordem crescente dos valores da coluna "ID"
    df.sort_values(by=['ID'], inplace=True)
    # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
    df.reset_index(drop=True, inplace=True)
    # Upload do arquivo "xlsx" que contém os CBO presentes nos arquivos PFXXaamm (dos anos de 2006 a 2019) e não presentes...
    # nas Tabelas MEDIC_02, NV_SUP_02, TECNIC_02 e CBO_02. Ou seja, isso parece ser uma falha dos dados do Datasus
    dataframe = pd.read_excel(path + 'CBO_OUT_4_DBF_ANOS_2006_2019' + '.xlsx')
    # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
    dataframe['ID'] = dataframe['ID'].astype('str')
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "dataframe" até formar uma "string" de tamanho = 6
    dataframe['ID'] = dataframe['ID'].apply(lambda x: x.zfill(6))
    # Adiciona a coluna "OCUPACAO" e respectivos valores ao objeto "dataframe"
    dataframe['OCUPACAO'] = ['NAO PROVIDO EM 4 ARQUIVOS DBF DE CBO'] * (dataframe.shape[0])
    # Concatenação do objeto "dataframe" ao objeto "df"
    frames = []
    frames.append(df)
    frames.append(dataframe)
    dfinal = pd.concat(frames, ignore_index=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" das colunas CBO_ID e CBOUNICO_ID da tabela PFBR
    dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
    return dfinal


# Função para adequar e formatar as colunas e valores da Tabela CR_CONSEL (arquivo CR_CONSEL.dbf)
def get_CR_CONSEL_treated():
    # Conversão da Tabela CR_CONSEL para um objeto pandas DataFrame
    file_name = 'CR_CONSEL'
    df = download_table_dbf(file_name)
    # Renomeia as colunas especificadas
    df.rename(index=str, columns={'CO_CONSE': 'ID', 'DS_CONSE': 'DENOMINACAO'}, inplace=True)
    # Drop as linhas inteiras em que a coluna "ID" tem o valor especificado por não representar conselho algum
    df = df.drop(df[df['ID']=='99'].index)
    df = df.drop(df[df['ID']==''].index)

    # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
    df.drop_duplicates(subset='ID', keep='first', inplace=True)

    # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
    df.sort_values(by=['ID'], inplace=True)
    # Reset o index devido ao sorting prévio e à exclusão e inclusão das linhas referidas acima
    df.reset_index(drop=True, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela PFBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df

# Função para adequar e formatar as colunas e valores da Tabela VINCULO (arquivo VINCULO.dbf)
def get_VINCULO_treated():
    # Conversão da Tabela VINCULO para um objeto pandas DataFrame
    file_name = 'VINCULO'
    df = download_table_dbf(file_name)
    # Renomeia as colunas especificadas
    df.rename(index=str, columns={'CO_VINC': 'ID', 'DS_VINC': 'DESCRICAO'}, inplace=True)
    # Drop a linha inteira em que a coluna "ID" tem o valor especificado por não representar nenhum tipo de vínculo
    df = df.drop(df[df['ID']=='000000'].index)

    # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
    df.drop_duplicates(subset='ID', keep='first', inplace=True)

    # Insere três linhas devido oa formato antigo da coluna VINCULAC de um único dígito variando de "1" a "3"
    df.loc[df.shape[0]] = ['000001', 'PROFISSIONAL CONTRATADO']
    df.loc[df.shape[0]] = ['000002', 'PROFISSIONAL AUTÔNOMO']
    df.loc[df.shape[0]] = ['000003', 'PROFISSIONAL VÍNCULO NÃO IDENTIFICADO']

    # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
    df.sort_values(by=['ID'], inplace=True)
    # Reset o index devido ao sorting prévio e à exclusão e inclusão das linhas referidas acima
    df.reset_index(drop=True, inplace=True)
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" até formar uma "string" de tamanho = 6
    df['ID'] = df['ID'].apply(lambda x: x.zfill(6))
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela PFBR
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

    # Ajustar "the_path" para a localização dos arquivos "xlsx"
    the_path = os.getcwd()[:-len('\\insertion\\data_wrangling')] + '\\files\\CNES\\'

    df = get_CBO_treated(the_path)
    print(df)
