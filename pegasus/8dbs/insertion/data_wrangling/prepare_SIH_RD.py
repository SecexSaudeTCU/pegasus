###########################################################################################################################################################################
# SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD #
###########################################################################################################################################################################

import os
from datetime import datetime

import numpy as np
import pandas as pd

from .online.download_SIH import download_SIHXXaamm, download_table_dbf, download_table_cnv

# import sys
# sys.path.append('C:\\Users\\ericc\\Desktop\\8dbs\\insertion\\data_wrangling\\online\\')
# from download_SIH import download_SIHXXaamm, download_table_dbf, download_table_cnv


"""
Script de tratamento de dados do SIH_RD (AIH Reduzidas) para atender ao framework do SGBD PostgreSQL.
Válido para os arquivos de dados RDXXaamm (RD = Reduzidas; XX = Estado; aa = Ano; mm = Mês)...
(originalmente em formato "dbc") a partir do ano de 2008.

"""


# Função para converter um "value" num certo "type" de objeto ou caso não seja possível utiliza o valor "default"
def tryconvert(value, default, type):
    try:
        return type(value)
    except (ValueError, TypeError):
        return default


# Função para converter valores inteiros da coluna IDADE em anos no caso de estarem em dias ou meses
def label_age(row):
    # No caso de o valor da coluna IDADE ser inteiro
    if type(row['IDADE']) == int:
        # No caso de o valor da coluna COD_IDADE ser "2" indicativo de "dias"
        if row['COD_IDADE'] == '2':
            # Converte o valor da coluna IDADE de "dias" para "anos"
            return round(row['IDADE']/365, 4)
        # No caso de o valor da coluna COD_IDADE ser "3" indicativo de "meses"
        elif row['COD_IDADE'] == '3':
            # Converte o valor da coluna IDADE de "meses" para "anos"
            return round(row['IDADE']/12, 4)
        # No caso de qualquer outro valor para a coluna COD_IDADE retorna o próprio valor da coluna IDADE, inclusive no caso de...
        # o valor da coluna IDADE já estar em anos
        else:
            return round(row['IDADE']/1, 4)
    # No caso de o valor da coluna IDADE não ser inteiro retorna o próprio valor da coluna IDADE
    else:
        return row['IDADE']


# Função para ler como um objeto pandas DataFrame um arquivo de dados RDXXaamm do SIH e adequar e formatar suas colunas e valores
def get_RDXXaamm_treated(state, year, month):
    # Lê o arquivo "dbc" como um objeto pandas DataFrame e ainda o salva no formato "parquet"
    dataframe = download_SIHXXaamm('RD', state, year, month)
    print('O número de linhas do arquivo RD{}{}{} é {}.'.format(state, year, month, dataframe.shape[0]))

    # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela RDBR da base de dados
    lista_columns = np.array(['N_AIH', 'IDENT', 'UF_ZI', 'ANO_CMPT', 'MES_CMPT', 'ESPEC', 'CGC_HOSP', 'CEP', 'MUNIC_RES', 'NASC',
                              'SEXO', 'UTI_MES_TO', 'MARCA_UTI', 'UTI_INT_TO', 'DIAR_ACOM', 'QT_DIARIAS', 'PROC_SOLIC', 'PROC_REA',
                              'VAL_SH', 'VAL_SP', 'VAL_TOT', 'VAL_UTI', 'US_TOT', 'DI_INTER', 'DT_SAIDA', 'DIAG_PRINC', 'COBRANCA',
                              'NATUREZA', 'NAT_JUR', 'GESTAO', 'IND_VDRL', 'MUNIC_MOV', 'COD_IDADE', 'IDADE', 'DIAS_PERM', 'MORTE',
                              'NACIONAL', 'CAR_INT', 'HOMONIMO', 'NUM_FILHOS', 'INSTRU', 'CID_NOTIF', 'CONTRACEP1', 'CONTRACEP2',
                              'GESTRISCO', 'INSC_PN', 'CBOR', 'CNAER', 'VINCPREV', 'GESTOR_TP', 'GESTOR_CPF', 'CNES', 'CNPJ_MANT',
                              'INFEHOSP', 'CID_ASSO', 'CID_MORTE', 'COMPLEX', 'FINANC', 'FAEC_TP', 'REGCT', 'RACA_COR', 'ETNIA',
                              'AUD_JUST', 'SIS_JUST', 'VAL_SH_FED', 'VAL_SP_FED', 'VAL_SH_GES', 'VAL_SP_GES'])

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

    # Simplifica/corrige a apresentação dos dados das colunas especificadas
    for col in np.array(['ESPEC', 'MARCA_UTI', 'CAR_INT', 'CONTRACEP1', 'CONTRACEP2', 'COMPLEX', 'FINANC', 'RACA_COR']):
        for i in np.array(['00', '01', '02', '03', '04', '05', '06', '07', '08', '09']):
            df[col].replace(i, str(int(i)), inplace=True)

    df['IDADE'] = df['IDADE'].apply(lambda x: x if x != '' else None)
    df['IDADE'] = df.apply(lambda x: label_age(x), axis=1)
    df = df.drop(['COD_IDADE'], axis=1) # Elimina a coluna COD_IDADE depois de seu uso pela função "label_age" no passo anterior

    for col in np.array(['NACIONAL', 'CNAER']):
        df[col] = df[col].apply(lambda x: x.zfill(3))
        df[col] = df[col].apply(str.strip)
        df[col] = df[col].apply(lambda x: x if len(x) == 3 else '')

    for col in np.array(['CBOR', 'FAEC_TP']):
        df[col] = df[col].apply(lambda x: x.zfill(6))
        df[col] = df[col].apply(str.strip)
        df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')

    df['GESTOR_CPF'] = df['GESTOR_CPF'].apply(lambda x: x[-11:])

    df['CNES'] = df['CNES'].apply(lambda x: x.zfill(7))
    df['CNES'] = df['CNES'].apply(str.strip)
    df['CNES'] = df['CNES'].apply(lambda x: x if len(x) == 7 else '')

    for col in np.array(['REGCT', 'ETNIA']):
        df[col] = df[col].apply(lambda x: x.zfill(4))
        df[col] = df[col].apply(str.strip)
        df[col] = df[col].apply(lambda x: x if len(x) == 4 else '')

    # Atualiza/corrige os labels das colunas especificadas
    for col in np.array(['UF_ZI', 'MUNIC_RES', 'MUNIC_MOV']):
        df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')
        df[col].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
        df[col].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
        df[col].replace(['000000', '150475', '421265', '422000', '431454', '500627', '510445', '999999'], '', inplace=True)
        df[col].replace(['530000', '530020', '530030', '530040', '530050', '530060', '530070', '530080', '530090',
                         '530100', '530110',  '530120', '530130', '530135', '530140', '530150', '530160','530170',
                         '530180'] + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

    df['IDENT'].replace(['2', '3', '4', '6', '7', '8' '9'], '0', inplace=True)

    for col in np.array(['NASC', 'DI_INTER', 'DT_SAIDA']):
        df[col] = df[col].apply(lambda x: x if len(x) == 8 else '')
        df[col] = df[col].apply(lambda x: x if 1 <= tryconvert(x[4:6], 0, int) <= 12 else '')
        df[col] = df[col].apply(lambda x: x if 1 <= tryconvert(x[6:8], 0, int) <= 31 else '')

    df['SEXO'].replace('1', 'M', inplace=True) # Label "M" de Masculino
    df['SEXO'].replace(['2', '3'], 'F', inplace=True) # Label "F" de Feminino
    df['SEXO'].replace(['0', '4', '5', '6', '7', '8', '9'], '', inplace=True)

    for col in np.array(['DIAG_PRINC']):
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
        df[col].replace(['X590', 'X591', 'X592', 'X593', 'X594', 'X595', 'X596', 'X597', 'X598'], 'X599', inplace=True)
        df[col].replace('Y34', 'Y349', inplace=True)
        df[col].replace('Y447', 'Y448', inplace=True)
        df[col].replace('U04', '', inplace=True)

    df['COBRANCA'].replace('32', '29', inplace=True)
    df['COBRANCA'].replace('17', '61', inplace=True)
    df['COBRANCA'].replace('13', '62', inplace=True)

    for col in np.array(['NATUREZA', 'COMPLEX', 'FINANC', 'RACA_COR']):
        df[col].replace('99', '', inplace=True)
    df['NATUREZA'].replace('00', '', inplace=True)
    df['NATUREZA'].replace('92', '94', inplace=True)

    df['NAT_JUR'].replace(['0000', '1236', '1244', '1260', '1279', '1333', '3301'], '', inplace=True)

    df['GESTAO'].replace('E', '2', inplace=True)
    df['GESTAO'].replace('M', '1', inplace=True)
    df['GESTAO'].replace(['3', '4', '5', '6', '7', '8', '9'], '', inplace=True)

    for col in np.array(['NACIONAL', 'CNAER']):
        df[col].replace(['000', '016', '463'], '', inplace=True)

    df['INSTRU'].replace(['5', '6', '7', '8', '9'], '', inplace=True)

    df['CBOR'].replace(['000000', '521140', '783230'], '', inplace=True)

    df['VINCPREV'].replace('9', '', inplace=True)

    df['FAEC_TP'].replace(['000000', '040066', '040067'], '', inplace=True)

    df['REGCT'].replace(['7112', '7113'], '', inplace=True)

    df['ETNIA'].replace('9999', '', inplace=True)

    # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
    for col in np.array(['UF_ZI', 'ESPEC', 'IDENT', 'MUNIC_RES', 'MARCA_UTI', 'PROC_SOLIC', 'PROC_REA', 'DIAG_PRINC',
                         'COBRANCA', 'NATUREZA', 'NAT_JUR', 'GESTAO', 'MUNIC_MOV', 'NACIONAL', 'CAR_INT', 'INSTRU',
                         'CONTRACEP1', 'CONTRACEP2', 'CBOR', 'CNAER', 'VINCPREV', 'CNES', 'COMPLEX', 'FINANC',
                         'FAEC_TP', 'REGCT', 'RACA_COR', 'ETNIA']):
        df[col].replace('', 'NA', inplace=True)

    # Substitui uma string vazia por None nas colunas de atributos especificadas
    for col in np.array(['MES_CMPT', 'CGC_HOSP', 'CEP', 'SEXO', 'HOMONIMO', 'CID_NOTIF', 'INSC_PN', 'GESTOR_TP',
                         'GESTOR_CPF', 'CNPJ_MANT', 'CID_ASSO', 'CID_MORTE', 'AUD_JUST', 'SIS_JUST']):
        df[col].replace('', None, inplace=True)

    # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes ("NaT") pela data...
    # futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
    for col in np.array(['NASC', 'DI_INTER', 'DT_SAIDA']):
        df[col] = df[col].apply(lambda x: datetime.strptime(x, '%Y%m%d').date() if x != '' else datetime(2099, 1, 1).date())

    # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui pela data futura "2099-01-01"
    for col in np.array(['NASC', 'DI_INTER', 'DT_SAIDA']):
        df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

    # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1) ou quando simplesmente se deseja inteiros
    for col in np.array(['ANO_CMPT', 'IND_VDRL', 'MORTE', 'GESTRISCO', 'INFEHOSP']):
        df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

    # Converte do tipo object para float sem casas decimais as colunas de atributos de valores representativos de quantidades
    # ou para o valor None caso a coluna esteja com a string vazia
    for col in np.array(['UTI_MES_TO', 'UTI_INT_TO', 'DIAR_ACOM', 'QT_DIARIAS', 'DIAS_PERM', 'NUM_FILHOS']):
        df[col] = df[col].apply(lambda x: round(float(x), 0) if x != '' else None)

    # Converte do tipo object para float com duas casas decimais as colunas de atributos de valores representativos de quantidades
    # ou para o valor None caso a coluna esteja com a string vazia
    for col in np.array(['VAL_SH', 'VAL_SP', 'VAL_TOT', 'VAL_UTI', 'US_TOT', 'VAL_SH_FED', 'VAL_SP_FED', 'VAL_SH_GES',
                         'VAL_SP_GES']):
        df[col] = df[col].apply(lambda x: round(float(x), 2) if x != '' else None)

    # Renomeia colunas que são foreign keys
    df.rename(index=str, columns={'IDENT': 'IDENT_ID', 'UF_ZI': 'UFZI_ID', 'ESPEC': 'ESPEC_ID', 'MUNIC_RES': 'MUNICRES_ID',
                                  'MARCA_UTI': 'MARCAUTI_ID', 'PROC_SOLIC': 'PROCSOLIC_ID', 'PROC_REA': 'PROCREA_ID',
                                  'DIAG_PRINC': 'DIAGPRINC_ID', 'COBRANCA': 'COBRANCA_ID', 'NATUREZA': 'NATUREZA_ID',
                                  'NAT_JUR': 'NATJUR_ID', 'GESTAO': 'GESTAO_ID', 'MUNIC_MOV': 'MUNICMOV_ID',
                                  'NACIONAL': 'NACIONAL_ID', 'CAR_INT': 'CARINT_ID', 'INSTRU': 'INSTRU_ID',
                                  'CONTRACEP1': 'CONTRACEP1_ID', 'CONTRACEP2': 'CONTRACEP2_ID', 'CBOR': 'CBOR_ID',
                                  'CNAER': 'CNAER_ID', 'VINCPREV': 'VINCPREV_ID', 'CNES': 'CNES_ID',
                                  'COMPLEX': 'COMPLEX_ID', 'FINANC': 'FINANC_ID', 'FAEC_TP': 'FAECTP_ID',
                                  'REGCT': 'REGCT_ID', 'RACA_COR': 'RACACOR_ID', 'ETNIA': 'ETNIA_ID'}, inplace=True)

    print('Terminou de tratar o arquivo RD{}{}{} (shape final: {} x {}).'.format(state, year, month, df.shape[0], df.shape[1]))

    return df


# Função para adequar e formatar as colunas e valores da TCC IDENT (arquivo IDENT.cnv)
def get_IDENT_treated():
    # Conversão da TCC IDENT para um objeto pandas DataFrame
    file_name = 'IDENT'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO_AIH'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


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
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE', '?', '?', '?', '?', '?', '?', 'NA', '?', '?', '?', np.nan, np.nan, np.nan, np.nan, '?', '?', '?']
    return df


# Função para adequar e formatar as colunas e valores da TCC LEITOS (arquivo LEITOS.cnv)
def get_LEITOS_treated():
    # Conversão da TCC LEITOS para um objeto pandas DataFrame
    file_name = 'LEITOS'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'LEITO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC MARCAUTI (arquivo MARCAUTI.cnv)
def get_MARCAUTI_treated():
    # Conversão da TCC MARCAUTI para um objeto pandas DataFrame
    file_name = 'MARCAUTI'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO_UTI'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da Tabela TB_SIGTAP (arquivo TB_SIGTAP.dbf)
def get_TB_SIGTAP_treated(path):
    # Conversão da Tabela TB_SIGTAP para um objeto pandas DataFrame
    file_name = 'TB_SIGTAP'
    df = download_table_dbf(file_name)
    # Renomeia as colunas especificadas
    df.rename(index=str, columns={'IP_COD': 'ID', 'IP_DSCR': 'PROCEDIMENTO'}, inplace=True)
    # Upload do arquivo "xlsx" que contém os PROC_SOLIC/PROC_REA presentes nos arquivos RDXXaamm (dos anos de 2008 a 2019)...
    # e não presentes no arquivo TB_SIGTAP.dbf. Ou seja, isso parece ser uma falha dos dados do Datasus
    dataframe = pd.read_excel(path + 'COD_PROCEDIMENTOS_OUT_TB_SIGTAP_ANOS_2008_2019' + '.xlsx')
    # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
    dataframe['ID'] = dataframe['ID'].astype('str')
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "dataframe" até formar uma "string" de tamanho = 10
    dataframe['ID'] = dataframe['ID'].apply(lambda x: x.zfill(10))
    # Adiciona a coluna DESCESTAB e respectivos valores ao objeto "dataframe"
    dataframe['PROCEDIMENTO'] = 'NAO PROVIDO NO ARQUIVO TB_SIGTAP.dbf'
    # Concatenação do objeto "dataframe" ao objeto "df"
    frames = []
    frames.append(df)
    frames.append(dataframe)
    dfinal = pd.concat(frames, ignore_index=True)
    dfinal.drop_duplicates(subset='ID', keep='first', inplace=True)
    dfinal.reset_index(drop=True, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
    return dfinal


# Função para adequar e formatar as colunas e valores da Tabela cid10 (arquivo cid10.dbf) e de 22 TCC com nome CID10_XX (arquivos "cnv")
# sendo XX indo de 01 a 22, um para cada capítulo do CID 10.
def get_CID10_treated():
    # Conversão da Tabela cid10 para um objeto pandas DataFrame
    file_name = 'cid10'
    df1 = download_table_dbf(file_name)
    # Renomeia as colunas especificadas
    df1.rename(index=str, columns={'CD_COD': 'ID', 'CD_DESCR': 'DIAGNOSTICO'}, inplace=True)
    # Coloca todas as string da coluna especificada como UPPER CASE
    df1['DIAGNOSTICO'] = df1['DIAGNOSTICO'].apply(lambda x: x.upper())
    # Ordena as linhas de "df1" por ordem crescente dos valores da coluna ID
    df1.sort_values(by=['ID'], inplace=True)
    # Reset o index devido ao sorting prévio
    df1.reset_index(drop=True, inplace=True)
    # Conversão das 22 TCC CID10_XX para um objeto pandas DataFrame
    frames = []
    for i in range(1, 23):
        i = str(i).zfill(2)
        file_name = 'CID10_' + i
        dfi = download_table_cnv(file_name)
        frames.append(dfi)
    df2 = pd.concat(frames, ignore_index=True)
    df2.drop_duplicates(subset='ID', keep='first', inplace=True)
    df2.sort_values(by=['ID'], inplace=True)
    df2.reset_index(drop=True, inplace=True)
    # Renomeia a coluna SIGNIFICACAO
    df2.rename(index=str, columns={'SIGNIFICACAO': 'DIAGNOSTICO'}, inplace=True)
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
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC SAIDAPERM (arquivo SAIDAPERM.cnv)
def get_SAIDAPERM_treated():
    # Conversão da TCC SAIDAPERM para um objeto pandas DataFrame
    file_name = 'SAIDAPERM'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'MOTIVO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC NATUREZA (arquivo NATUREZA.cnv)
def get_NATUREZA_treated():
    # Conversão da TCC NATUREZA para um objeto pandas DataFrame
    file_name = 'NATUREZA'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'NATUREZA'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC natjur (arquivo natjur.cnv)
def get_natjur_treated():
    # Conversão da TCC natjur para um objeto pandas DataFrame
    file_name = 'natjur'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'NATUREZA'}, inplace=True)
    # Drop a linha inteira em que a coluna "ID" tem o valor especificado por não representar nada
    df = df.drop(df[df['ID']=='0'].index)
    # Reset o index devido ao sorting prévio e à exclusão da linha referida acima
    df.reset_index(drop=True, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC GESTAO (arquivo GESTAO.cnv)
def get_GESTAO_treated():
    # Conversão da TCC GESTAO para um objeto pandas DataFrame
    file_name = 'GESTAO'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'GESTAO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC NACION3D (arquivo NACION3D.cnv)
def get_NACION3D_treated():
    # Conversão da TCC NACION3D para um objeto pandas DataFrame
    file_name = 'NACION3D'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'NACIONALIDADE'}, inplace=True)
    # Preenche os valores da coluna ID com zeros a esquerda até formar três digitos
    df['ID'] = df['ID'].apply(lambda x: x.zfill(3))
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC CARATEND (arquivo CARATEND.cnv)
def get_CARATEND_treated():
    # Conversão da TCC CARATEND para um objeto pandas DataFrame
    file_name = 'CARATEND'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'MOTIVO'}, inplace=True)
    # Coleta da coluna MOTIVO apenas a substring depois de dois dígitos e um espaço
    df['MOTIVO'].replace(to_replace='^\d{2}\s{1}', value= '', regex=True, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC INSTRU (arquivo INSTRU.cnv)
def get_INSTRU_treated():
    # Conversão da TCC INSTRU para um objeto pandas DataFrame
    file_name = 'INSTRU'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'NIVEL'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC CONTRAC (arquivo CONTRAC.cnv)
def get_CONTRAC_treated():
    # Conversão da TCC CONTRAC para um objeto pandas DataFrame
    file_name = 'CONTRAC'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'CONTRACEPTIVO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da Tabela CBO (arquivo CBO.dbf)
def get_CBO_treated():
    # Conversão da Tabela CBO para um objeto pandas DataFrame
    file_name = 'CBO'
    df = download_table_dbf(file_name)
    # Renomeia as colunas especificadas
    df.rename(index=str, columns={'CBO': 'ID', 'DS_CBO': 'OCUPACAO'}, inplace=True)
    # Drop a linha inteira em que a coluna "ID" tem o valor especificado por não representar valor válido
    df = df.drop(df[df['ID']=='000000'].index)
    # Ordena as linhas de "df" por ordem crescente dos valores da coluna ID
    df.sort_values(by=['ID'], inplace=True)
    # Elimina eventuais linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
    df.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Reset o index devido ao sorting prévio
    df.reset_index(drop=True, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC CNAE (arquivo CNAE.cnv)
def get_CNAE_treated():
    # Conversão da TCC CNAE para um objeto pandas DataFrame
    file_name = 'CNAE'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'ATIVIDADE'}, inplace=True)
    # Preenche os valores da coluna ID com zeros a esquerda até formar três digitos
    df['ID'] = df['ID'].apply(lambda x: x.zfill(3))
    # Coleta da coluna ATIVIDADE apenas a substring depois de três dígitos e um espaço
    df['ATIVIDADE'].replace(to_replace='^\d{3}\s{1}', value= '', regex=True, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC VINCPREV (arquivo VINCPREV.cnv)
def get_VINCPREV_treated():
    # Conversão da TCC VINCPREV para um objeto pandas DataFrame
    file_name = 'VINCPREV'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'VINCULO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da Tabela TCNESBR (arquivo TCNESBR.dbf), da Tabela HUF_MEC (arquivo HUF_MEC.dbf)...
# e da TCC HOSFEDRJ (arquivo "cnv")
def get_CNES_treated(path):
    # Conversão da Tabela TCNESBR para um objeto pandas DataFrame
    file_name = 'TCNESBR'
    df1 = download_table_dbf(file_name)
    # Renomeia as colunas especificadas
    df1.rename(index=str, columns={'CNES': 'ID', 'NOMEFANT': 'DESCESTAB'}, inplace=True)
    # Remove colunas indesejáveis do objeto pandas DataFrame
    df1 = df1.drop(['UF_ZI', 'CMPT'], axis=1)
    # Conversão da Tabela HUF_MEC para um objeto pandas DataFrame
    file_name = 'HUF_MEC'
    df2 = download_table_dbf(file_name)
    # Renomeia as colunas especificadas
    df2.rename(index=str, columns={'PA_CODUNI_': 'ID', 'FANTASIA': 'DESCESTAB'}, inplace=True)
    # Conversão da TCC HOSFEDRJ para um objeto pandas DataFrame
    file_name = 'HOSFEDRJ'
    df3 = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df3.rename(index=str, columns={'SIGNIFICACAO': 'DESCESTAB'}, inplace=True)
    # Conversão de "df1", "df2" e "df3" para um único objeto pandas DataFrame
    frames = []
    frames.append(df1)
    frames.append(df2)
    frames.append(df3)
    df = pd.concat(frames, ignore_index=True)
    # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
    df.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Ordena as linhas por ordem crescente dos valores da coluna ID
    df.sort_values(by=['ID'], inplace=True)
    # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
    df.reset_index(drop=True, inplace=True)
    # Upload do arquivo "xlsx" que contém os CNES presentes nos arquivos RDXXaamm (dos anos de 2008 a 2019) e não presentes...
    # nos arquivos TCNESBR.dbf, HUF_MEC.dbf e HOSFEDRJ.cnv. Ou seja, isso parece ser uma falha dos dados do Datasus
    dataframe = pd.read_excel(path + 'CNES_OUT_TCNESBR_PLUS_ANOS_2008_2019' + '.xlsx')
    # Converte a coluna "ID" do objeto "dataframe" de "int" para "string"
    dataframe['ID'] = dataframe['ID'].astype('str')
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "dataframe" até formar uma "string" de tamanho = 7
    dataframe['ID'] = dataframe['ID'].apply(lambda x: x.zfill(7))
    # Adiciona a coluna DESCESTAB e respectivos valores ao objeto "dataframe"
    dataframe['DESCESTAB'] = 'NAO PROVIDO EM 2 ARQUIVOS DBF E UM CNV DE CNES'
    # Concatenação do objeto "dataframe" ao objeto "df"
    frames = []
    frames.append(df)
    frames.append(dataframe)
    dfinal = pd.concat(frames, ignore_index=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
    return dfinal


# Função para adequar e formatar as colunas e valores da TCC COMPLEX2 (arquivo COMPLEX2.cnv)
def get_COMPLEX2_treated():
    # Conversão da TCC COMPLEX2 para um objeto pandas DataFrame
    file_name = 'COMPLEX2'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'COMPLEXIDADE'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC FINANC (arquivo FINANC.cnv)
def get_FINANC_treated():
    # Conversão da TCC FINANC para um objeto pandas DataFrame
    file_name = 'FINANC'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'FONTE'}, inplace=True)
    # Coleta da coluna FONTE apenas a substring depois de dois dígitos e um espaço
    df['FONTE'].replace(to_replace='^\d{2}\s{1}', value= '', regex=True, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC FAECTP (arquivo FAECTP.cnv)
def get_FAECTP_treated():
    # Conversão da TCC FAECTP para um objeto pandas DataFrame
    file_name = 'FAECTP'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'SUBFONTE'}, inplace=True)
    # Preenche os valores da coluna ID com zeros a esquerda até formar seis digitos
    df['ID'] = df['ID'].apply(lambda x: x.zfill(6))
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC REGCT (arquivo REGCT.cnv)
def get_REGCT_treated():
    # Conversão da TCC REGCT para um objeto pandas DataFrame
    file_name = 'REGCT'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'REGRA'}, inplace=True)
    # Preenche os valores da coluna ID com zeros a esquerda até formar quatro digitos
    df['ID'] = df['ID'].apply(lambda x: x.zfill(4))
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC RACACOR (arquivo RACACOR.cnv)
def get_RACACOR_treated():
    # Conversão da TCC RACACOR para um objeto pandas DataFrame
    file_name = 'RACACOR'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'RACA'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC etnia (arquivo etnia.cnv)
def get_etnia_treated():
    # Conversão da TCC etnia para um objeto pandas DataFrame
    file_name = 'etnia'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'INDIGENA'}, inplace=True)
    # Preenche os valores da coluna ID com zeros a esquerda até formar quatro digitos
    df['ID'] = df['ID'].apply(lambda x: x.zfill(4))
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
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
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" das tabelas UFZI, MUNICRES e MUNICMOV
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE', '?']
    return df



if __name__ == '__main__':

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    #df = get_RDXXaamm_treated('AC', '11', '12')

    df = get_TB_SIGTAP_treated()
    print(df)
