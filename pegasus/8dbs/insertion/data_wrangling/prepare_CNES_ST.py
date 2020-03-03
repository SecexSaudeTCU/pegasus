###########################################################################################################################################################################
# CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST #
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
Script de tratamento de dados do CNES_ST (Estabelecimentos) para atender ao framework do SGBD PostgreSQL.
Válido para os arquivos de dados STXXaamm (ST = Estabelecimentos; XX = Estado; aa = Ano; mm = Mês)...
(originalmente em formato "dbc") a partir do ano de 2006.

"""


# Função para converter um "value" num certo "type" de objeto ou caso não seja possível utiliza o valor "default"
def tryconvert(value, default, type):
    try:
        return type(value)
    except (ValueError, TypeError):
        return default


# Função para ler como um objeto pandas DataFrame um arquivo de dados STXXaamm do CNES e adequar e formatar suas colunas e valores
def get_STXXaamm_treated(state, year, month):
    # Lê o arquivo "dbc" como um objeto pandas DataFrame e ainda o salva no formato "parquet"
    dataframe = download_CNESXXaamm('ST', state, year, month)
    print('O número de linhas do arquivo ST{}{}{} é {}.'.format(state, year, month, dataframe.shape[0]))

    # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela STBR da base de dados
    lista_columns = np.array(['CNES', 'CODUFMUN', 'COD_CEP', 'CPF_CNPJ', 'PF_PJ', 'NIV_DEP', 'CNPJ_MAN', 'COD_IR', 'VINC_SUS',
                              'TPGESTAO', 'ESFERA_A', 'RETENCAO', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL', 'TP_UNID', 'TURNO_AT',
                              'NIV_HIER', 'TP_PREST', 'CO_BANCO', 'CO_AGENC', 'C_CORREN', 'ALVARA', 'DT_EXPED', 'ORGEXPED',
                              'AV_ACRED', 'CLASAVAL', 'DT_ACRED', 'AV_PNASS', 'DT_PNASS', 'GESPRG1E', 'GESPRG1M', 'GESPRG2E',
                              'GESPRG2M', 'GESPRG4E', 'GESPRG4M', 'NIVATE_A', 'GESPRG3E', 'GESPRG3M', 'GESPRG5E', 'GESPRG5M',
                              'GESPRG6E', 'GESPRG6M', 'NIVATE_H', 'QTLEITP1', 'QTLEITP2', 'QTLEITP3', 'LEITHOSP', 'QTINST01',
                              'QTINST02', 'QTINST03', 'QTINST04', 'QTINST05', 'QTINST06', 'QTINST07', 'QTINST08', 'QTINST09',
                              'QTINST10', 'QTINST11', 'QTINST12', 'QTINST13', 'QTINST14', 'URGEMERG', 'QTINST15', 'QTINST16',
                              'QTINST17', 'QTINST18', 'QTINST19', 'QTINST20', 'QTINST21', 'QTINST22', 'QTINST23', 'QTINST24',
                              'QTINST25', 'QTINST26', 'QTINST27', 'QTINST28', 'QTINST29', 'QTINST30', 'ATENDAMB', 'QTINST31',
                              'QTINST32', 'QTINST33', 'CENTRCIR', 'QTINST34', 'QTINST35', 'QTINST36', 'QTINST37', 'CENTROBS',
                              'QTLEIT05', 'QTLEIT06', 'QTLEIT07', 'QTLEIT09', 'QTLEIT19', 'QTLEIT20', 'QTLEIT21', 'QTLEIT22',
                              'QTLEIT23', 'QTLEIT32', 'QTLEIT34', 'QTLEIT38', 'QTLEIT39', 'QTLEIT40', 'CENTRNEO', 'ATENDHOS',
                              'SERAP01P', 'SERAP01T', 'SERAP02P', 'SERAP02T', 'SERAP03P', 'SERAP03T', 'SERAP04P', 'SERAP04T',
                              'SERAP05P', 'SERAP05T', 'SERAP06P', 'SERAP06T', 'SERAP07P', 'SERAP07T', 'SERAP08P', 'SERAP08T',
                              'SERAP09P', 'SERAP09T', 'SERAP10P', 'SERAP10T', 'SERAP11P', 'SERAP11T', 'SERAPOIO', 'RES_BIOL',
                              'RES_QUIM', 'RES_RADI', 'RES_COMU', 'COLETRES', 'COMISS01', 'COMISS02', 'COMISS03', 'COMISS04',
                              'COMISS05', 'COMISS06', 'COMISS07', 'COMISS08', 'COMISS09', 'COMISS10', 'COMISS11', 'COMISS12',
                              'COMISSAO', 'AP01CV01', 'AP01CV02', 'AP01CV05', 'AP01CV06', 'AP01CV03', 'AP01CV04', 'AP02CV01',
                              'AP02CV02', 'AP02CV05', 'AP02CV06', 'AP02CV03', 'AP02CV04', 'AP03CV01', 'AP03CV02', 'AP03CV05',
                              'AP03CV06', 'AP03CV03', 'AP03CV04', 'AP04CV01', 'AP04CV02', 'AP04CV05', 'AP04CV06', 'AP04CV03',
                              'AP04CV04', 'AP05CV01', 'AP05CV02', 'AP05CV05', 'AP05CV06', 'AP05CV03', 'AP05CV04', 'AP06CV01',
                              'AP06CV02', 'AP06CV05', 'AP06CV06', 'AP06CV03', 'AP06CV04', 'AP07CV01', 'AP07CV02', 'AP07CV05',
                              'AP07CV06', 'AP07CV03', 'AP07CV04', 'ATEND_PR', 'NAT_JUR'])

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

    for col in np.array(['ESFERA_A', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL', 'TP_UNID', 'TURNO_AT', 'NIV_HIER']):
        for i in np.array(['00', '01', '02', '03', '04', '05', '06', '07', '08', '09']):
            df[col].replace(i, str(int(i)), inplace=True)

    df['C_CORREN'] = df['C_CORREN'].apply(lambda x: str(tryconvert(x, '', int)))

    # Atualiza/corrige os labels das colunas especificadas

    df['CODUFMUN'] = df['CODUFMUN'].apply(lambda x: x if len(x) == 6 else '')
    df['CODUFMUN'].replace([str(i) for i in range(334501, 334531)], '330455', inplace=True)
    df['CODUFMUN'].replace([str(i) for i in range(358001, 358059)], '355030', inplace=True)
    df['CODUFMUN'].replace(['000000', '150475', '421265', '422000', '431454', '500627', '510445', '999999'], '', inplace=True)
    df['CODUFMUN'].replace(['530020', '530030', '530040', '530050', '530060', '530070', '530080', '530090',
                            '530100', '530110',  '530120', '530130', '530135', '530140', '530150', '530160',
                            '530170', '530180'] + [str(i) for i in range(539900, 540000)], '530010', inplace=True)

    df['NIV_DEP'].replace('5', '', inplace=True)

    df['COD_IR'].replace('00', '', inplace=True)
    df['COD_IR'].replace('IR', '19', inplace=True)

    df['TPGESTAO'].replace('S', 'Z', inplace=True)

    for col in np.array(['ESFERA_A', 'RETENCAO', 'ATIVIDAD', 'NATUREZA', 'CLIENTEL', 'TURNO_AT', 'TP_PREST']):
        df[col].replace('99', '', inplace=True)

    df['TP_UNID'].replace(['3', '84', '85'], '', inplace=True)

    df['NIV_HIER'].replace(['0', '99'], '', inplace=True)

    df['DT_EXPED'] = df['DT_EXPED'].apply(lambda x: x if len(x) == 8 else '')
    df['DT_EXPED'] = df['DT_EXPED'].apply(lambda x: x if 1 <= tryconvert(x[4:6], 0, int) <= 12 else '')
    df['DT_EXPED'] = df['DT_EXPED'].apply(lambda x: x if 1 <= tryconvert(x[6:8], 0, int) <= 31 else '')

    df['ORGEXPED'].replace('9', '', inplace=True)

    for col in np.array(['AV_ACRED', 'AV_PNASS']):
        df[col].replace(['0', '3', '4', '5', '6', '7', '8', '9'], '', inplace=True)
        df[col].replace('2', '0', inplace=True) # "2", representativo de "Não", é convertido para o objeto string "0" do domínio binário


    df['CLASAVAL'].replace(['N', 'A'], '', inplace=True)

    df['NAT_JUR'].replace('1333', '1000', inplace=True)
    df['NAT_JUR'].replace('2100', '2000', inplace=True)
    df['NAT_JUR'].replace('3301', '3000', inplace=True)

    # Substitui uma string vazia pela string "NA" nas colunas de foreign keys
    for col in np.array(['CNES', 'CODUFMUN', 'PF_PJ', 'NIV_DEP', 'COD_IR', 'TPGESTAO', 'ESFERA_A', 'RETENCAO', 'ATIVIDAD',
                         'NATUREZA', 'CLIENTEL', 'TP_UNID', 'TURNO_AT', 'NIV_HIER', 'TP_PREST', 'ORGEXPED', 'CLASAVAL', 'NAT_JUR']):
        df[col].replace('', 'NA', inplace=True)

    # Substitui uma string vazia por None nas colunas de atributos especificadas
    for col in np.array(['COD_CEP', 'CPF_CNPJ', 'CNPJ_MAN', 'CO_BANCO', 'CO_AGENC', 'C_CORREN', 'ALVARA']):
        df[col].replace('', None, inplace=True)

    # Converte do tipo string para datetime as colunas especificadas substituindo as datas faltantes ("NaT") pela data...
    # futura "2099-01-01" para permitir a inserção das referidas colunas no SGBD postgreSQL
    df['DT_EXPED'] = df['DT_EXPED'].apply(lambda x: datetime.strptime(x, '%Y%m%d').date() if x != '' else datetime(2099, 1, 1).date())

    for col in np.array(['DT_ACRED', 'DT_PNASS']):
        df[col] = df[col].apply(lambda x: datetime.strptime(x, '%Y%m').date() if x != '' else datetime(2099, 1, 1).date())

    # Verifica se as datas das colunas especificadas são absurdas e em caso afirmativo as substitui pela data futura "2099-01-01"
    for col in np.array(['DT_EXPED', 'DT_ACRED', 'DT_PNASS']):
        df[col] = df[col].apply(lambda x: x if datetime(1850, 12, 31).date() < x < datetime(2020, 12, 31).date() else datetime(2099, 1, 1).date())

    # Converte do tipo object para int ou para None as colunas de atributos de valores binários (0 ou 1)
    for col in np.array(['VINC_SUS', 'AV_ACRED', 'AV_PNASS', 'GESPRG1E', 'GESPRG1M', 'GESPRG2E', 'GESPRG2M', 'GESPRG4E', 'GESPRG4M',
                         'NIVATE_A', 'GESPRG3E', 'GESPRG3M', 'GESPRG5E', 'GESPRG5M', 'GESPRG6E', 'GESPRG6M', 'NIVATE_H', 'LEITHOSP',
                         'URGEMERG', 'ATENDAMB', 'CENTRCIR', 'CENTROBS', 'CENTRNEO', 'ATENDHOS', 'SERAP01P', 'SERAP01T', 'SERAP02P',
                         'SERAP02T', 'SERAP03P', 'SERAP03T', 'SERAP04P', 'SERAP04T', 'SERAP05P', 'SERAP05T', 'SERAP06P', 'SERAP06T',
                         'SERAP07P', 'SERAP07T', 'SERAP08P', 'SERAP08T', 'SERAP09P', 'SERAP09T', 'SERAP10P', 'SERAP10T', 'SERAP11P',
                         'SERAP11T', 'SERAPOIO', 'RES_BIOL', 'RES_QUIM', 'RES_RADI', 'RES_COMU', 'COLETRES', 'COMISS01', 'COMISS02',
                         'COMISS03', 'COMISS04', 'COMISS05', 'COMISS06', 'COMISS07', 'COMISS08', 'COMISS09', 'COMISS10', 'COMISS11',
                         'COMISS12', 'COMISSAO', 'AP01CV01', 'AP01CV02', 'AP01CV05', 'AP01CV06', 'AP01CV03', 'AP01CV04', 'AP02CV01',
                         'AP02CV02', 'AP02CV05', 'AP02CV06', 'AP02CV03', 'AP02CV04', 'AP03CV01', 'AP03CV02', 'AP03CV05', 'AP03CV06',
                         'AP03CV03', 'AP03CV04', 'AP04CV01', 'AP04CV02', 'AP04CV05', 'AP04CV06', 'AP04CV03', 'AP04CV04', 'AP05CV01',
                         'AP05CV02', 'AP05CV05', 'AP05CV06', 'AP05CV03', 'AP05CV04', 'AP06CV01', 'AP06CV02', 'AP06CV05', 'AP06CV06',
                         'AP06CV03', 'AP06CV04', 'AP07CV01', 'AP07CV02', 'AP07CV05', 'AP07CV06', 'AP07CV03', 'AP07CV04', 'ATEND_PR']):
        df[col] = df[col].apply(lambda x: tryconvert(x, None, int))

    # Converte do tipo object para float sem casas decimais as colunas de atributos de valores representativos de quantidades
    # ou para o valor None caso a coluna esteja com a string vazia
    for col in np.array(['QTLEITP1', 'QTLEITP2', 'QTLEITP3', 'QTINST01', 'QTINST02', 'QTINST03', 'QTINST04', 'QTINST05', 'QTINST06',
                         'QTINST07', 'QTINST08', 'QTINST09', 'QTINST10', 'QTINST11', 'QTINST12', 'QTINST13', 'QTINST14', 'QTINST15',
                         'QTINST16', 'QTINST17', 'QTINST18', 'QTINST19', 'QTINST20', 'QTINST21', 'QTINST22', 'QTINST23', 'QTINST24',
                         'QTINST25', 'QTINST26', 'QTINST27', 'QTINST28', 'QTINST29', 'QTINST30', 'QTINST31', 'QTINST32', 'QTINST33',
                         'QTINST34', 'QTINST35', 'QTINST36', 'QTINST37', 'QTLEIT05', 'QTLEIT06', 'QTLEIT07', 'QTLEIT09', 'QTLEIT19',
                         'QTLEIT20', 'QTLEIT21', 'QTLEIT22', 'QTLEIT23', 'QTLEIT32', 'QTLEIT34', 'QTLEIT38', 'QTLEIT39', 'QTLEIT40']):
        df[col] = df[col].apply(lambda x: round(float(x),0) if x != '' else None)

    # Renomeia colunas que são foreign keys
    df.rename(index=str, columns={'CNES': 'CNES_ID', 'CODUFMUN': 'CODUFMUN_ID', 'PF_PJ': 'PFPJ_ID',
                                  'NIV_DEP': 'NIVDEP_ID', 'COD_IR': 'CODIR_ID', 'TPGESTAO': 'TPGESTAO_ID',
                                  'ESFERA_A': 'ESFERAA_ID', 'RETENCAO': 'RETENCAO_ID', 'ATIVIDAD': 'ATIVIDAD_ID',
                                  'NATUREZA': 'NATUREZA_ID', 'CLIENTEL': 'CLIENTEL_ID', 'TP_UNID': 'TPUNID_ID',
                                  'TURNO_AT': 'TURNOAT_ID', 'NIV_HIER': 'NIVHIER_ID',  'TP_PREST': 'TPPREST_ID',
                                  'ORGEXPED': 'ORGEXPED_ID', 'CLASAVAL': 'CLASAVAL_ID', 'NAT_JUR': 'NATJUR_ID'}, inplace=True)

    print('Terminou de tratar o arquivo ST{}{}{} (shape final: {} x {}).'.format(state, year, month, df.shape[0], df.shape[1]))

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
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
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
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE', '?', '?', '?', '?', '?', '?', 'NA', '?', '?', '?', np.nan, np.nan, np.nan, np.nan, '?', '?', '?']
    return df


# Função para adequar e formatar as colunas e valores da TCC TP_PFPJ (arquivo TP_PFPJ.cnv)
def get_TP_PFPJ_treated():
    # Conversão da TCC TP_PFPJ para um objeto pandas DataFrame
    file_name = 'TP_PFPJ'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'PESSOA'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC NIVELDEP (arquivo NIVELDEP.cnv)
def get_NIVELDEP_treated():
    # Conversão da TCC NIVELDEP para um objeto pandas DataFrame
    file_name = 'NIVELDEP'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC RETENMAN (arquivo RETENMAN.cnv)
def get_RETENMAN_treated():
    # Conversão da TCC RETENMAN para um objeto pandas DataFrame
    file_name = 'RETENMAN'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'RETENCAO'}, inplace=True)
    # Drop a linha inteira em que a coluna "ID" tem o valor especificado
    df = df.drop(df[df['ID']=='IR'].index)
    # Reset o index devido à exclusão efetuada no passo anterior
    df.reset_index(drop=True, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC TPGESTAO (arquivo TPGESTAO.cnv)
def get_TPGESTAO_treated():
    # Conversão da TCC TPGESTAO para um objeto pandas DataFrame
    file_name = 'TPGESTAO'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'GESTAO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC EsferAdm (arquivo EsferAdm.cnv)
def get_EsferAdm_treated():
    # Conversão da TCC EsferAdm para um objeto pandas DataFrame
    file_name = 'EsferAdm'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'ADMINISTRACAO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC RETENCAO (arquivo RETENCAO.cnv)
def get_RETENCAO_treated():
    # Conversão da TCC RETENCAO para um objeto pandas DataFrame
    file_name = 'RETENCAO'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'RETENCAO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC Ativ_Ens (arquivo Ativ_Ens.cnv)
def get_Ativ_Ens_treated():
    # Conversão da TCC Ativ_Ens para um objeto pandas DataFrame
    file_name = 'Ativ_Ens'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'ATIVIDADE'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC NATUREZA (arquivo NATUREZA.cnv)
def get_NATUREZA_treated():
    # Conversão da TCC NATUREZA para um objeto pandas DataFrame
    file_name = 'NATUREZA'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'NATUREZA'}, inplace=True)
    # Drop a linha inteira em que a coluna "ID" tem o valor especificado
    df = df.drop(df[df['ID']=='0'].index)
    # Reset o index devido à exclusão efetuada no passo anterior
    df.reset_index(drop=True, inplace=True)
    # Coleta da coluna NATUREZA apenas a substring depois de dois dígitos e um traço
    df1 = df['NATUREZA'].str.extract('^\d{2}-(.*)', expand=True).rename(columns={0:'NATUREZA'})
    # Concatena ao longo do eixo das colunas os objetos pandas DataFrame "df[['ID']]" e "df1"
    dfinal = pd.concat([df[['ID']], df1], axis=1)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
    return dfinal


# Função para adequar e formatar as colunas e valores da TCC Flux_Cli (arquivo Flux_Cli.cnv)
def get_Flux_Cli_treated():
    # Conversão da TCC Flux_Cli para um objeto pandas DataFrame
    file_name = 'Flux_Cli'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'CLIENTELA'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC TP_ESTAB (arquivo TP_ESTAB.cnv)
def get_TP_ESTAB_treated():
    # Conversão da TCC TP_ESTAB para um objeto pandas DataFrame
    file_name = 'TP_ESTAB'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'TIPO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC TurnosAt (arquivo TurnosAt.cnv)
def get_TurnosAt_treated():
    # Conversão da TCC TurnosAt para um objeto pandas DataFrame
    file_name = 'TurnosAt'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'TURNO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC NIV_HIER (arquivo NIV_HIER.cnv)
def get_NIV_HIER_treated():
    # Conversão da TCC NIV_HIER para um objeto pandas DataFrame
    file_name = 'NIV_HIER'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'NIVEL'}, inplace=True)
    # Coleta da coluna NIVEL apenas a substring depois de um traço
    df1 = df['NIVEL'].str.extract('^NH \d-(.*)', expand=True).rename(columns={0:'NIVEL'})
    # Concatena ao longo do eixo das colunas os objetos pandas DataFrame "df[['ID']]" e "df1"
    dfinal = pd.concat([df[['ID']], df1], axis=1)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    dfinal.loc[dfinal.shape[0]] = ['NA', 'NOT AVAILABLE']
    return dfinal


# Função para adequar e formatar as colunas e valores da TCC TIPOPRES (arquivo TIPOPRES.cnv)
def get_TIPOPRES_treated():
    # Conversão da TCC TIPOPRES para um objeto pandas DataFrame
    file_name = 'TIPOPRES'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'PRESTADOR'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC ORGEXPED (arquivo ORGEXPED.cnv)
def get_ORGEXPED_treated():
    # Conversão da TCC ORGEXPED para um objeto pandas DataFrame
    file_name = 'ORGEXPED'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'EXPEDIDOR'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC CLASAVAL (arquivo CLASAVAL.cnv)
def get_CLASAVAL_treated():
    # Conversão da TCC CLASAVAL para um objeto pandas DataFrame
    file_name = 'CLASAVAL'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'AVALIACAO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']
    return df


# Função para adequar e formatar as colunas e valores da TCC NATJUR (arquivo NATJUR.cnv)
def get_NATJUR_treated():
    # Conversão da TCC NATJUR para um objeto pandas DataFrame
    file_name = 'NATJUR'
    df = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df.rename(index=str, columns={'SIGNIFICACAO': 'NATUREZA'}, inplace=True)
    # Drop a linha inteira em que a coluna "ID" tem o valor especificado por não representar nada
    df = df.drop(df[df['ID']=='0'].index)
    # Reset o index devido à eliminação de linha efetuada no passo anterior
    df.reset_index(drop=True, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela STBR
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
    the_path = os.getcwd()[:-len('insertion\\data_wrangling')] + 'files\\CNES\\'

    df = get_CADGERBR_treated(the_path)

    print(df)
