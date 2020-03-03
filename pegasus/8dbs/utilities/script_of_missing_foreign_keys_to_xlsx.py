# import sys
# sys.path.append('C:\\Users\\ericc\\Desktop\\susansa\\insertion\\data_wrangling\\online\\')

from insertion.data_wrangling.online.folder import CACHEPATH
from insertion.data_wrangling.online.read import read_dbc, read_cnv

import os
import pandas as pd
import numpy as np


"""
Para criação de arquivos "xlsx" de valores de determinadas colunas de uma tabela principal de dados que
não constam das tabelas relacionais respectivas (originais com extensão "dbf" ou "cnv")

"""


# Função para converter um "value" num certo "type" de objeto ou caso não seja possível utiliza o valor "default"
def tryconvert(value, default, type):
    try:
        return type(value)
    except (ValueError, TypeError):
        return default


########################################################################################################################################################
#  SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC SINASC  #
########################################################################################################################################################

def get_DNXXaaaa_simplified(state, year):
    dataframe = download_DNXXaaaa(state, year)
    # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela dnbr da Base de Dados
    lista_columns = ['NUMERODN', 'CODESTAB', 'CODMUNNASC', 'CODMUNRES', 'CODOCUPMAE', 'CODANOMAL', 'NATURALMAE']
    # Criação de um objeto pandas DataFrame vazio com as colunas especificadas acima
    df = pd.DataFrame(columns=lista_columns)
    # Colocação dos dados da variável dataframe na variável df nas colunas de mesmo nome preenchendo automaticamente com o float NaN...
    # as colunas da variável df não presentes na variável dataframe
    for col in df.columns.values:
        for coluna in dataframe.columns.values:
            if coluna == col:
                df[col] = dataframe[coluna].tolist()
                break
    # Coloca na variável dif_set o objeto set dos nomes das colunas da variável df que não estão presentes na variável dataframe
    dif_set = set(lista_columns) - set(dataframe.columns.values)
    # Substitui o float NaN pela string vazia as colunas da variável df não presentes na variável dataframe
    for col in dif_set:
        df[col].replace(np.nan, '', inplace=True)

    # Simplifica/corrige a apresentação dos dados das colunas especificadas
    df['CODESTAB'] = df['CODESTAB'].apply(lambda x: x.zfill(7))

    df['CODOCUPMAE'] = df['CODOCUPMAE'].apply(lambda x: x.zfill(6))
    df['CODOCUPMAE'] = df['CODOCUPMAE'].apply(str.strip)
    df['CODOCUPMAE'] = df['CODOCUPMAE'].apply(lambda x: x if len(x) == 6 else '')

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

    df['CODOCUPMAE'] = df['CODOCUPMAE'].apply(lambda x: x if 'ó' not in x else '')
    df['CODOCUPMAE'] = df['CODOCUPMAE'].apply(lambda x: x if ' ' not in x else '')
    df['CODOCUPMAE'] = df['CODOCUPMAE'].apply(lambda x: x if 'X' not in x else '')
    df['CODOCUPMAE'].replace('000000', '', inplace=True)

    df['CODANOMAL'].replace('Q314', 'P288', inplace=True)
    df['CODANOMAL'].replace('Q350', 'Q351', inplace=True)
    df['CODANOMAL'].replace('Q352', 'Q353', inplace=True)
    df['CODANOMAL'].replace('Q354', 'Q355', inplace=True)
    df['CODANOMAL'].replace(['Q356', 'Q358'], 'Q359', inplace=True)

    df['NATURALMAE'].replace(['000', '999'], '', inplace=True)
    df['NATURALMAE'].replace('800', '001', inplace=True)
    return df


def get_missing_codestab_SINASC(path):
    # Conversão da Tabela CNESDN18 para um objeto pandas DataFrame
    file_name = 'CNESDN18'
    df1 = download_table_dbf(file_name)
    # Ordena as linhas de "df1" por ordem crescente dos valores da coluna CODESTAB
    df1.sort_values(by=['CODESTAB'], inplace=True)
    # Reset o index devido ao sorting prévio
    df1.reset_index(drop=True, inplace=True)
    # Converte a coluna especificada de string para int
    df1['CODESTAB'] = df1['CODESTAB'].astype('int')
    # Conversão da TCC CNESDN07 para um objeto pandas DataFrame
    file_name = 'CNESDN07'
    df2 = download_table_cnv(file_name)
    # Renomeia as colunas especificadas
    df2.rename(index=str, columns={'ID': 'CODESTAB', 'SIGNIFICACAO': 'DESCESTAB'}, inplace=True)
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
    # Conversão da TCC ESFEDN07 para um objeto pandas DataFrame
    file_name = 'ESFEDN07'
    df3 = download_table_cnv(file_name)
    # Adequa e formata a TCC ESFEDN07
    df3.rename(index=str, columns={'ID': 'CODESTAB', 'SIGNIFICACAO': 'ESFERA'}, inplace=True)
    # Conversão da TCC NATDN07 para um objeto pandas DataFrame
    file_name = 'NATDN07'
    df4 = download_table_cnv(file_name)
    # Adequa e formata a TCC NATDN07
    df4.rename(index=str, columns={'ID': 'CODESTAB', 'SIGNIFICACAO': 'REGIME'}, inplace=True)
    # Realiza o "merge" da TCC NATDN07 à TCC ESFEDN07
    df3['REGIME'] = df4['REGIME'].tolist() # Isso só é possível corretamente com essa rotina pois o número de linhas e a...
                                           # ordem dos valores das colunas "CODESTAB" dos objetos pandas DataFrame "df3"...
                                           # e "df4" são os mesmos
    # Elimina linhas duplicadas tendo por base a coluna CODESTAB e mantém a primeira ocorrência
    df3.drop_duplicates(subset='CODESTAB', keep='first', inplace=True)

    # Realiza o "merge" da TCC ESFEDN07 (+ TCC NATDN07) à (Tabela CNESDN18 + TCC CNESDN07)
    df5 = df.append(df3, sort=False)
    df6 = df5.replace(np.nan,'').groupby('CODESTAB',as_index=False).agg(''.join)
    df6.sort_values(by=['CODESTAB'], inplace=True)
    df6.reset_index(drop=True, inplace=True)
    # Converte a coluna "CODESTAB" do objeto "df6" de "int" para "string"
    df6['CODESTAB'] = df6['CODESTAB'].astype('str')
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna "CODESTAB" do objeto "df6" até formar uma "string" de tamanho = 7
    df6['CODESTAB'] = df6['CODESTAB'].apply(lambda x: x.zfill(7))

    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE' ,'SP', 'TO']
    for ano in range(1997, 2018, 1):
        for estado in lista_estados:
            print('\nDN{}{}'.format(estado, ano))
            try:
                df_DN = get_DNXXaaaa_simplified(estado, ano)
            except:
                print('O arquivo DN{}{} não existe.'.format(estado, ano))
                continue
            else:
                print('O número de linhas do DN{}{} no início é: {}'.format(estado, ano, df_DN.shape[0]))
                df_DN = df_DN.drop(df_DN[df_DN['CODESTAB']==''].index)
                print('O número de linhas do DN{}{} após exclusão das string vazias é: {}'.format(estado, ano, df_DN.shape[0]))
                df_DN.drop_duplicates(subset='CODESTAB', keep='first', inplace=True)
                print('O número de linhas do DN{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, df_DN.shape[0]))
                frames.append(df_DN)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset='CODESTAB', keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Converte a coluna CODESTAB de "int" para "string"
    full_df['CODESTAB'] = full_df['CODESTAB'].astype(str)
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna CODESTAB do objeto "full_df" até formar uma "string" de tamanho = 7
    full_df['CODESTAB'] = full_df['CODESTAB'].apply(lambda x: x.zfill(7))
    # Converte para um objeto list a coluna CODESTAB do objeto "full_df"
    full_codigos = full_df['CODESTAB'].tolist()

    # Obtém em um arquivo "xlsx"
    full_codestab = df6['CODESTAB'].tolist()
    dif_codestab = list(set(full_codigos) - set(full_codestab))
    dif_codestab.sort()
    df_dif_codestab = pd.DataFrame(columns=['CODESTAB'])
    df_dif_codestab['CODESTAB'] = dif_codestab
    df_dif_codestab.to_excel(os.getcwd() + '\\SINASC_CODESTAB_MISSING.xlsx', index=False)
    print('Game over!')


def get_missing_codocupmae_SINASC(path):
    # Conversão da Tabela TABOCUP (em formato "dbf") para um objeto pandas DataFrame
    file_name = 'TABOCUP'
    df1 = download_table_dbf(file_name, cache=True)
    # Renomeia as colunas especificadas
    df1.rename(index=str, columns={'CODIGO': 'ID', 'DESCRICAO': 'OCUPACAO'}, inplace=True)
    # Ordena as linhas de "df1" por ordem crescente dos valores da coluna ID
    df1.sort_values(by=['ID'], inplace=True)
    # Reset o index devido ao sorting prévio
    df1.reset_index(drop=True, inplace=True)
    # Converte a coluna especificada de string para int
    df1['ID'] = df1['ID'].astype('int')
    # Conversão da TCC CBO2002 para um objeto pandas DataFrame
    file_name = 'CBO2002'
    df2 = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df2.rename(index=str, columns={'SIGNIFICACAO': 'OCUPACAO'}, inplace=True)
    # Coloca todas as string da coluna especificada como UPPER CASE
    df2['OCUPACAO'] = df2['OCUPACAO'].apply(lambda x: x.upper())
    # Conversão da TCC OCUPA para um objeto pandas DataFrame
    file_name = 'OCUPA'
    df3 = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df3.rename(index=str, columns={'SIGNIFICACAO': 'OCUPACAO'}, inplace=True)
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
    # Converte para string a coluna especificada
    df['ID'] = df['ID'].astype('str')
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna ID do objeto "df" até formar uma "string" de tamanho = 6
    df['ID'] = df['ID'].apply(lambda x: x.zfill(6))

    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE' ,'SP', 'TO']
    for ano in range(1997, 2018, 1):
        for estado in lista_estados:
            print('\nDN{}{}'.format(estado, ano))
            try:
                df_DN = get_DNXXaaaa_simplified(estado, ano)
            except:
                print('O arquivo DN{}{} não existe.'.format(estado, ano))
                continue
            else:
                print('O número de linhas do DN{}{} no início é: {}'.format(estado, ano, df_DN.shape[0]))
                df_DN = df_DN.drop(df_DN[df_DN['CODOCUPMAE']==''].index)
                print('O número de linhas do DN{}{} após exclusão das string vazias é: {}'.format(estado, ano, df_DN.shape[0]))
                df_DN.drop_duplicates(subset='CODOCUPMAE', keep='first', inplace=True)
                print('O número de linhas do DN{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, df_DN.shape[0]))
                frames.append(df_DN)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset='CODOCUPMAE', keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Converte a coluna CODOCUPMAE de "int" para "string"
    full_df['CODOCUPMAE'] = full_df['CODOCUPMAE'].astype(str)
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna CODOCUPMAE do objeto "full_df" até formar uma "string" de tamanho = 6
    full_df['CODOCUPMAE'] = full_df['CODOCUPMAE'].apply(lambda x: x.zfill(6))
    # Converte para um objeto list a coluna CODOCUPMAE do objeto "full_df"
    full_codigos = full_df['CODOCUPMAE'].tolist()

    # Obtém em um arquivo "xlsx"
    full_codocupmae = df['ID'].tolist()
    dif_codocupmae = list(set(full_codigos) - set(full_codocupmae))
    dif_codocupmae.sort()
    df_dif_codocupmae = pd.DataFrame(columns=['ID'])
    df_dif_codocupmae['ID'] = dif_codocupmae
    df_dif_codocupmae.to_excel(os.getcwd() + '\\SINASC_CODOCUPMAE_MISSING.xlsx', index=False)
    print('Game over!')


def get_missing_codanomal_SINASC(path):
    # Conversão da Tabela CID10 para um objeto pandas DataFrame
    file_name = 'CID10'
    df1 = download_table_dbf(file_name, cache=True)
    # Remove colunas indesejáveis do objeto pandas DataFrame
    df1 = df1.drop(['OPC', 'CAT', 'SUBCAT', 'RESTRSEXO'], axis=1)
    # Renomeia as colunas especificadas
    df1.rename(index=str, columns={'CID10': 'ID', 'DESCR': 'ANOMALIA'}, inplace=True)
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

    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE' ,'SP', 'TO']
    for ano in range(1997, 2018, 1):
        for estado in lista_estados:
            print('\nDN{}{}'.format(estado, ano))
            try:
                df_DN = get_DNXXaaaa_simplified(estado, ano)
            except:
                print('O arquivo DN{}{} não existe.'.format(estado, ano))
                continue
            else:
                print('O número de linhas do DN{}{} no início é: {}'.format(estado, ano, df_DN.shape[0]))
                df_DN = df_DN.drop(df_DN[df_DN['CODANOMAL']==''].index)
                print('O número de linhas do DN{}{} após exclusão das string vazias é: {}'.format(estado, ano, df_DN.shape[0]))
                df_DN.drop_duplicates(subset='CODANOMAL', keep='first', inplace=True)
                print('O número de linhas do DN{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, df_DN.shape[0]))
                frames.append(df_DN)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset='CODANOMAL', keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Considera apenas a primeira anomalia descrita
    lista = []
    for valor in full_df['CODANOMAL']:
        if len(valor) > 4:
            subst = valor[:4]
        else:
            subst = valor
        lista.append(subst)
    full_df['CODANOMAL'] = lista
    full_codigos = full_df['CODANOMAL'].tolist()

    # Obtém em um arquivo "xlsx"
    full_codanomal = df['ID'].tolist()
    dif_codanomal = list(set(full_codigos) - set(full_codanomal))
    dif_codanomal.sort()
    df_dif_codanomal = pd.DataFrame(columns=['CODANOMAL'])
    df_dif_codanomal['CODANOMAL'] = dif_codanomal
    df_dif_codanomal.to_excel(os.getcwd() + '\\SINASC_CODANOMAL_MISSING.xlsx', index=False)
    print('Game over!')


#################################################################################################################################################################
#  SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM SIM  #
#################################################################################################################################################################

def get_DOXXaaaa_simplified(state, year):
    dataframe = download_DOXXaaaa(state, year)
    # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela dobr da Base de Dados
    lista_columns = ['NUMERODO', 'NATURAL', 'CODMUNNATU', 'OCUP', 'CODMUNRES', 'CODESTAB', 'CODMUNOCOR', 'CAUSABAS', 'CAUSABAS_O']
    # Criação de um objeto pandas DataFrame vazio com as colunas especificadas acima
    df = pd.DataFrame(columns=lista_columns)
    # Colocação dos dados da variável dataframe na variável df nas colunas de mesmo nome preenchendo automaticamente
    # com o float NaN as colunas da variável df não presentes na variável dataframe
    for col in df.columns.values:
        for coluna in dataframe.columns.values:
            if coluna == col:
                df[col] = dataframe[coluna].tolist()
                break
    # Coloca na variável dif_set o objeto set dos nomes das colunas da variável df que não estão presentes na variável dataframe
    dif_set = set(lista_columns) - set(dataframe.columns.values)
    # Substitui o float NaN pela string vazia as colunas da variável df não presentes na variável dataframe
    for col in dif_set:
        df[col].replace(np.nan, '', inplace=True)

    # Simplifica/corrige a apresentação dos dados das colunas especificadas
    df['NATURAL'] = df['NATURAL'].apply(lambda x: x.zfill(3))

    df['OCUP'] = df['OCUP'].apply(lambda x: x.zfill(6))
    df['OCUP'] = df['OCUP'].apply(str.strip)
    df['OCUP'] = df['OCUP'].apply(lambda x: x if len(x) == 6 else '')

    df['CODESTAB'] = df['CODESTAB'].apply(lambda x: x.zfill(7))

    # Atualiza/corrige os labels das colunas especificadas
    df['NATURAL'].replace(['000', '999'], '', inplace=True)
    df['NATURAL'].replace('800', '001', inplace=True)
    df['NATURAL'].replace('00.', '', inplace=True)
    df['NATURAL'].replace('8s9', '', inplace=True)

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

    for col in ['CAUSABAS', 'CAUSABAS_O']:
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

    df['CAUSABAS_O'].replace(regex='.$',value='', inplace=True)
    return df


def get_missing_codestab_SIM(path):
    # Conversão da Tabela CNESDO18 para um objeto pandas DataFrame
    file_name = 'CNESDO18'
    df1 = download_table_dbf(file_name)
    # Ordena as linhas de "df1" por ordem crescente dos valores da coluna CODESTAB
    df1.sort_values(by=['CODESTAB'], inplace=True)
    # Reset o index devido ao sorting prévio
    df1.reset_index(drop=True, inplace=True)
    # Converte a coluna especificada de string para int
    df1['CODESTAB'] = df1['CODESTAB'].astype('int')
    # Conversão da TCC ESTAB06 para um objeto pandas DataFrame
    file_name = 'ESTAB06'
    df2 = download_table_cnv(file_name)
    df2.rename(index=str, columns={'ID': 'CODESTAB', 'SIGNIFICACAO': 'DESCESTAB'}, inplace=True)
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
    # Conversão da TCC NAT_ORG (já em formato "xlsx" e não "cnv") para um objeto pandas DataFrame
    file_name = 'NAT_ORG'
    df4 = download_table_cnv(file_name)
    # Adequa e formata a TCC NAT_ORG
    df4.rename(index=str, columns={'ID': 'CODESTAB', 'SIGNIFICACAO': 'REGIME'}, inplace=True)
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
    # Converte a coluna "CODESTAB" do objeto "df8" de "int" para "string"
    df8['CODESTAB'] = df8['CODESTAB'].astype('str')
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna "CODESTAB" do objeto "df8" até formar uma "string" de tamanho = 7
    df8['CODESTAB'] = df8['CODESTAB'].apply(lambda x: x.zfill(7))

    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE' ,'SP', 'TO']
    for ano in range(1997, 2018, 1):
        for estado in lista_estados:
            print('\nDO{}{}'.format(estado, ano))
            try:
                df_DO = get_DOXXaaaa_simplified(estado, ano)
            except:
                print('O arquivo DO{}{} não existe.'.format(estado, ano))
                continue
            else:
                print('O número de linhas do DO{}{} no início é: {}'.format(estado, ano, df_DO.shape[0]))
                df_DO = df_DO.drop(df_DO[df_DO['CODESTAB']==''].index)
                print('O número de linhas do DO{}{} após exclusão das string vazias é: {}'.format(estado, ano, df_DO.shape[0]))
                df_DO.drop_duplicates(subset='CODESTAB', keep='first', inplace=True)
                print('O número de linhas do DO{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, df_DO.shape[0]))
                frames.append(df_DO)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset='CODESTAB', keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Converte a coluna CODESTAB de "int" para "string"
    full_df['CODESTAB'] = full_df['CODESTAB'].astype(str)
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna CODESTAB do objeto "full_df" até formar uma "string" de tamanho = 7
    full_df['CODESTAB'] = full_df['CODESTAB'].apply(lambda x: x.zfill(7))
    # Converte para um objeto list a coluna CODESTAB do objeto "full_df"
    full_codigos = full_df['CODESTAB'].tolist()

    # Obtém em um arquivo "xlsx"
    full_codestab = df8['CODESTAB'].tolist()
    dif_codestab = list(set(full_codigos) - set(full_codestab))
    dif_codestab.sort()
    df_dif_codestab = pd.DataFrame(columns=['CODESTAB'])
    df_dif_codestab['CODESTAB'] = dif_codestab
    df_dif_codestab.to_excel(os.getcwd() + '\\SIM_CODESTAB_MISSING.xlsx', index=False)
    print('Game over!')


def get_missing_natural_SIM(path):
    # Conversão da TCC NAT1212 para um objeto pandas DataFrame
    file_name = 'NAT1212'
    df = download_table_cnv(file_name)
    # Adequa e formata a TCC NAT1212
    df.rename(index=str, columns={'SIGNIFICACAO': 'LOCAL'}, inplace=True)
    df.drop_duplicates(subset='ID', keep="first", inplace=True)
    # Converte para string a coluna especificada
    df['ID'] = df['ID'].astype('str')
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna "ID" do objeto "df" até formar uma "string" de tamanho = 3
    df['ID'] = df['ID'].apply(lambda x: x.zfill(3))

    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE' ,'SP', 'TO']
    for ano in range(1997, 2018, 1):
        for estado in lista_estados:
            print('\nDO{}{}'.format(estado, ano))
            try:
                df_DO = get_DOXXaaaa_simplified(estado, ano)
            except:
                print('O arquivo DO{}{} não existe.'.format(estado, ano))
                continue
            else:
                print('O número de linhas do DO{}{} no início é: {}'.format(estado, ano, df_DO.shape[0]))
                df_DO = df_DO.drop(df_DO[df_DO['NATURAL']==''].index)
                print('O número de linhas do DO{}{} após exclusão das string vazias é: {}'.format(estado, ano, df_DO.shape[0]))
                df_DO.drop_duplicates(subset='NATURAL', keep='first', inplace=True)
                print('O número de linhas do DO{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, df_DO.shape[0]))
                frames.append(df_DO)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset='NATURAL', keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Converte a coluna NATURAL de "int" para "string"
    full_df['NATURAL'] = full_df['NATURAL'].astype(str)
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna NATURAL do objeto "full_df" até formar uma "string" de tamanho = 3
    full_df['NATURAL'] = full_df['NATURAL'].apply(lambda x: x.zfill(3))
    # Converte para um objeto list a coluna OCUP do objeto "full_df"
    full_codigos = full_df['NATURAL'].tolist()

    # Obtém em um arquivo "xlsx"
    full_natural = df['ID'].tolist()
    dif_natural = list(set(full_codigos) - set(full_natural))
    dif_natural.sort()
    df_dif_natural = pd.DataFrame(columns=['ID'])
    df_dif_natural['ID'] = dif_natural
    df_dif_natural.to_excel(os.getcwd() + '\\SIM_NATURAL_MISSING.xlsx', index=False)
    print('Game over!')


def get_missing_ocup_SIM(path):
    # Conversão da Tabela TABOCUP para um objeto pandas DataFrame
    file_name = 'TABOCUP'
    df1 = download_table_dbf(file_name, cache=True)
    # Renomeia as colunas especificadas
    df1.rename(index=str, columns={'CODIGO': 'ID', 'DESCRICAO': 'OCUPACAO'}, inplace=True)
    # Ordena as linhas de "df1" por ordem crescente dos valores da coluna ID
    df1.sort_values(by=['ID'], inplace=True)
    # Reset o index devido ao sorting prévio
    df1.reset_index(drop=True, inplace=True)
    # Converte a coluna especificada de string para int
    df1['ID'] = df1['ID'].astype('int')
    # Conversão da TCC CBO2002 para um objeto pandas DataFrame
    file_name = 'CBO2002'
    df2 = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df2.rename(index=str, columns={'SIGNIFICACAO': 'OCUPACAO'}, inplace=True)
    # Coloca todas as string da coluna especificada como UPPER CASE
    df2['OCUPACAO'] = df2['OCUPACAO'].apply(lambda x: x.upper())
    # Conversão da TCC OCUPA para um objeto pandas DataFrame
    file_name = 'OCUPA'
    df3 = download_table_cnv(file_name)
    # Renomeia a coluna SIGNIFICACAO
    df3.rename(index=str, columns={'SIGNIFICACAO': 'OCUPACAO'}, inplace=True)
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
    # Converte para string a coluna especificada
    df['ID'] = df['ID'].astype('str')
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna ID do objeto "df" até formar uma "string" de tamanho = 6
    df['ID'] = df['ID'].apply(lambda x: x.zfill(6))

    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE' ,'SP', 'TO']
    for ano in range(1997, 2018, 1):
        for estado in lista_estados:
            print('\nDO{}{}'.format(estado, ano))
            try:
                df_DO = get_DOXXaaaa_simplified(estado, ano)
            except:
                print('O arquivo DO{}{} não existe.'.format(estado, ano))
                continue
            else:
                print('O número de linhas do DO{}{} no início é: {}'.format(estado, ano, df_DO.shape[0]))
                df_DO = df_DO.drop(df_DO[df_DO['OCUP']==''].index)
                print('O número de linhas do DO{}{} após exclusão das string vazias é: {}'.format(estado, ano, df_DO.shape[0]))
                df_DO.drop_duplicates(subset='OCUP', keep='first', inplace=True)
                print('O número de linhas do DO{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, df_DO.shape[0]))
                frames.append(df_DO)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset='OCUP', keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Converte a coluna OCUP de "int" para "string"
    full_df['OCUP'] = full_df['OCUP'].astype(str)
    # Adiciona zeros à esquerda nos valores (tipo string) da coluna OCUP do objeto "full_df" até formar uma "string" de tamanho = 6
    full_df['OCUP'] = full_df['OCUP'].apply(lambda x: x.zfill(6))
    # Converte para um objeto list a coluna OCUP do objeto "full_df"
    full_codigos = full_df['OCUP'].tolist()

    # Obtém em um arquivo "xlsx"
    full_ocup = df['ID'].tolist()
    dif_ocup = list(set(full_codigos) - set(full_ocup))
    dif_ocup.sort()
    df_dif_ocup = pd.DataFrame(columns=['ID'])
    df_dif_ocup['ID'] = dif_ocup
    df_dif_ocup.to_excel(os.getcwd() + '\\SIM_OCUP_MISSING.xlsx', index=False)
    print('Game over!')


def get_missing_causabas_o_SIM(path):
    # Conversão da Tabela CID10 para um objeto pandas DataFrame
    file_name = 'CID10'
    df1 = download_table_dbf(file_name)
    # Remove colunas indesejáveis do objeto pandas DataFrame
    df1 = df1.drop(['OPC', 'CAT', 'SUBCAT', 'RESTRSEXO'], axis=1)
    # Renomeia as colunas especificadas
    df1.rename(index=str, columns={'CID10': 'ID', 'DESCR': 'DOENCA'}, inplace=True)
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

    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE' ,'SP', 'TO']
    for ano in range(1997, 2018, 1):
        for estado in lista_estados:
            print('\nDO{}{}'.format(estado, ano))
            try:
                df_DO = get_DOXXaaaa_simplified(estado, ano)
            except:
                print('O arquivo DO{}{} não existe.'.format(estado, ano))
                continue
            else:
                print('O número de linhas do DO{}{} no início é: {}'.format(estado, ano, df_DO.shape[0]))
                df_DO = df_DO.drop(df_DO[df_DO['CAUSABAS_O']==''].index)
                print('O número de linhas do DO{}{} após exclusão das string vazias é: {}'.format(estado, ano, df_DO.shape[0]))
                df_DO.drop_duplicates(subset='CAUSABAS_O', keep='first', inplace=True)
                print('O número de linhas do DO{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, df_DO.shape[0]))
                frames.append(df_DO)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset='CAUSABAS_O', keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Considera apenas a primeira anomalia descrita
    lista = []
    for valor in full_df['CAUSABAS_O']:
        if len(valor) > 4:
            subst = valor[:4]
        else:
            subst = valor
        lista.append(subst)
    full_df['CAUSABAS_O'] = lista
    full_codigos = full_df['CAUSABAS_O'].tolist()

    # Obtém em um arquivo "xlsx"
    full_causabas_o = df['ID'].tolist()
    dif_causabas_o = list(set(full_codigos) - set(full_causabas_o))
    dif_causabas_o.sort()
    df_dif_causabas_o = pd.DataFrame(columns=['CAUSABAS_O'])
    df_dif_causabas_o['CAUSABAS_O'] = dif_causabas_o
    df_dif_causabas_o.to_excel(os.getcwd() + '\\SIM_CAUSABAS_O_MISSING.xlsx', index=False)
    print('Game over!')


###########################################################################################################################################################################
# CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST CNES_ST #
###########################################################################################################################################################################

def get_STXXaamm_simplified(state, year, month):
    # Lê o arquivo "dbc" como um objeto pandas DataFrame e o salva no formato "parquet"
    dataframe = download_CNESXXaamm('ST', state, year, month)
    # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela stbr da base de dados
    lista_columns = np.array(['CNES'])

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

    return df

def get_missing_cnes_CNES_ST():

    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']
    for state in lista_estados:
        file_name = 'CADGER' + state
        print(file_name)
        dfi = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        dfi.rename(index=str, columns={'CNES': 'ID'}, inplace=True)
        # Remove colunas indesejáveis do objeto pandas DataFrame
        dfi = dfi.drop(['CPF_CNPJ', 'FANTASIA', 'RAZ_SOCI', 'LOGRADOU', 'NUM_END', 'COMPLEME',
                        'BAIRRO', 'COD_CEP', 'TELEFONE', 'FAX', 'EMAIL', 'REGSAUDE', 'MICR_REG',
                        'DISTRSAN', 'DISTRADM', 'CODUFMUN', 'EXCLUIDO', 'DATAINCL', 'DATAEXCL', 'NATUREZA'], axis=1)
        # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        dfi.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas por ordem crescente dos valores da coluna ID
        dfi.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
        dfi.reset_index(drop=True, inplace=True)
        frames.append(dfi)
    df = pd.concat(frames, ignore_index=True)

    coluna = 'CNES'
    frames = []

    for ano in range(8, 20):
        ano = str(ano).zfill(2)
        for mes in range(1, 13):
            mes = str(mes).zfill(2)
            for estado in lista_estados:
                print('\nST{}{}{}'.format(estado, ano, mes))
                try:
                    df_ST = get_STXXaamm_simplified(estado, ano, mes)
                except:
                    print('O arquivo ST{}{}{} não existe.'.format(estado, ano, mes))
                    continue
                else:
                    print('O número de linhas do ST{}{}{} no início é: {}'.format(estado, ano, mes, df_ST.shape[0]))
                    df_ST = df_ST.drop(df_ST[df_ST[coluna]==''].index)
                    print('O número de linhas do ST{}{}{} após exclusão das string vazias é: {}'.format(estado, ano, mes, df_ST.shape[0]))
                    df_ST.drop_duplicates(subset=coluna, keep='first', inplace=True)
                    print('O número de linhas do ST{}{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, mes, df_ST.shape[0]))
                    frames.append(df_ST)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset=coluna, keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Converte para um objeto list a coluna OCUP do objeto "full_df"
    full_codigos = full_df[coluna].tolist()

    # Obtém em um arquivo "xlsx"
    full_ocup = df['ID'].tolist()
    dif_ocup = list(set(full_codigos) - set(full_ocup))
    dif_ocup.sort()
    df_dif_ocup = pd.DataFrame(columns=['ID'])
    df_dif_ocup['ID'] = dif_ocup
    df_dif_ocup.to_excel(os.getcwd() + '\\CNES_ST' + coluna + '_MISSING.xlsx', index=False)
    print('Game over!')


###########################################################################################################################################################################
# CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF CNES_PF #
###########################################################################################################################################################################

def get_PFXXaamm_simplified(state, year, month):
    # Lê o arquivo "dbc" como um objeto pandas DataFrame e o salva no formato "parquet"
    dataframe = download_CNESXXaamm('PF', state, year, month)
    # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela stbr da base de dados
    lista_columns = np.array(['CNES', 'CBO', 'CBOUNICO'])

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
    for col in np.array(['CBO', 'CBOUNICO']):
        df[col] = df[col].apply(lambda x: x.zfill(6))
        df[col] = df[col].apply(str.strip)
        df[col] = df[col].apply(lambda x: x if len(x) == 6 else '')

    return df

def get_missing_cbo_CNES_PF():
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

    coluna = 'CBO'
    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE' ,'SP', 'TO']
    for ano in range(6, 20):
        ano = str(ano).zfill(2)
        for mes in range(1, 13):
            mes = str(mes).zfill(2)
            for estado in lista_estados:
                print('\nPF{}{}{}'.format(estado, ano, mes))
                try:
                    df_PF = get_PFXXaamm_simplified(estado, ano, mes)
                except:
                    print('O arquivo PF{}{}{} não existe.'.format(estado, ano, mes))
                    continue
                else:
                    print('O número de linhas do PF{}{}{} no início é: {}'.format(estado, ano, mes, df_PF.shape[0]))
                    df_PF = df_PF.drop(df_PF[df_PF[coluna]==''].index)
                    print('O número de linhas do PF{}{}{} após exclusão das string vazias é: {}'.format(estado, ano, mes, df_PF.shape[0]))
                    df_PF.drop_duplicates(subset=coluna, keep='first', inplace=True)
                    print('O número de linhas do PF{}{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, mes, df_PF.shape[0]))
                    frames.append(df_PF)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset=coluna, keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Converte para um objeto list a coluna OCUP do objeto "full_df"
    full_codigos = full_df[coluna].tolist()

    # Obtém em um arquivo "xlsx"
    full_ocup = df['ID'].tolist()
    dif_ocup = list(set(full_codigos) - set(full_ocup))
    dif_ocup.sort()
    df_dif_ocup = pd.DataFrame(columns=['ID'])
    df_dif_ocup['ID'] = dif_ocup
    df_dif_ocup.to_excel(os.getcwd() + '\\CNES_PF_' + coluna + '_MISSING.xlsx', index=False)
    print('Game over!')


###########################################################################################################################################################################
# CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP CNES_EP #
###########################################################################################################################################################################

def get_EPXXaamm_simplified(state, year, month):
    # Lê o arquivo "dbc" como um objeto pandas DataFrame e o salva no formato "parquet"
    dataframe = download_CNESXXaamm('EP', state, year, month)

    # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela EPBR da base de dados
    lista_columns = np.array(['CNES', 'IDEQUIPE', 'ID_AREA', 'ID_SEGM'])

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

    # Atualiza/corrige os labels das colunas especificadas

    df['ID_AREA'].replace(regex=['^1[1-7][09]{8}$', '^2[1-9][09]{8}$','^3[1235][09]{8}$', '^4[1-3][09]{8}$', '^5[0-3][09]{8}$'], value='', inplace=True)

    df['ID_SEGM'].replace(regex=['^1[1-7][09]{6}$', '^2[1-9][09]{6}$','^3[1235][09]{6}$', '^4[1-3][09]{6}$', '^5[0-3][09]{6}$'], value='', inplace=True)

    return df


def get_missing_IDEQUIPE_CNES_EP():
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
    # Elimina linhas duplicadas de "dfinal" tendo por base a coluna ID e mantém a primeira ocorrência
    df.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Ordena as linhas de "dfinal" por ordem crescente dos valores da coluna ID
    df.sort_values(by=['ID'], inplace=True)
    # Reset o index de "dfinal" devido ao sorting prévio e à exclusão das linhas referidas acima
    df.reset_index(drop=True, inplace=True)

    coluna = 'IDEQUIPE'
    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE' ,'SP', 'TO']
    for ano in range(8, 20):
        ano = str(ano).zfill(2)
        for mes in range(1, 13):
            mes = str(mes).zfill(2)
            for estado in lista_estados:
                print('\nEP{}{}{}'.format(estado, ano, mes))
                try:
                    df_EP = get_EPXXaamm_simplified(estado, ano, mes)
                except:
                    print('O arquivo EP{}{}{} não existe.'.format(estado, ano, mes))
                    continue
                else:
                    print('O número de linhas do EP{}{}{} no início é: {}'.format(estado, ano, mes, df_EP.shape[0]))
                    df_EP = df_EP.drop(df_EP[df_EP[coluna]==''].index)
                    print('O número de linhas do EP{}{}{} após exclusão das string vazias é: {}'.format(estado, ano, mes, df_EP.shape[0]))
                    df_EP.drop_duplicates(subset=coluna, keep='first', inplace=True)
                    print('O número de linhas do EP{}{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, mes, df_EP.shape[0]))
                    frames.append(df_EP)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset=coluna, keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Converte para um objeto list a coluna OCUP do objeto "full_df"
    full_codigos = full_df[coluna].tolist()

    # Obtém em um arquivo "xlsx"
    full_ocup = df['ID'].tolist()
    dif_ocup = list(set(full_codigos) - set(full_ocup))
    dif_ocup.sort()
    df_dif_ocup = pd.DataFrame(columns=['ID'])
    df_dif_ocup['ID'] = dif_ocup
    df_dif_ocup.to_excel(os.getcwd() + '\\CNES_EP' + coluna + '_MISSING.xlsx', index=False)
    print('Game over!')


def get_missing_ID_AREA_CNES_EP():
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
    # Elimina linhas duplicadas de "dfinal" tendo por base a coluna ID e mantém a primeira ocorrência
    df.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Ordena as linhas de "dfinal" por ordem crescente dos valores da coluna ID
    df.sort_values(by=['ID'], inplace=True)
    # Reset o index de "dfinal" devido ao sorting prévio e à exclusão das linhas referidas acima
    df.reset_index(drop=True, inplace=True)

    coluna = 'ID_AREA'
    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE' ,'SP', 'TO']
    for ano in range(8, 20):
        ano = str(ano).zfill(2)
        for mes in range(1, 13):
            mes = str(mes).zfill(2)
            for estado in lista_estados:
                print('\nEP{}{}{}'.format(estado, ano, mes))
                try:
                    df_EP = get_EPXXaamm_simplified(estado, ano, mes)
                except:
                    print('O arquivo EP{}{}{} não existe.'.format(estado, ano, mes))
                    continue
                else:
                    print('O número de linhas do EP{}{}{} no início é: {}'.format(estado, ano, mes, df_EP.shape[0]))
                    df_EP = df_EP.drop(df_EP[df_EP[coluna]==''].index)
                    print('O número de linhas do EP{}{}{} após exclusão das string vazias é: {}'.format(estado, ano, mes, df_EP.shape[0]))
                    df_EP.drop_duplicates(subset=coluna, keep='first', inplace=True)
                    print('O número de linhas do EP{}{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, mes, df_EP.shape[0]))
                    frames.append(df_EP)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset=coluna, keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Converte para um objeto list a coluna OCUP do objeto "full_df"
    full_codigos = full_df[coluna].tolist()

    # Obtém em um arquivo "xlsx"
    full_ocup = df['ID'].tolist()
    dif_ocup = list(set(full_codigos) - set(full_ocup))
    dif_ocup.sort()
    df_dif_ocup = pd.DataFrame(columns=['ID'])
    df_dif_ocup['ID'] = dif_ocup
    df_dif_ocup.to_excel(os.getcwd() + '\\CNES_EP' + coluna + '_MISSING.xlsx', index=False)
    print('Game over!')


def get_missing_ID_SEGM_CNES_EP():
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
    # Elimina linhas duplicadas de "dfinal" tendo por base a coluna ID e mantém a primeira ocorrência
    df.drop_duplicates(subset='ID', keep='first', inplace=True)
    # Ordena as linhas de "dfinal" por ordem crescente dos valores da coluna ID
    df.sort_values(by=['ID'], inplace=True)
    # Reset o index de "dfinal" devido ao sorting prévio e à exclusão das linhas referidas acima
    df.reset_index(drop=True, inplace=True)

    coluna = 'ID_SEGM'
    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE' ,'SP', 'TO']
    for ano in range(8, 20):
        ano = str(ano).zfill(2)
        for mes in range(1, 13):
            mes = str(mes).zfill(2)
            for estado in lista_estados:
                print('\nEP{}{}{}'.format(estado, ano, mes))
                try:
                    df_EP = get_EPXXaamm_simplified(estado, ano, mes)
                except:
                    print('O arquivo EP{}{}{} não existe.'.format(estado, ano, mes))
                    continue
                else:
                    print('O número de linhas do EP{}{}{} no início é: {}'.format(estado, ano, mes, df_EP.shape[0]))
                    df_EP = df_EP.drop(df_EP[df_EP[coluna]==''].index)
                    print('O número de linhas do EP{}{}{} após exclusão das string vazias é: {}'.format(estado, ano, mes, df_EP.shape[0]))
                    df_EP.drop_duplicates(subset=coluna, keep='first', inplace=True)
                    print('O número de linhas do EP{}{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, mes, df_EP.shape[0]))
                    frames.append(df_EP)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset=coluna, keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Converte para um objeto list a coluna OCUP do objeto "full_df"
    full_codigos = full_df[coluna].tolist()

    # Obtém em um arquivo "xlsx"
    full_ocup = df['ID'].tolist()
    dif_ocup = list(set(full_codigos) - set(full_ocup))
    dif_ocup.sort()
    df_dif_ocup = pd.DataFrame(columns=['ID'])
    df_dif_ocup['ID'] = dif_ocup
    df_dif_ocup.to_excel(os.getcwd() + '\\CNES_EP' + coluna + '_MISSING.xlsx', index=False)
    print('Game over!')


###########################################################################################################################################################################
# SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD SIH_RD #
###########################################################################################################################################################################

def get_RDXXaamm_simplified(state, year, month):
    # Lê o arquivo "dbc" como um objeto pandas DataFrame e o salva no formato "parquet"
    #dataframe = download_SIHXXaamm('RD', state, year, month)
    dataframe = download_SIHXXaamm('SP', state, year, month)
    # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela rdbr da base de dados
    lista_columns = np.array(['SP_ATOPROF'])

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

    return df

def get_missing_PROC_SOLIC_SIH_RD():
    # Conversão da Tabela TB_SIGTAP para um objeto pandas DataFrame
    file_name = 'TB_SIGTAP'
    df = download_table_dbf(file_name)
    # Renomeia as colunas especificadas
    df.rename(index=str, columns={'IP_COD': 'ID', 'IP_DSCR': 'PROCEDIMENTO'}, inplace=True)
    # Inserção da primary key "NA" na tabela de que trata esta função para retratar "missing value" da tabela RDBR
    df.loc[df.shape[0]] = ['NA', 'NOT AVAILABLE']

    coluna = 'SP_ATOPROF'
    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']

    for ano in range(8, 20):
        ano = str(ano).zfill(2)
        for mes in range(1, 13):
            mes = str(mes).zfill(2)
            for estado in lista_estados:
                print('\nRD{}{}{}'.format(estado, ano, mes))
                try:
                    df_RD = get_RDXXaamm_simplified(estado, ano, mes)
                except:
                    print('O arquivo RD{}{}{} não existe.'.format(estado, ano, mes))
                    continue
                else:
                    print('O número de linhas do RD{}{}{} no início é: {}'.format(estado, ano, mes, df_RD.shape[0]))
                    df_RD = df_RD.drop(df_RD[df_RD[coluna]==''].index)
                    print('O número de linhas do RD{}{}{} após exclusão das string vazias é: {}'.format(estado, ano, mes, df_RD.shape[0]))
                    df_RD.drop_duplicates(subset=coluna, keep='first', inplace=True)
                    print('O número de linhas do RD{}{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, mes, df_RD.shape[0]))
                    frames.append(df_RD)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset=coluna, keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Converte para um objeto list a coluna OCUP do objeto "full_df"
    full_codigos = full_df[coluna].tolist()

    # Obtém em um arquivo "xlsx"
    full_ocup = df['ID'].tolist()
    dif_ocup = list(set(full_codigos) - set(full_ocup))
    dif_ocup.sort()
    df_dif_ocup = pd.DataFrame(columns=['ID'])
    df_dif_ocup['ID'] = dif_ocup
    df_dif_ocup.to_excel(os.getcwd() + '\\SIH_RD_' + coluna + '_MISSING.xlsx', index=False)
    print('Game over!')

def get_missing_cnes_SIH_RD():
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

    coluna = 'CNES'
    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']

    for ano in range(8, 20):
        ano = str(ano).zfill(2)
        for mes in range(1, 13):
            mes = str(mes).zfill(2)
            for estado in lista_estados:
                print('\nRD{}{}{}'.format(estado, ano, mes))
                try:
                    df_RD = get_RDXXaamm_simplified(estado, ano, mes)
                except:
                    print('O arquivo RD{}{}{} não existe.'.format(estado, ano, mes))
                    continue
                else:
                    print('O número de linhas do RD{}{}{} no início é: {}'.format(estado, ano, mes, df_RD.shape[0]))
                    df_RD = df_RD.drop(df_RD[df_RD[coluna]==''].index)
                    print('O número de linhas do RD{}{}{} após exclusão das string vazias é: {}'.format(estado, ano, mes, df_RD.shape[0]))
                    df_RD.drop_duplicates(subset=coluna, keep='first', inplace=True)
                    print('O número de linhas do RD{}{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, mes, df_RD.shape[0]))
                    frames.append(df_RD)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset=coluna, keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Converte para um objeto list a coluna OCUP do objeto "full_df"
    full_codigos = full_df[coluna].tolist()

    # Obtém em um arquivo "xlsx"
    full_ocup = df['ID'].tolist()
    dif_ocup = list(set(full_codigos) - set(full_ocup))
    dif_ocup.sort()
    df_dif_ocup = pd.DataFrame(columns=['ID'])
    df_dif_ocup['ID'] = dif_ocup
    df_dif_ocup.to_excel(os.getcwd() + '\\SIH_RD_' + coluna + '_MISSING.xlsx', index=False)
    print('Game over!')


###########################################################################################################################################################################
# SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA SIA_PA #
###########################################################################################################################################################################

def get_PAXXaamm_simplified(state, year, month):
    # Lê o arquivo "dbc" como um objeto pandas DataFrame e o salva no formato "parquet"
    dataframe = download_SIAXXaamm('PA', state, year, month)
    # Colunas definidas como necessárias no objeto pandas DataFrame que incrementará a tabela pabr da base de dados
    lista_columns = np.array(['PA_CODUNI'])

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

    return df

def get_missing_cnes_SIA_PA():

    frames = []
    lista_estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
                     'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']
    for state in lista_estados:
        file_name = 'CADGER' + state
        print(file_name)
        dfi = download_table_dbf(file_name)
        # Renomeia as colunas especificadas
        dfi.rename(index=str, columns={'CNES': 'ID'}, inplace=True)
        # Remove colunas indesejáveis do objeto pandas DataFrame
        dfi = dfi.drop(['CPF_CNPJ', 'FANTASIA', 'RAZ_SOCI', 'LOGRADOU', 'NUM_END', 'COMPLEME',
                        'BAIRRO', 'COD_CEP', 'TELEFONE', 'FAX', 'EMAIL', 'REGSAUDE', 'MICR_REG',
                        'DISTRSAN', 'DISTRADM', 'CODUFMUN', 'EXCLUIDO', 'DATAINCL', 'DATAEXCL', 'NATUREZA'], axis=1)
        # Elimina linhas duplicadas tendo por base a coluna ID e mantém a primeira ocorrência
        dfi.drop_duplicates(subset='ID', keep='first', inplace=True)
        # Ordena as linhas por ordem crescente dos valores da coluna ID
        dfi.sort_values(by=['ID'], inplace=True)
        # Reset o index devido ao sorting prévio e à eventual eliminação de duplicates
        dfi.reset_index(drop=True, inplace=True)
        frames.append(dfi)
    df = pd.concat(frames, ignore_index=True)

    coluna = 'PA_CODUNI'
    frames = []

    for ano in range(8, 20):
        ano = str(ano).zfill(2)
        for mes in range(1, 13):
            mes = str(mes).zfill(2)
            for estado in lista_estados:
                print('\nPA{}{}{}'.format(estado, ano, mes))
                try:
                    df_PA = get_PAXXaamm_simplified(estado, ano, mes)
                except:
                    print('O arquivo PA{}{}{} não existe.'.format(estado, ano, mes))
                    continue
                else:
                    print('O número de linhas do PA{}{}{} no início é: {}'.format(estado, ano, mes, df_PA.shape[0]))
                    df_PA = df_PA.drop(df_PA[df_PA[coluna]==''].index)
                    print('O número de linhas do PA{}{}{} após exclusão das string vazias é: {}'.format(estado, ano, mes, df_PA.shape[0]))
                    df_PA.drop_duplicates(subset=coluna, keep='first', inplace=True)
                    print('O número de linhas do PA{}{}{} após eliminação das próprias duplicates: {}'.format(estado, ano, mes, df_PA.shape[0]))
                    frames.append(df_PA)
    full_df = pd.concat(frames, ignore_index=True)
    print(full_df.shape)
    full_df.drop_duplicates(subset=coluna, keep='first', inplace=True)
    print(full_df.shape)
    print('Finished prestuff!')

    # Converte para um objeto list a coluna OCUP do objeto "full_df"
    full_codigos = full_df[coluna].tolist()

    # Obtém em um arquivo "xlsx"
    full_ocup = df['ID'].tolist()
    dif_ocup = list(set(full_codigos) - set(full_ocup))
    dif_ocup.sort()
    df_dif_ocup = pd.DataFrame(columns=['ID'])
    df_dif_ocup['ID'] = dif_ocup
    df_dif_ocup.to_excel(os.getcwd() + '\\SIA_' + coluna + '_MISSING.xlsx', index=False)
    print('Game over!')



if __name__ == '__main__':

    datasus_db = input('Enter with the Datasus database name (maybe lower case): ').upper()

    the_path_Tabelas = os.getcwd() + '\\files\\' + datasus_db + '\\'

    if datasus_db == 'SINASC':
        from insertion.data_wrangling.online.download_SINASC import download_DNXXaaaa, download_table_dbf, download_table_cnv
        #get_missing_codestab_SINASC(the_path_Tabelas)
        #get_missing_codocupmae_SINASC(the_path_Tabelas)
        #get_missing_codanomal_SINASC(the_path_Tabelas)
    elif datasus_db == 'SIM':
        from insertion.data_wrangling.online.download_SIM import download_DOXXaaaa, download_table_dbf, download_table_cnv
        #get_missing_codestab_SIM(the_path_Tabelas)
        #get_missing_natural_SIM(the_path_Tabelas)
        #get_missing_ocup_SIM(the_path_Tabelas)
        #get_missing_causabas_o_SIM(the_path_Tabelas)
    elif datasus_db == 'CNES':
        from insertion.data_wrangling.online.download_CNES import download_CNESXXaamm, download_table_dbf, download_table_cnv
        #get_missing_cnes_CNES_ST()
        #get_missing_cbo_CNES_PF()
        #get_missing_IDEQUIPE_CNES_EP()
        #get_missing_ID_AREA_CNES_EP()
        get_missing_ID_SEGM_CNES_EP()
    elif datasus_db == 'SIH':
        from insertion.data_wrangling.online.download_SIH import download_SIHXXaamm, download_table_dbf, download_table_cnv
        get_missing_PROC_SOLIC_SIH_RD()
        #get_missing_cnes_SIH_RD()
    elif datasus_db == 'SIA':
        from insertion.data_wrangling.online.download_SIA import download_SIAXXaamm, download_table_dbf, download_table_cnv
        get_missing_cnes_SIA_PA()
